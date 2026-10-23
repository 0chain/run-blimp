#!/usr/bin/env python3
"""query_pools.py — key pools that make an appended CDC tick HIT a query.

seed_tpcds.py draws every foreign key uniformly over the dimension's key range
and every date uniformly over --years, so a query with narrow dimension
filters (q18: one year, one demographics tuple, a state list) almost never
sees its appended rows: the delta term folds to 0 rows and the merge is never
exercised.

This tool reads the query's simple dimension predicates (equality, IN,
BETWEEN, comparisons, LIKE, and OR-groups confined to one dimension),
evaluates them against the CUSTOMER Iceberg catalog with DuckDB, and writes a
JSON of key pools:

    {"date_sk":     [d_date_sk, …],                     # union, all queries
     "dims":        {"item": [i_item_sk, …], …},        # union, all queries
     "date_by_col": {"cs_sold_date_sk": [d_date_sk, …]},# union, per fact column
     "queries": [{"name": "q72",
                  "date_by_col": {"cs_sold_date_sk": […]},
                  "date_sk": […],
                  "dims": {"customer_demographics": […], …}}, …]}

The PER-QUERY entries are the part that makes a tick land. A single flat pool
unioned over a wave cannot work:

  * it is not keyed by FACT COLUMN, so q72's `d1.d_year = 1999` (which filters
    cs_sold_date_sk) and q67's `d_month_seq BETWEEN 1200 AND 1200+11` (which
    filters ss_sold_date_sk) merge into one list and each query's own fact gets
    only a share of rows inside its window;
  * it is not keyed by QUERY, so a row can take its date from q67 and its
    demographics keys from q72 and satisfy NEITHER — fatal for a query like
    q72 whose MV needs d_year=1999 AND cd_marital_status='D' AND
    hd_buy_potential='>10000' on the SAME row.

seed_tpcds.py --key-pools <file> assigns each appended row to ONE query
(round-robin) and draws that row's date and dimension keys from that query's
pools, uniform where no pool exists. Dimensions reached only through customer
(customer_address, customer_demographics, household_demographics via
c_current_*_sk) narrow the customer pool; a dimension the fact joins whose
key equals another pooled dimension's column (sr_cdemo_sk = cd_demo_sk =
c_current_cdemo_sk) takes that column over the pooled rows. Anything the parser cannot read
is skipped, never guessed; an empty pool for a dimension means "no
constraint" for that dimension.

Usage:
  query_pools.py --sql-file q18.sql [--sql-file …] --catalog http://host:8181 \
      --warehouse s3://my-bucket/wh --namespace tpcds \
      [--s3-endpoint http://host:9000] [--s3-key … --s3-secret …] --out pools.json
"""
import argparse, json, os, re, sys

# Column prefix → (dimension table, key column). Test fixture: the bench knows
# the TPC-DS naming; product code never carries such a map.
DIM_BY_PREFIX = [
    ("web_", ("web_site", "web_site_sk")),
    ("wp_", ("web_page", "wp_web_page_sk")),
    ("cc_", ("call_center", "cc_call_center_sk")),
    ("cp_", ("catalog_page", "cp_catalog_page_sk")),
    ("sm_", ("ship_mode", "sm_ship_mode_sk")),
    ("ca_", ("customer_address", "ca_address_sk")),
    ("cd_", ("customer_demographics", "cd_demo_sk")),
    ("hd_", ("household_demographics", "hd_demo_sk")),
    ("ib_", ("income_band", "ib_income_band_sk")),
    ("d_", ("date_dim", "d_date_sk")),
    ("t_", ("time_dim", "t_time_sk")),
    ("i_", ("item", "i_item_sk")),
    ("c_", ("customer", "c_customer_sk")),
    ("s_", ("store", "s_store_sk")),
    ("p_", ("promotion", "p_promo_sk")),
    ("w_", ("warehouse", "w_warehouse_sk")),
    ("r_", ("reason", "r_reason_sk")),
]
# customer → dims reached through its current_* keys
CUSTOMER_LINKS = {
    "customer_address": "c_current_addr_sk",
    "customer_demographics": "c_current_cdemo_sk",
    "household_demographics": "c_current_hdemo_sk",
}

IDENT = re.compile(r"(?i)\b(?:([a-z_][a-z0-9_]*)\.)?([a-z_][a-z0-9_]*)\b")
KEYWORDS = {"and", "or", "not", "in", "between", "like", "is", "null", "case", "when", "then",
            "else", "end", "cast", "as", "date", "substr", "substring", "coalesce", "true",
            "false", "integer", "bigint", "varchar", "double", "decimal", "interval", "day",
            "days", "year", "years", "month", "lower", "upper", "trim", "abs", "round"}


def dim_of(col):
    c = col.lower()
    for pfx, dim in DIM_BY_PREFIX:
        if c.startswith(pfx):
            return dim
    return None


def strip_comments(sql):
    sql = re.sub(r"--[^\n]*", " ", sql)
    return re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)


def top_level_split(text, kw):
    """Split text on the keyword (AND/OR) at parenthesis depth 0, outside quotes."""
    out, depth, cur, i, inq = [], 0, [], 0, False
    low = text.lower()
    pat = re.compile(r"\b%s\b" % kw)
    while i < len(text):
        ch = text[i]
        if ch == "'":
            inq = not inq
        if not inq:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif depth == 0:
                m = pat.match(low, i)
                if m and (i == 0 or not text[i - 1].isalnum()):
                    out.append("".join(cur).strip())
                    cur = []
                    i = m.end()
                    continue
        cur.append(ch)
        i += 1
    out.append("".join(cur).strip())
    return [p for p in out if p]


def where_conjuncts(sql):
    """Top-level conjuncts of every WHERE clause in the query (each scope's
    WHERE up to its GROUP/ORDER/HAVING/LIMIT or closing paren), BETWEEN kept whole."""
    s = strip_comments(sql)
    conj = []
    for m in re.finditer(r"(?i)\bwhere\b", s):
        i, depth, j, inq = m.end(), 0, m.end(), False
        low = s.lower()
        while j < len(s):
            ch = s[j]
            if ch == "'":
                inq = not inq
            if not inq:
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    if depth == 0:
                        break
                    depth -= 1
                elif depth == 0 and re.match(r"(group\s+by|order\s+by|having|limit|union|intersect|except|qualify|window)\b", low[j:]):
                    break
            j += 1
        parts = top_level_split(s[i:j], "and")
        # BETWEEN a AND b was split — rejoin
        k = 0
        while k < len(parts):
            p = parts[k]
            # a BETWEEN whose lower bound has no AND yet (the split ate it):
            # `d_date BETWEEN cast('1999-02-22' AS date)` + `(cast(…) + INTERVAL '30 days')`
            if re.search(r"(?i)\bbetween\b", p) and not re.search(r"(?i)\bbetween\b.*\band\b", p) and k + 1 < len(parts):
                parts[k] = p + " AND " + parts[k + 1]
                del parts[k + 1]
            k += 1
        conj.extend(_flatten_and_groups(parts))
    return conj


def _flatten_and_groups(parts):
    """A parenthesised AND group is its conjuncts: the `_filter` variants wrap
    the original WHERE as `(A AND B …) AND d_moy <= 6`, and read as ONE
    conjunct it named no date window and no dimension pool — v_q27_filter's
    rows were drawn over all 20,000 dates and its ticks merged nothing (node
    144, 2026-10-23). An OR group stays whole (expand_or_groups reads it)."""
    out = []
    for p in parts:
        inner = peel(p)
        if inner != p.strip() and len(top_level_split(inner, "or")) == 1:
            sub = top_level_split(inner, "and")
            if len(sub) > 1:
                k = 0
                while k < len(sub):  # BETWEEN's own AND was split: rejoin
                    if re.search(r"(?i)\bbetween\b", sub[k]) and not re.search(r"(?i)\bbetween\b.*\band\b", sub[k]) and k + 1 < len(sub):
                        sub[k] = sub[k] + " AND " + sub[k + 1]
                        del sub[k + 1]
                    k += 1
                out.extend(_flatten_and_groups(sub))
                continue
        out.append(p)
    return out


def strip_literals(expr):
    """String literals become a placeholder so their contents are never read as identifiers."""
    return re.sub(r"'(?:[^']|'')*'", " 'L' ", expr)


def refs(expr):
    out = []
    for q, name in IDENT.findall(strip_literals(expr)):
        n = name.lower()
        if n in KEYWORDS or re.match(r"^\d", n):
            continue
        if q and q.lower() in KEYWORDS:
            continue
        d = dim_of(n)
        if d:
            out.append((q.lower() if q else "", n, d[0]))
    return out


def customer_link_aliases(sql):
    """{dim: alias} for dimension instances joined THROUGH customer
    (c_current_addr_sk = ca.ca_address_sk etc.); '' means the bare table."""
    out = {}
    pat = re.compile(r"(?i)\b(?:[a-z_][a-z0-9_]*\.)?(c_current_(addr|cdemo|hdemo)_sk)\s*=\s*(?:([a-z_][a-z0-9_]*)\.)?([a-z_][a-z0-9_]*)|\b(?:([a-z_][a-z0-9_]*)\.)?([a-z_][a-z0-9_]*)\s*=\s*(?:[a-z_][a-z0-9_]*\.)?(c_current_(addr|cdemo|hdemo)_sk)\b")
    for c in where_conjuncts(sql):
        m = pat.search(strip_literals(c))
        if not m:
            continue
        if m.group(1):
            alias, col = (m.group(3) or ""), m.group(4)
        else:
            alias, col = (m.group(5) or ""), m.group(6)
        d = dim_of(col)
        if d and d[0] in CUSTOMER_LINKS:
            al = alias.lower()
            if al == d[0]:
                al = ""  # table-name qualifier is the bare instance
            out[d[0]] = al
    return out


def dim_predicates(sql):
    """{dim: {alias: [predicate text without qualifiers]}} for conjuncts that
    reference exactly one dimension (any alias), no other table, no subquery."""
    out = {}
    for c in expand_or_groups(where_conjuncts(sql)):
        low = c.lower()
        if "select" in low or "exists" in low:
            continue
        rs = refs(c)
        if not rs:
            continue
        # every identifier that is a column must belong to the same dim;
        # non-dim identifiers (fact columns) disqualify the conjunct
        allcols = [(q, n) for q, n in IDENT.findall(strip_literals(c))
                   if n.lower() not in KEYWORDS and n != "L" and not re.match(r"^\d", n)
                   and not (q and q.lower() in KEYWORDS)]
        dims = {d for _, _, d in rs}
        if len(dims) != 1:
            continue
        dim = next(iter(dims))
        if any(dim_of(n) is None or dim_of(n)[0] != dim for _, n in allcols if not re.match(r"(?i)^(cast|substr|substring|coalesce|lower|upper|trim|abs|round|date)$", n)):
            # a fact column or unknown identifier in the conjunct
            unknown = [n for _, n in allcols if dim_of(n) is None and not re.match(r"(?i)^(cast|substr|substring|coalesce|lower|upper|trim|abs|round|date)$", n)]
            if unknown:
                continue
        # a join equality (two columns, no literal) is not a filter
        if re.match(r"(?i)^\s*[a-z_][\w.]*\s*=\s*[a-z_][\w.]*\s*$", c):
            continue
        aliases = {("" if q == dim else q) for q, _, _ in rs}
        # A conjunct spanning TWO instances of the same dimension is a
        # correlation between them, not a filter on either: q72's
        # `d3.d_date > d1.d_date + 5` compares the ship date_dim with the sold
        # date_dim. Filed under alias "" it became a third pool group whose
        # SQL (`d_date > d_date + 5`) is unsatisfiable, and any such conjunct
        # that DID return keys would union a non-filtering instance's keys into
        # the filtering one's pool. Only single-instance conjuncts are filters.
        if len(aliases) != 1:
            continue
        alias = next(iter(aliases))
        # drop qualifiers so the predicate binds on the bare dim table
        bare = re.sub(r"(?i)\b([a-z_][a-z0-9_]*)\.(?=[a-z_])", "", c)
        out.setdefault(dim, {}).setdefault(alias, []).append(bare.strip())
    return out


# FACT-COLUMN PREDICATES. A conjunct over the fact's own columns (a measure
# band `ss_sales_price BETWEEN 100 AND 150`, a foreign-key literal
# `ss_store_sk = 4`, `ss_addr_sk IS NULL`) was skipped above, so the appended
# rows never satisfied it and the tick's delta terms produced 0 rows: q13, q19,
# q44, q48, q49 and their variants never exercised a merge (regression on
# dfd56293, 2026-10-23). Parsed into per-column constraints the seeder enforces
# on the rows the query owns.
FACT_COL = re.compile(r"(?i)^(ss|sr|cs|cr|ws|wr|inv)_[a-z0-9_]+$")
NUM = r"[-+]?\d+(?:\.\d+)?(?:\s*[-+*/]\s*[-+]?\d+(?:\.\d+)?)*"


def _num(expr):
    expr = expr.strip()
    if not re.fullmatch(r"[-+*/().\d\s]+", expr):
        return None
    try:
        return float(eval(expr, {"__builtins__": {}}, {}))
    except Exception:
        return None


def _bare_cols(c):
    return [n.lower() for q, n in IDENT.findall(strip_literals(c))
            if n.lower() not in KEYWORDS and n != "L" and not re.match(r"^\d", n)
            and not (q and q.lower() in KEYWORDS)]


def fact_constraint(c):
    """(col, constraint) for a conjunct over ONE fact column, else None.
    constraint: {"eq": [v…]} | {"lo": x, "hi": y} | {"null": bool}."""
    cols = _bare_cols(c)
    if len(set(cols)) != 1 or not FACT_COL.match(cols[0]):
        return None
    col = cols[0]
    b = peel(re.sub(r"(?i)\b[a-z_][a-z0-9_]*\.(?=[a-z_])", "", c))
    m = re.fullmatch(r"(?is)%s\s+between\s+(%s)\s+and\s+(%s)" % (col, NUM, NUM), b)
    if m:
        lo, hi = _num(m.group(1)), _num(m.group(2))
        return (col, {"lo": lo, "hi": hi}) if lo is not None and hi is not None else None
    m = re.fullmatch(r"(?is)%s\s*(<=|>=|<|>|=)\s*(%s)" % (col, NUM), b)
    if m:
        v = _num(m.group(2))
        if v is None:
            return None
        op = m.group(1)
        return (col, {"eq": [v]} if op == "=" else {"lo": v} if op in (">", ">=") else {"hi": v})
    m = re.fullmatch(r"(?is)%s\s+in\s*\(([^()]*)\)" % col, b)
    if m:
        vs = [_num(x) for x in m.group(1).split(",")]
        return (col, {"eq": vs}) if vs and all(v is not None for v in vs) else None
    m = re.fullmatch(r"(?is)%s\s+is\s+(not\s+)?null" % col, b)
    if m:
        return (col, {"null": not m.group(1)})
    return None


def peel(t):
    """Remove outer parentheses only while they are ONE matching pair."""
    t = t.strip()
    while t.startswith("(") and t.endswith(")"):
        depth = 0
        for i, ch in enumerate(t):
            depth += ch == "("
            depth -= ch == ")"
            if depth == 0 and i < len(t) - 1:
                return t  # the first '(' closes before the end: not a wrapper
        t = t[1:-1].strip()
    return t


def expand_or_groups(conjs):
    """A top-level OR whose disjuncts mix dimension and fact columns (q13/q48's
    demographic-and-price bands) is replaced by its FIRST disjunct's conjuncts:
    a row satisfying one disjunct satisfies the OR."""
    out = []
    for c in conjs:
        ors = top_level_split(peel(c), "or")
        if len(ors) > 1 and any(FACT_COL.match(n) for n in _bare_cols(c)):
            parts = top_level_split(peel(ors[0]), "and")
            # BETWEEN's own AND was split: rejoin, as where_conjuncts does.
            joined = []
            for p_ in parts:
                if joined and re.search(r"(?i)\bbetween\b", joined[-1]) and not re.search(r"(?i)\bbetween\b.*\band\b", joined[-1]):
                    joined[-1] = joined[-1] + " AND " + p_
                else:
                    joined.append(p_)
            out.extend(peel(p_) for p_ in joined)
        else:
            out.append(c)
    return out


def fact_predicates(sql):
    """{fact column: merged constraint} over the query's conjuncts (OR-groups
    expanded to their first disjunct)."""
    out = {}
    for c in expand_or_groups(where_conjuncts(sql)):
        if "select" in c.lower():
            continue
        fc = fact_constraint(c)
        if not fc:
            continue
        col, k = fc
        e = out.setdefault(col, {})
        if "eq" in k:
            e["eq"] = k["eq"]
        if "null" in k:
            e["null"] = k["null"]
        if k.get("lo") is not None:
            e["lo"] = max(e.get("lo", k["lo"]), k["lo"])
        if k.get("hi") is not None:
            e["hi"] = min(e.get("hi", k["hi"]), k["hi"])
    return out


# The surrogate-key column of each dimension, inverted from DIM_BY_PREFIX so no
# key name is typed twice.
KEYCOL = {dim: key for _, (dim, key) in DIM_BY_PREFIX}

EQ_PAIR = re.compile(r"(?i)(?:([a-z_][a-z0-9_]*)\.)?([a-z_][a-z0-9_]*)\s*=\s*(?:([a-z_][a-z0-9_]*)\.)?([a-z_][a-z0-9_]*)")


def dim_join_columns(sql, dim):
    """{alias: [fact column joined to THAT instance of `dim`]}.

    A pool is only useful if the seeder knows WHICH fact column it constrains.
    q72 joins three date_dim instances — d1 to cs_sold_date_sk, d2 to
    inv_date_sk, d3 to cs_ship_date_sk — and only d1 carries the year filter.
    A single flat "date_sk" pool cannot express that: applied to the sold date
    it is right for d1 and meaningless for the other two, and unioned across a
    wave it is wrong for every query in it.

    Scans the WHOLE statement, not just WHERE: q72 writes its joins as
    `JOIN date_dim d1 ON (cs_sold_date_sk = d1.d_date_sk)`, which where_conjuncts
    never sees."""
    key = KEYCOL[dim]
    s = strip_literals(strip_comments(sql))
    out = {}
    for qa, ca, qb, cb in EQ_PAIR.findall(s):
        for qd, cd, qf, cf in ((qa, ca, qb, cb), (qb, cb, qa, ca)):
            if cd.lower() != key:
                continue
            f = cf.lower()
            # the other side must be a fact column: a _sk that is not itself a
            # dimension's own column (d_date_sk = d_date_sk is not a fact join)
            if not f.endswith("_sk") or dim_of(f):
                continue
            al = qd.lower()
            if al == dim:
                al = ""
            out.setdefault(al, [])
            if f not in out[al]:
                out[al].append(f)
    return out


# DIMENSION-TO-DIMENSION LINKS. q84 reaches its fact only through
# `sr_cdemo_sk = cd_demo_sk = c_current_cdemo_sk`: customer_demographics has no
# predicate of its own, the customer pool holds the Edgewood customers, and the
# appended rows' sr_cdemo_sk was drawn uniformly — no appended row joined a
# pooled customer and every q84 tick merged an empty delta (node 144,
# 2026-10-23). A dimension X joined to a fact column whose key equals a column
# of another pooled dimension Y takes as its pool the projection of that Y
# column over Y's pooled rows (the pooled customers' c_current_cdemo_sk).

def dim_links(sql):
    """[(X, x_alias, Y, y_column)] for every `X.key = Y.column` equality
    (Y's column not Y's own key), anywhere in the statement. Like
    dim_join_columns, but the other side is a dimension column, not a fact one."""
    dim_of_key = {k: d for d, k in KEYCOL.items()}
    out = []
    for qa, ca, qb, cb in EQ_PAIR.findall(strip_literals(strip_comments(sql))):
        for qx, cx, cy in ((qa, ca, cb), (qb, cb, ca)):
            x = dim_of_key.get(cx.lower())
            y = dim_of(cy)
            if not x or not y or y[0] == x or cy.lower() == y[1]:
                continue
            al = qx.lower()
            e = (x, "" if al == x else al, y[0], cy.lower())
            if e not in out:
                out.append(e)
    return out


def linked_pools(con, ns, sql, dims, limit):
    """{X: keys} derived through dim_links for every fact-joined dimension
    instance that has no pool of its own (a direct predicate's pool is never
    replaced). Iterated so a chain of links resolves. Dates are bound per fact
    column elsewhere, so date_dim is never derived here."""
    out = {}
    links = [l for l in dim_links(sql) if l[0] != "date_dim"]
    changed = True
    while changed:
        changed = False
        for x, xal, y, ycol in links:
            if x in dims or x in out or xal not in dim_join_columns(sql, x):
                continue
            ypool = dims.get(y) or out.get(y)
            if not ypool:
                continue
            q = "SELECT DISTINCT %s FROM src.%s.%s WHERE %s IN (%s) AND %s IS NOT NULL LIMIT %d" % (
                ycol, ns, y, KEYCOL[y], ",".join(str(int(k)) for k in ypool), ycol, limit)
            try:
                ks = sorted({int(k) for (k,) in con.execute(q).fetchall() if k is not None})
            except Exception as e:
                print("  skip %s via %s.%s: %s" % (x, y, ycol, str(e).splitlines()[0][:160]), file=sys.stderr)
                continue
            if ks:
                out[x] = ks
                changed = True
                print("  %s (via %s.%s over %d pooled %s): %d key(s)"
                      % (x, y, ycol, len(ypool), y, len(ks)), file=sys.stderr)
    return out


def duck_connect(a):
    import duckdb
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;")
    if a.s3_endpoint:
        ep = re.sub(r"^https?://", "", a.s3_endpoint)
        ssl = "true" if a.s3_endpoint.startswith("https") else "false"
        con.execute("CREATE OR REPLACE SECRET src (TYPE S3, KEY_ID '%s', SECRET '%s', ENDPOINT '%s', URL_STYLE 'path', USE_SSL %s)"
                    % (a.s3_key or "minioadmin", a.s3_secret or "minioadmin", ep, ssl))
    elif a.s3_key:
        # AWS S3: without a secret DuckDB reads anonymously and every dim lookup
        # fails, which leaves the tick with no pools.
        con.execute("CREATE OR REPLACE SECRET src (TYPE S3, KEY_ID '%s', SECRET '%s', REGION '%s')"
                    % (a.s3_key, a.s3_secret, a.s3_region))
    else:
        con.execute("CREATE OR REPLACE SECRET src (TYPE S3, PROVIDER credential_chain, REGION '%s')" % a.s3_region)
    con.execute("CREATE OR REPLACE SECRET ic (TYPE ICEBERG, ENDPOINT '%s', TOKEN '')" % a.catalog)
    con.execute("ATTACH '%s' AS src (TYPE ICEBERG, SECRET ic)" % a.warehouse)
    return con


def pool_by_alias(con, ns, dim, key, groups, limit):
    """{alias: sorted keys} — one pool per dimension INSTANCE, never merged.

    Merging them is what made the pool useless for a multi-instance dimension
    (see dim_join_columns): the caller decides which instance constrains which
    fact column."""
    out = {}
    for alias, preds in groups.items():
        where = " AND ".join("(" + p + ")" for p in preds)
        q = "SELECT %s FROM src.%s.%s WHERE %s LIMIT %d" % (key, ns, dim, where, limit)
        try:
            ks = sorted({int(k) for (k,) in con.execute(q).fetchall() if k is not None})
        except Exception as e:
            print("  skip %s [%s]: %s" % (dim, alias or "-", str(e).splitlines()[0][:160]), file=sys.stderr)
            continue
        if ks:
            out[alias] = ks
    return out


def pool_for(con, ns, dim, key, groups, limit):
    """Union over alias groups of keys satisfying that group's predicates."""
    keys = set()
    for ks in pool_by_alias(con, ns, dim, key, groups, limit).values():
        keys.update(ks)
    return sorted(keys)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sql-file", action="append", required=True)
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--namespace", required=True)
    ap.add_argument("--s3-endpoint", default="")
    # Same credentials the seeder writes with (AWS_* env, set by bench_cdc.sh);
    # minioadmin only as the local-MinIO fallback, the AWS credential chain
    # when no key is set.
    ap.add_argument("--s3-key", default=os.environ.get("AWS_ACCESS_KEY_ID", ""))
    ap.add_argument("--s3-secret", default=os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
    ap.add_argument("--s3-region", default=os.environ.get("AWS_REGION") or "us-east-1")
    ap.add_argument("--limit", type=int, default=20000)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    con = duck_connect(a)
    pools = {"date_sk": [], "dims": {}, "date_by_col": {}, "queries": []}
    date_keys, dim_keys, date_by_col = set(), {}, {}
    keymap = {dim: key for _, (dim, key) in DIM_BY_PREFIX}
    for f in a.sql_file:
        sql = open(f).read()
        qname = re.sub(r"\.sql$", "", f.rsplit("/", 1)[-1]) or f
        qentry = {"name": qname, "date_sk": [], "date_by_col": {}, "dims": {}, "fact": fact_predicates(sql)}
        if qentry["fact"]:
            print("  fact predicates: %s" % qentry["fact"], file=sys.stderr)
        preds = dim_predicates(sql)
        links = customer_link_aliases(sql)  # dim -> alias joined through customer
        print("%s: predicated dims %s; customer-linked %s" % (f, {d: sum(len(v) for v in g.values()) for d, g in preds.items()}, links), file=sys.stderr)
        cust_joins, cust_where = [], []
        for dim, groups in preds.items():
            fact_groups = {al: ps for al, ps in groups.items() if not (dim in links and al == links[dim])}
            link_groups = {al: ps for al, ps in groups.items() if dim in links and al == links[dim]}
            if dim == "customer":
                cust_where.extend("(" + p + ")" for ps in groups.values() for p in ps)
                continue
            if fact_groups:
                by_alias = pool_by_alias(con, a.namespace, dim, keymap[dim], fact_groups, a.limit)
                ks = sorted({k for v in by_alias.values() for k in v})
                print("  %s (fact-side %s): %d key(s)" % (dim, list(fact_groups), len(ks)), file=sys.stderr)
                if dim == "date_dim":
                    # bind each constrained date_dim INSTANCE to the fact
                    # column(s) it is joined to, so the seeder can date each
                    # column from the window that actually filters it.
                    joins = dim_join_columns(sql, dim)
                    for al, aks in by_alias.items():
                        cols = joins.get(al) or []
                        if not cols:
                            print("    date_dim [%s]: %d key(s) but no fact column joined to it "
                                  "— cannot target, skipped" % (al or "-", len(aks)), file=sys.stderr)
                            continue
                        for c in cols:
                            qentry["date_by_col"].setdefault(c, set()).update(aks)
                            date_by_col.setdefault(c, set()).update(aks)
                        print("    date_dim [%s] -> %s: %d key(s) [%d..%d]"
                              % (al or "-", ",".join(cols), len(aks), min(aks), max(aks)), file=sys.stderr)
                if ks:
                    if dim == "date_dim":
                        date_keys.update(ks)
                    else:
                        dim_keys.setdefault(dim, set()).update(ks)
                        qentry["dims"].setdefault(dim, set()).update(ks)
            if link_groups:
                alias = "l_" + dim
                cust_joins.append("JOIN src.%s.%s %s ON c.%s = %s.%s" % (a.namespace, dim, alias, CUSTOMER_LINKS[dim], alias, keymap[dim]))
                for ps in link_groups.values():
                    for p_ in ps:
                        cust_where.append("(" + p_ + ")")
        if cust_joins or cust_where:
            q = "SELECT c.c_customer_sk FROM src.%s.customer c %s%s LIMIT %d" % (
                a.namespace, " ".join(cust_joins), (" WHERE " + " AND ".join(cust_where)) if cust_where else "", a.limit)
            try:
                cks = {int(k) for (k,) in con.execute(q).fetchall() if k is not None}
                print("  customer (own predicates + linked %s): %d key(s)" % ([d for d in links if d in preds], len(cks)), file=sys.stderr)
                if cks:
                    dim_keys.setdefault("customer", set()).update(cks)
                    qentry["dims"].setdefault("customer", set()).update(cks)
            except Exception as e:
                print("  customer narrowing failed: %s" % str(e).splitlines()[0][:200], file=sys.stderr)
        for dim, ks in linked_pools(con, a.namespace, sql, qentry["dims"], a.limit).items():
            dim_keys.setdefault(dim, set()).update(ks)
            qentry["dims"].setdefault(dim, set()).update(ks)
        qentry["date_by_col"] = {c: sorted(v) for c, v in qentry["date_by_col"].items() if v}
        qentry["dims"] = {d: sorted(v) for d, v in qentry["dims"].items() if v}
        qentry["date_sk"] = sorted({k for v in qentry["date_by_col"].values() for k in v})
        if qentry["date_by_col"] or qentry["dims"] or qentry["fact"]:
            pools["queries"].append(qentry)
        else:
            print("  %s: NO usable pool — its tick rows are drawn uniformly" % qname, file=sys.stderr)
    pools["date_sk"] = sorted(date_keys)
    pools["dims"] = {d: sorted(v) for d, v in dim_keys.items() if v}
    pools["date_by_col"] = {c: sorted(v) for c, v in date_by_col.items() if v}
    json.dump(pools, open(a.out, "w"))
    print("pools: %d date(s), %s" % (len(pools["date_sk"]), {d: len(v) for d, v in pools["dims"].items()}), file=sys.stderr)
    for q in pools["queries"]:
        print("pools: %s -> %s; dims %s"
              % (q["name"],
                 {c: "[%d..%d]x%d" % (min(v), max(v), len(v)) for c, v in q["date_by_col"].items()} or "no date window",
                 {d: len(v) for d, v in q["dims"].items()} or "-"), file=sys.stderr)


if __name__ == "__main__":
    main()
