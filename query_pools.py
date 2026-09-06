#!/usr/bin/env python3
"""query_pools.py — key pools that make an appended CDC tick HIT a query.

seed_tpcds.py draws every foreign key uniformly over the dimension's key range
and every date uniformly over --years, so a query with narrow dimension
filters (q18: one year, one demographics tuple, a state list) almost never
sees its appended rows: the delta term folds to 0 rows and the merge is never
exercised (node 1788402989672, 2026-09-06).

This tool reads the query's simple dimension predicates (equality, IN,
BETWEEN, comparisons, LIKE, and OR-groups confined to one dimension),
evaluates them against the CUSTOMER Iceberg catalog with DuckDB, and writes a
JSON of key pools:

    {"date_sk": [d_date_sk, …], "dims": {"item": [i_item_sk, …], "customer": […], …}}

seed_tpcds.py --key-pools <file> then draws the appended facts' keys from the
pools (uniform draws elsewhere). Dimensions reached only through customer
(customer_address, customer_demographics, household_demographics via
c_current_*_sk) narrow the customer pool. Anything the parser cannot read
is skipped, never guessed; an empty pool for a dimension means "no
constraint" for that dimension.

Usage:
  query_pools.py --sql-file q18.sql [--sql-file …] --catalog http://host:8181 \
      --warehouse s3://tpcds1000/wh --namespace tpcds_sf1000 \
      [--s3-endpoint http://host:9002] [--s3-key … --s3-secret …] --out pools.json
"""
import argparse, json, re, sys

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
        conj.extend(parts)
    return conj


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
    for c in where_conjuncts(sql):
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
        alias = next(iter(aliases)) if len(aliases) == 1 else ""
        # drop qualifiers so the predicate binds on the bare dim table
        bare = re.sub(r"(?i)\b([a-z_][a-z0-9_]*)\.(?=[a-z_])", "", c)
        out.setdefault(dim, {}).setdefault(alias, []).append(bare.strip())
    return out


def duck_connect(a):
    import duckdb
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg;")
    if a.s3_endpoint:
        ep = re.sub(r"^https?://", "", a.s3_endpoint)
        ssl = "true" if a.s3_endpoint.startswith("https") else "false"
        con.execute("CREATE OR REPLACE SECRET src (TYPE S3, KEY_ID '%s', SECRET '%s', ENDPOINT '%s', URL_STYLE 'path', USE_SSL %s)"
                    % (a.s3_key, a.s3_secret, ep, ssl))
    con.execute("CREATE OR REPLACE SECRET ic (TYPE ICEBERG, ENDPOINT '%s', TOKEN '')" % a.catalog)
    con.execute("ATTACH '%s' AS src (TYPE ICEBERG, SECRET ic)" % a.warehouse)
    return con


def pool_for(con, ns, dim, key, groups, limit):
    """Union over alias groups of keys satisfying that group's predicates."""
    keys = set()
    for alias, preds in groups.items():
        where = " AND ".join("(" + p + ")" for p in preds)
        q = "SELECT %s FROM src.%s.%s WHERE %s LIMIT %d" % (key, ns, dim, where, limit)
        try:
            for (k,) in con.execute(q).fetchall():
                if k is not None:
                    keys.add(int(k))
        except Exception as e:
            print("  skip %s [%s]: %s" % (dim, alias or "-", str(e).splitlines()[0][:160]), file=sys.stderr)
    return sorted(keys)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sql-file", action="append", required=True)
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--namespace", required=True)
    ap.add_argument("--s3-endpoint", default="")
    ap.add_argument("--s3-key", default="minioadmin")
    ap.add_argument("--s3-secret", default="minioadmin")
    ap.add_argument("--limit", type=int, default=20000)
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    con = duck_connect(a)
    pools = {"date_sk": [], "dims": {}}
    date_keys, dim_keys = set(), {}
    keymap = {dim: key for _, (dim, key) in DIM_BY_PREFIX}
    for f in a.sql_file:
        sql = open(f).read()
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
                ks = pool_for(con, a.namespace, dim, keymap[dim], fact_groups, a.limit)
                print("  %s (fact-side %s): %d key(s)" % (dim, list(fact_groups), len(ks)), file=sys.stderr)
                if ks:
                    if dim == "date_dim":
                        date_keys.update(ks)
                    else:
                        dim_keys.setdefault(dim, set()).update(ks)
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
            except Exception as e:
                print("  customer narrowing failed: %s" % str(e).splitlines()[0][:200], file=sys.stderr)
    pools["date_sk"] = sorted(date_keys)
    pools["dims"] = {d: sorted(v) for d, v in dim_keys.items() if v}
    json.dump(pools, open(a.out, "w"))
    print("pools: %d date(s), %s" % (len(pools["date_sk"]), {d: len(v) for d, v in pools["dims"].items()}), file=sys.stderr)


if __name__ == "__main__":
    main()
