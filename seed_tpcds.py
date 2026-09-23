#!/usr/bin/env python3
"""Register/append TPC-DS-style tables into your Iceberg REST catalog so a Blimp
cluster has something to query. Requires: pip install "pyiceberg[s3fs]" pyarrow.

  python3 seed_tpcds.py --catalog http://localhost:8181 \
    --warehouse s3://YOUR-BUCKET/iceberg_wh --namespace tpcds \
    --table store_returns --rows 100000

  # one realistic CDC tick across all six facts (4:2:1 sales, 10% returns):
  python3 seed_tpcds.py --catalog ... --warehouse ... --namespace tpcds \
    --tick --rows 50000

Point Blimp's Production tab at:  Iceberg REST URL = http://<this-host>:8181,
Warehouse = your warehouse, Namespace = tpcds. Leave S3 keys blank if the Blimp
cluster runs in the same AWS account (it reads via its instance role).

================================ WHY THIS SHAPE ===============================
Everything below exists because a delta whose values are wrong is INDISTINGUISH-
ABLE from a delta that is missing: the merge runs, reports a time, and computes
nothing. Four separate mechanisms produced 0-row deltas on real runs:

 1. MISSING COLUMNS. The generator carried a hand-maintained subset of each
    fact's columns and conform_to_table_schema null-filled the rest. NULL never
    satisfies an equi-join, so any query joining on an omitted key saw an empty
    delta. Measured on test2 SF1000 (2026-08-04): store_sales appends had NULL
    ss_ticket_number / ss_net_paid / ss_sold_time_sk / ss_hdemo_sk, so q24's
    15,233 ms "merge" and q88's 2,346 ms "merge" both produced 0-row delta parts.
    FIX: generate EVERY physical column of the fact, driven off the table's own
    schema. A hand-maintained column list is a bug generator; there is no list.

 2. OUT-OF-RANGE DATES. The date_sk range was hardcoded to year 2000
    (2451545..2451910) while q4's MV filters d_year IN (2001, 2002) — disjoint,
    so q4's merge left its MV at exactly 53,491,237 rows. FIX: --years.

 3. OUT-OF-RANGE DIMENSION KEYS. Every dimension key range was a hardcoded
    guess. Two ways that silently drops rows:
      - too HIGH: ss_promo_sk used randint(1,1800) but SF1000's promotion tops
        out at p_promo_sk=1500 (measured), so ~20% of the delta could not join
        promotion at all;
      - too HIGH FOR THE SCALE: item is 300,000 rows at SF1000 but 18,000 at
        SF1, so the same randint(1,300000) drops ~94% of every item join on the
        SF1 box.
    FIX: read each dimension's real key range from the CATALOG at run time
    (iceberg manifest lower/upper bounds — no data scan, measured 0.02s/table).

 4. COLLIDING SYNTHETIC KEYS. ss_ticket_number was a module constant and
    cs_order_number was random.randint(1e9, 9e9): the second append reissued the
    first append's tickets, and the random order-number window overlapped the
    base's real range (max 8,859,306,610, measured). Duplicate keys make the
    sales x returns join fan out, so the "delta" is wrong rather than empty.
    FIX: key base = (current max in the table) + 1, read from manifest bounds.

Plus: web_returns was not a recognised --table at all. It fell through to the
store_returns branch, which built sr_* columns and add_files'd them into
web_returns — every wr_* column NULL, i.e. a 100%-dead delta.
"""
import argparse, bisect, random, decimal, os, json
DRY_RUN = False   # --dry-run: build + conform every delta, write nothing
# NOTE: pyarrow/pyiceberg are imported INSIDE the catalog-facing functions, never
# at module scope. The generator and its regression tests must run on a plain
# python3 with neither installed — that is what keeps the delta-shape guarantees
# testable on a laptop instead of only on a box with the whole stack.

# ---------------------------------------------------------------------------
# Pure generation helpers (no pyarrow / no catalog) so they can be unit-tested.
#
# d_year -> inclusive d_date_sk bounds. Verified against the SF1000 date_dim on
# test2 (2026-08-04): SELECT d_year, min(d_date_sk), max(d_date_sk) ... .
# ---------------------------------------------------------------------------
YEAR_DATE_SK = {
    1998: (2450815, 2451179), 1999: (2451180, 2451544), 2000: (2451545, 2451910),
    2001: (2451911, 2452275), 2002: (2452276, 2452640),
}

def date_sk_bounds(years):
    """Inclusive (lo, hi) d_date_sk span covering every year in `years`.

    The delta must land inside the *queried* year filter or the merge silently
    computes nothing. Measured on test2 2026-08-04: the hardcoded 2000-only span
    (2451545..2451910) put every appended row outside q4's `d_year IN (2001,2002)`
    MV filter, so q4's 7.3s "incremental merge" added 0 rows and left the MV at
    exactly 53,491,237. Spanning 2000-2002 by default keeps the d_year=2000
    queries fed AND gives q4 a non-empty delta."""
    lo = min(YEAR_DATE_SK[y][0] for y in years)
    hi = max(YEAR_DATE_SK[y][1] for y in years)
    return lo, hi

# ---------------------------------------------------------------------------
# Canonical TPC-DS fact schemas. Used to CREATE a fact that does not exist yet,
# and to let the generator run in unit tests with no catalog. When the table DOES
# exist its own schema wins — these are a fallback, never the source of truth.
# Kinds: i=int32, l=int64, d=decimal(7,2). Dumped from the SF1000 catalog on
# test2 (2026-08-04); identical to the TPC-DS 2.x spec.
# ---------------------------------------------------------------------------
def _cols(spec):
    return [(n, k) for k, names in spec for n in names.split()]

FACT_COLUMNS = {
 "store_sales": _cols([
   ("i", "ss_sold_date_sk ss_sold_time_sk ss_item_sk ss_customer_sk ss_cdemo_sk "
          "ss_hdemo_sk ss_addr_sk ss_store_sk ss_promo_sk"),
   ("l", "ss_ticket_number"),
   ("i", "ss_quantity"),
   ("d", "ss_wholesale_cost ss_list_price ss_sales_price ss_ext_discount_amt "
          "ss_ext_sales_price ss_ext_wholesale_cost ss_ext_list_price ss_ext_tax "
          "ss_coupon_amt ss_net_paid ss_net_paid_inc_tax ss_net_profit")]),
 "store_returns": _cols([
   ("i", "sr_returned_date_sk sr_return_time_sk sr_item_sk sr_customer_sk "
          "sr_cdemo_sk sr_hdemo_sk sr_addr_sk sr_store_sk sr_reason_sk"),
   ("l", "sr_ticket_number"),
   ("i", "sr_return_quantity"),
   ("d", "sr_return_amt sr_return_tax sr_return_amt_inc_tax sr_fee "
          "sr_return_ship_cost sr_refunded_cash sr_reversed_charge sr_store_credit "
          "sr_net_loss")]),
 "catalog_sales": _cols([
   ("i", "cs_sold_date_sk cs_sold_time_sk cs_ship_date_sk cs_bill_customer_sk "
          "cs_bill_cdemo_sk cs_bill_hdemo_sk cs_bill_addr_sk cs_ship_customer_sk "
          "cs_ship_cdemo_sk cs_ship_hdemo_sk cs_ship_addr_sk cs_call_center_sk "
          "cs_catalog_page_sk cs_ship_mode_sk cs_warehouse_sk cs_item_sk cs_promo_sk"),
   ("l", "cs_order_number"),
   ("i", "cs_quantity"),
   ("d", "cs_wholesale_cost cs_list_price cs_sales_price cs_ext_discount_amt "
          "cs_ext_sales_price cs_ext_wholesale_cost cs_ext_list_price cs_ext_tax "
          "cs_coupon_amt cs_ext_ship_cost cs_net_paid cs_net_paid_inc_tax "
          "cs_net_paid_inc_ship cs_net_paid_inc_ship_tax cs_net_profit")]),
 "catalog_returns": _cols([
   ("i", "cr_returned_date_sk cr_returned_time_sk cr_item_sk cr_refunded_customer_sk "
          "cr_refunded_cdemo_sk cr_refunded_hdemo_sk cr_refunded_addr_sk "
          "cr_returning_customer_sk cr_returning_cdemo_sk cr_returning_hdemo_sk "
          "cr_returning_addr_sk cr_call_center_sk cr_catalog_page_sk cr_ship_mode_sk "
          "cr_warehouse_sk cr_reason_sk"),
   ("l", "cr_order_number"),
   ("i", "cr_return_quantity"),
   ("d", "cr_return_amount cr_return_tax cr_return_amt_inc_tax cr_fee "
          "cr_return_ship_cost cr_refunded_cash cr_reversed_charge cr_store_credit "
          "cr_net_loss")]),
 "web_sales": _cols([
   ("i", "ws_sold_date_sk ws_sold_time_sk ws_ship_date_sk ws_item_sk "
          "ws_bill_customer_sk ws_bill_cdemo_sk ws_bill_hdemo_sk ws_bill_addr_sk "
          "ws_ship_customer_sk ws_ship_cdemo_sk ws_ship_hdemo_sk ws_ship_addr_sk "
          "ws_web_page_sk ws_web_site_sk ws_ship_mode_sk ws_warehouse_sk ws_promo_sk"),
   ("l", "ws_order_number"),
   ("i", "ws_quantity"),
   ("d", "ws_wholesale_cost ws_list_price ws_sales_price ws_ext_discount_amt "
          "ws_ext_sales_price ws_ext_wholesale_cost ws_ext_list_price ws_ext_tax "
          "ws_coupon_amt ws_ext_ship_cost ws_net_paid ws_net_paid_inc_tax "
          "ws_net_paid_inc_ship ws_net_paid_inc_ship_tax ws_net_profit")]),
 "web_returns": _cols([
   ("i", "wr_returned_date_sk wr_returned_time_sk wr_item_sk wr_refunded_customer_sk "
          "wr_refunded_cdemo_sk wr_refunded_hdemo_sk wr_refunded_addr_sk "
          "wr_returning_customer_sk wr_returning_cdemo_sk wr_returning_hdemo_sk "
          "wr_returning_addr_sk wr_web_page_sk wr_reason_sk"),
   ("l", "wr_order_number"),
   ("i", "wr_return_quantity"),
   ("d", "wr_return_amt wr_return_tax wr_return_amt_inc_tax wr_fee "
          "wr_return_ship_cost wr_refunded_cash wr_reversed_charge wr_account_credit "
          "wr_net_loss")]),
}

# fact -> (its returns fact, the shared join keys as (sales_col, returns_col)).
# Every returns fact joins its parent on (item, ticket/order number) — the pair
# q24 (store), q64 (catalog) and the web shapes all key on.
RETURNS_OF = {
    "store_sales":   ("store_returns",   [("ss_item_sk", "sr_item_sk"),
                                          ("ss_ticket_number", "sr_ticket_number")]),
    "catalog_sales": ("catalog_returns", [("cs_item_sk", "cr_item_sk"),
                                          ("cs_order_number", "cr_order_number")]),
    "web_sales":     ("web_returns",     [("ws_item_sk", "wr_item_sk"),
                                          ("ws_order_number", "wr_order_number")]),
}
SALES_FACTS = ["store_sales", "catalog_sales", "web_sales"]

# The realistic TPC-DS fact mix: store:catalog:web sales stand roughly 4:2:1 and
# each returns fact is ~10% of its parent. A FLAT count per table (what the CDC
# bench appended — 50k to each of five facts) makes web_sales as busy as
# store_sales and returns as busy as sales, which is not a workload any customer
# has. --ratios / --returns-ratio override.
DEFAULT_RATIOS = {"store_sales": 1.0, "catalog_sales": 0.5, "web_sales": 0.25}
DEFAULT_RETURNS_RATIO = 0.1

# The synthetic surrogate key each fact numbers its rows by.
# The sold-date column of each sales fact: the dataset's "now" for --stream-days.
FACT_DATE_COL = {"store_sales": "ss_sold_date_sk", "catalog_sales": "cs_sold_date_sk",
                 "web_sales": "ws_sold_date_sk"}

FACT_KEY_COL = {"store_sales": "ss_ticket_number", "catalog_sales": "cs_order_number",
                "web_sales": "ws_order_number", "store_returns": "sr_ticket_number",
                "catalog_returns": "cr_order_number", "web_returns": "wr_order_number"}

# Column-name SUFFIX -> (dimension table, its surrogate-key column). Suffix-based
# so it covers every prefix variant (cs_bill_addr_sk, cs_ship_addr_sk,
# wr_refunded_addr_sk ... all resolve to customer_address) without a 100-entry
# table that goes stale the moment a fact gains a column. ORDER MATTERS: cdemo_sk
# and hdemo_sk must be tested before customer_sk.
DIM_BY_SUFFIX = [
    ("_date_sk",        ("date_dim", "d_date_sk")),
    ("_time_sk",        ("time_dim", "t_time_sk")),
    ("item_sk",         ("item", "i_item_sk")),
    ("cdemo_sk",        ("customer_demographics", "cd_demo_sk")),
    ("hdemo_sk",        ("household_demographics", "hd_demo_sk")),
    ("addr_sk",         ("customer_address", "ca_address_sk")),
    ("customer_sk",     ("customer", "c_customer_sk")),
    ("store_sk",        ("store", "s_store_sk")),
    ("promo_sk",        ("promotion", "p_promo_sk")),
    ("call_center_sk",  ("call_center", "cc_call_center_sk")),
    ("catalog_page_sk", ("catalog_page", "cp_catalog_page_sk")),
    ("ship_mode_sk",    ("ship_mode", "sm_ship_mode_sk")),
    ("warehouse_sk",    ("warehouse", "w_warehouse_sk")),
    ("web_page_sk",     ("web_page", "wp_web_page_sk")),
    ("web_site_sk",     ("web_site", "web_site_sk")),
    ("reason_sk",       ("reason", "r_reason_sk")),
]

# LAST-RESORT dimension key ranges, used only when neither the catalog stats nor
# a column scan can be read (i.e. the table is unreachable, not merely
# stats-less). These are SF1000 values and are WRONG at every other scale:
# measured against the TPC-DS row counts they over-state item by 94% at SF1,
# 66% at SF10 and 32% at SF100, and customer by 99/96/83%. A key drawn above a
# dimension's real max cannot join, so using these silently folds most of the
# delta away — at SF1 to zero (visible), at SF100 to a plausible-looking but
# wrong number (worse). scan_dim_hi() exists so this is essentially never hit.
FALLBACK_DIM_HI = {
    "date_dim": 2488070, "time_dim": 86399, "item": 300000, "customer": 12000000,
    "customer_demographics": 1920800, "household_demographics": 7200,
    "customer_address": 6000000, "store": 1002, "promotion": 1500,
    "call_center": 42, "catalog_page": 30000, "ship_mode": 20, "warehouse": 20,
    "web_page": 3000, "web_site": 54, "reason": 65,
}

# Key pools from query_pools.py (--key-pools). Shape:
#   {"date_sk": [...],                      union over the wave (legacy)
#    "dims": {dim: [...]},                  union over the wave (legacy)
#    "date_by_col": {fact_col: [...]},      union, keyed by the fact column
#    "queries": [{"name", "date_by_col", "date_sk", "dims"}, ...]}
#
# WHY PER-QUERY AND PER-COLUMN, not one flat list (measured 2026-09-19, q72 on
# node 1788402989672). q72's MV bakes `d1.d_year = 1999`; the gateway's bounds
# prover refused every tick with
#     kterm_dimfilter_empty: Δcatalog_sales.cs_sold_date_sk ∈ [2451545,2452640]
#     vs date_dim.d_date_sk under "d1.d_year = 1999" ∈ [2451180,2451544]
#     — disjoint, the term contributes no rows
#     delta_noop: the Δ terms produced 0 rows — no part written
# 2451545..2452640 is exactly date_sk_bounds([2000,2001,2002]), i.e. the UNIFORM
# default: no date window reached the sold-date column at all. Two defects made
# that possible and both are structural, not q72-specific:
#   1. the pool was ONE list for every fact column. q72 filters cs_sold_date_sk
#      (via date_dim d1) and q67 filters ss_sold_date_sk (via the bare
#      date_dim); unioned, each query's own fact gets only a share of its window
#      and a query whose date_dim instance is NOT the one joined to the sold
#      date (q72 has three: d1 sold, d2 inventory, d3 ship) gets nothing usable.
#   2. every column was drawn from the union INDEPENDENTLY, so a row could take
#      its date from q67 and its demographics keys from q72 and satisfy neither.
#      q72's MV needs d_year=1999 AND cd_marital_status='D' AND
#      hd_buy_potential='>10000' on the SAME row.
# Fix: each appended row is OWNED by one query (round-robin over the queries
# that constrain this table) and draws its date and its dimension keys from that
# query's pools. Columns no query constrains keep the uniform draw.
KEY_POOLS = {"date_sk": [], "dims": {}, "date_by_col": {}, "queries": []}

# Lines describing which window each date column drew from, drained and printed
# by append_fact / append_table. A structural no-op is then visible IN THE TICK
# LOG instead of only in the gateway's refusal three minutes later.
DATE_REPORTS = []


def _span(v):
    return "[%d..%d]x%d" % (min(v), max(v), len(v)) if v else "-"


def flush_date_reports():
    global DATE_REPORTS
    for line in DATE_REPORTS:
        print(line)
    DATE_REPORTS = []


def load_key_pools(path):
    global KEY_POOLS
    with open(path) as f:
        d = json.load(f)
    qs = []
    for q in (d.get("queries") or []):
        e = {"name": q.get("name") or "?",
             "date_by_col": {k: list(v) for k, v in (q.get("date_by_col") or {}).items() if v},
             "date_sk": list(q.get("date_sk") or []),
             "dims": {k: list(v) for k, v in (q.get("dims") or {}).items() if v}}
        if e["date_by_col"] or e["dims"]:
            qs.append(e)
    KEY_POOLS = {
        "date_sk": list(d.get("date_sk") or []),
        "dims": {k: list(v) for k, v in (d.get("dims") or {}).items() if v},
        "date_by_col": {k: list(v) for k, v in (d.get("date_by_col") or {}).items() if v},
        "queries": qs,
    }
    print("   key pools: %d date(s) [union], dims %s"
          % (len(KEY_POOLS["date_sk"]), {k: len(v) for k, v in KEY_POOLS["dims"].items()}))
    for q in qs:
        print("   key pools: %s -> dates %s; dims %s"
              % (q["name"],
                 {c: _span(v) for c, v in q["date_by_col"].items()} or "NO date window",
                 {k: len(v) for k, v in q["dims"].items()} or "-"))
    if not qs:
        print("   key pools: legacy file with NO per-query windows — every column is "
              "drawn from the UNION, so a query whose MV bakes a date filter gets "
              "only its share of the rows and may get none")


def clear_key_pools():
    """Drop every pool (used by --stream-days, which dates rows by wall position)."""
    global KEY_POOLS
    KEY_POOLS = {"date_sk": [], "dims": {}, "date_by_col": {}, "queries": []}


def pool_queries_for(colnames):
    """The per-query pool entries that constrain at least one of these columns.

    A query qualifies if it has a date window for one of the table's date
    columns, or a pool for one of the dimensions the table references."""
    cols = set(colnames)
    dims = {d[0] for d in (dim_for(c) for c in colnames) if d}
    return [q for q in KEY_POOLS["queries"]
            if any(c in cols for c in q["date_by_col"]) or any(d in dims for d in q["dims"])]


def row_owners(colnames, n):
    """(owner per row, qualifying queries). Round-robin so the share is exact
    and not left to chance: with k queries each gets ceil(n/k) rows whose keys
    ALL come from its own predicate box."""
    qs = pool_queries_for(colnames)
    if not qs:
        return [None] * n, []
    return [qs[i % len(qs)] for i in range(n)], qs


def _date_pool_for_row(owner, col):
    """(pool, label) a row owned by `owner` should use for `col`, most specific
    first: the owner's window for exactly this column, then any window the owner
    has (its own year beats another query's), then the wave-wide union for this
    column, then the wave-wide union, then no pool at all."""
    if owner:
        p = owner["date_by_col"].get(col)
        if p:
            return p, owner["name"]
        if owner["date_sk"]:
            return owner["date_sk"], owner["name"] + " (its other window)"
    p = KEY_POOLS["date_by_col"].get(col)
    if p:
        return p, "(wave union for this column)"
    if KEY_POOLS["date_sk"]:
        return KEY_POOLS["date_sk"], "(wave union)"
    return None, "(uniform)"


def _note(prov, label, pool, value):
    """Record one drawn date under `label`: [pool, rows drawn, lo, hi]."""
    if prov is None:
        return
    e = prov.get(label)
    if e is None:
        prov[label] = [pool, 1, value, value]
        return
    e[1] += 1
    e[2] = min(e[2], value)
    e[3] = max(e[3], value)


def draw_dates(col, n, owners, date_lo, date_hi, rnd, prov=None):
    """n dates for `col`. `prov` (a dict) collects {label: [pool, rows, lo, hi]}
    so the caller can state WHICH window each row actually came from — a guess
    reconstructed from KEY_POOLS gets the fallbacks wrong."""
    out = []
    for i in range(n):
        p, label = _date_pool_for_row(owners[i] if i < len(owners) else None, col)
        v = rnd.choice(p) if p else rnd.randint(date_lo, date_hi)
        out.append(v)
        _note(prov, label, p, v)
    return out


def draw_dim(dimtbl, n, owners, lo, hi, rnd):
    out = []
    union = KEY_POOLS["dims"].get(dimtbl)
    for i in range(n):
        o = owners[i] if i < len(owners) else None
        p = (o["dims"].get(dimtbl) if o else None) or union
        out.append(rnd.choice(p) if p else rnd.randint(lo, hi))
    return out


def date_window_report(table, col, vals, prov):
    """One line: which window each row was dated from and how many of the whole
    append landed inside it. `0/N in window` is the structural no-op, stated at
    append time instead of three minutes later in the gateway's refusal."""
    parts = []
    for label, (pool, drawn, lo, hi) in prov.items():
        if not pool:
            parts.append("%s %d rows spread %d..%d — NO query window; a query whose MV "
                         "bakes a date filter outside that span merges 0 of them"
                         % (label, drawn, lo, hi))
            continue
        s = set(pool)
        parts.append("%s [%d..%d] %d drawn, %d/%d of the append in window"
                     % (label, min(pool), max(pool), drawn,
                        sum(1 for v in vals if v in s), len(vals)))
    return "   %s.%s: %s" % (table, col, "; ".join(parts) or "no rows")


# DEPRECATED shims — the generator now uses draw_dim / draw_dates with a row
# OWNER so a row's keys all come from one query. These keep the pre-2026-09-19
# signatures working for an external tick script (CDC_TICK_CMD) that imported
# them; they can only ever produce the union behaviour.
def pool_pick(dimtbl, n, lo, hi, rnd):
    """n keys for dimtbl: from its pool when one exists, else uniform in [lo, hi]."""
    return draw_dim(dimtbl, n, [None] * n, lo, hi, rnd)

def pool_dates(n, date_lo, date_hi, rnd):
    return draw_dates("", n, [None] * n, date_lo, date_hi, rnd)

def dim_for(col):
    """(dimension_table, key_column) a fact column references, or None."""
    if not col.endswith("_sk"):
        return None
    for suffix, dim in DIM_BY_SUFFIX:
        if col.endswith(suffix):
            return dim
    return None

def own_key_of(table):
    """The surrogate key a DIMENSION issues for its own rows, or None.

    dim_for() resolves FACT columns by suffix, so it cannot name a dimension's
    own key when that key does not carry the suffix: "hd_demo_sk" ends in
    "_demo_sk", not "hdemo_sk", and likewise "cd_demo_sk". Both callers that
    ask "which of this table's columns is its PK" were using dim_for() and got
    None, so the demographics dims never issued fresh keys — gen_table_cols
    filled the PK with int noise and append_table skipped its bounds lookup.

    Measured on the node 2026-09-21: household_demographics held 62,320 rows
    over 7,200 distinct hd_demo_sk (8.66x duplication, max never past 7,200),
    and its 6-row tick appends carried random keys in 25..943. A re-used key is
    reachable from pre-append facts, so the RI-prune gate must refuse the dim
    delta and the merge pays a fact scan instead of a metadata comparison —
    q72's chart merge ran 95-170 s for 785 delta rows.
    """
    return next((k for _s, (t, k) in DIM_BY_SUFFIX if t == table), None)


def dims_needed(columns):
    """Every dimension table the given fact columns reference, deduped."""
    out = []
    for c in columns:
        d = dim_for(c)
        if d and d[0] not in out:
            out.append(d[0])
    return out


def _q(x):
    """Round to decimal(7,2) — the physical type of every TPC-DS fact measure."""
    return decimal.Decimal(str(round(float(x), 2)))


def gen_fact_cols(fact, columns, n, *, date_lo, date_hi, dim_hi, key_base, rnd=random):
    """Generate EVERY column of `fact` — no null-filled column, ever again.

    `columns` is the fact's physical column list as (name, kind), taken from the
    LIVE table schema; `dim_hi` maps dimension table -> max surrogate key read
    from the catalog; `key_base` is the first ticket/order number to issue.

    Values are internally consistent (ext_* = quantity * unit,
    net_paid = ext_sales_price - coupon_amt, net_paid_inc_tax = net_paid +
    ext_tax) because the MVs aggregate these together: an incoherent delta merges
    to a number verification would reject even when the row count did move."""
    names = [c[0] for c in columns]
    out = {}

    def hi(dimtbl):
        return dim_hi.get(dimtbl) or FALLBACK_DIM_HI.get(dimtbl) or 1000

    # --- keys -------------------------------------------------------------
    # Each row belongs to ONE query's predicate box (see KEY_POOLS): its date
    # AND its dimension keys come from the same query, so a query with several
    # predicates gets rows that satisfy all of them together.
    owners, _qs = row_owners(names, n)
    # One primary date per row drives every other date on that row. The primary
    # is the fact's own sold/returned date — the column a query's date filter
    # almost always binds — identified as the date column that is not a ship
    # date, never by name.
    primary = next((c for c, _ in columns
                    if (dim_for(c) or ("",))[0] == "date_dim" and "ship" not in c), None)
    prov = {}
    sold = (draw_dates(primary, n, owners, date_lo, date_hi, rnd, prov) if primary
            else [rnd.randint(date_lo, date_hi) for _ in range(n)])
    if primary:
        DATE_REPORTS.append(date_window_report(fact, primary, sold, prov))
    for name, kind in columns:
        d = dim_for(name)
        if d is None:
            continue
        dimtbl = d[0]
        if dimtbl == "date_dim":
            if "ship" not in name:
                out[name] = list(sold)          # sold / returned: the primary date
                continue
            # A ship date TRAILS the sale. When a query constrains this exact
            # column it gets its own window, but only the part of it that is
            # still after the sale — q72 itself requires d3.d_date > d1.d_date+5,
            # so a ship date drawn independently of the sale would be filtered
            # out by the query even though it sits inside the window.
            v, sprov, _sorted = [], {}, {}
            for i in range(n):
                o = owners[i]
                p = (o["date_by_col"].get(name) if o else None) or KEY_POOLS["date_by_col"].get(name)
                cand = None
                if p:
                    # bisect a sorted copy instead of filtering the pool per row:
                    # the naive scan is O(rows x pool) and a 50k-row append
                    # against a 20k-key pool is a billion comparisons.
                    key = id(p)
                    sp = _sorted.get(key)
                    if sp is None:
                        sp = _sorted[key] = sorted(p)
                    j = bisect.bisect_right(sp, sold[i])
                    cand = sp[j:] if j < len(sp) else None
                if cand:
                    x = cand[rnd.randrange(len(cand))]
                    v.append(x)
                    _note(sprov, o["name"] if o and o["date_by_col"].get(name)
                          else "(wave union for this column)", p, x)
                else:
                    x = sold[i] + rnd.randint(2, 90)
                    v.append(x)
                    _note(sprov, "(sale + 2..90 days)", None, x)
            out[name] = v
            if any(e[0] for e in sprov.values()):
                DATE_REPORTS.append(date_window_report(fact, name, v, sprov))
        elif dimtbl == "time_dim":
            out[name] = [rnd.randint(0, hi("time_dim")) for _ in range(n)]
        else:
            h = hi(dimtbl)
            out[name] = draw_dim(dimtbl, n, owners, 1, h, rnd)

    keycol = FACT_KEY_COL.get(fact)
    if keycol and keycol in names:
        out[keycol] = [key_base + i for i in range(n)]

    # --- quantity + the money model --------------------------------------
    qty_col = next((c for c in names if c.endswith("_quantity")), None)
    qty = [rnd.randint(1, 100) for _ in range(n)]
    if qty_col:
        out[qty_col] = qty

    if not fact.endswith("_returns"):
        # QUANTIZE THE INDEPENDENT MEASURES FIRST, then derive the composites
        # from the quantized values. Rounding each column independently made
        # net_paid_inc_tax differ from net_paid + ext_tax by a cent per row —
        # which at 50k rows is a ~£250 discrepancy the MV verifier would flag
        # against a source that computes the composite exactly.
        # Ranges MEASURED off the real SF1 store_sales manifest bounds
        # (2026-08-04): wholesale_cost 1.00..100.00, list_price 1.00..200.00,
        # sales_price 0.00..199.56, ext_sales_price 0..19308, ext_list_price
        # ..19984. A markup of up to 3x put list_price at 299 and
        # ext_sales_price at 27,823 — outside anything the real fact contains,
        # which silently biases every query with a literal value band
        # (q13's ss_sales_price BETWEEN 50 AND 150, q28's list-price bands,
        # q48's net-profit bands): the delta lands mostly OUTSIDE the band the
        # MV filters on, so the merge sees far fewer rows than it should.
        Q = lambda xs: [_q(x) for x in xs]
        wholesale = Q(rnd.uniform(1, 100) for _ in range(n))
        listp = Q(float(w) * rnd.uniform(1.0, 2.0) for w in wholesale)
        salesp = Q(float(l) * rnd.uniform(0.5, 1.0) for l in listp)
        ext_sales = Q(q * float(s) for q, s in zip(qty, salesp))
        ext_list = Q(q * float(l) for q, l in zip(qty, listp))
        ext_whole = Q(q * float(w) for q, w in zip(qty, wholesale))
        ext_tax = Q(float(es) * 0.07 for es in ext_sales)
        coupon = Q(float(es) * rnd.uniform(0, 0.2) for es in ext_sales)
        ext_ship = Q(q * rnd.uniform(0, 10) for q in qty)
        net_paid = [es - c for es, c in zip(ext_sales, coupon)]
        money = {
            "wholesale_cost": wholesale, "list_price": listp, "sales_price": salesp,
            "ext_sales_price": ext_sales, "ext_list_price": ext_list,
            "ext_wholesale_cost": ext_whole,
            "ext_discount_amt": [el - es for el, es in zip(ext_list, ext_sales)],
            "ext_tax": ext_tax, "coupon_amt": coupon, "ext_ship_cost": ext_ship,
            "net_paid": net_paid,
            "net_paid_inc_tax": [p + t for p, t in zip(net_paid, ext_tax)],
            "net_paid_inc_ship": [p + s for p, s in zip(net_paid, ext_ship)],
            "net_paid_inc_ship_tax": [p + s + t for p, s, t
                                      in zip(net_paid, ext_ship, ext_tax)],
            "net_profit": [p - w for p, w in zip(net_paid, ext_whole)],
        }
    else:
        # A standalone returns row still needs coherent money. When generated
        # REFERENTIALLY (gen_referential_returns) these are recomputed from the
        # parent sale instead — that is the path the benched joins need.
        amt = [_q(q * rnd.uniform(1, 300)) for q in qty]
        money = _returns_money(
            amt, [_q(float(a) * 0.07) for a in amt],
            [_q(rnd.uniform(0, 100)) for _ in range(n)],
            [_q(rnd.uniform(0, 50)) for _ in range(n)],
            [_q(float(a) * rnd.uniform(0, 0.6)) for a in amt], rnd)

    for name, kind in columns:
        if name in out or kind != "d":
            continue
        bare = name.split("_", 1)[1]      # strip the ss_/sr_/cs_/cr_/ws_/wr_ prefix
        vals = money.get(bare)
        if vals is None:
            # An unrecognised decimal must NEVER be left out: a null-filled
            # measure is the exact failure this module exists to prevent.
            print(f"   WARN: {fact}.{name} has no money model — filling with noise")
            vals = [rnd.uniform(1, 1000) for _ in range(n)]
        out[name] = [_q(v) for v in vals]

    # anything still unset (an int that is neither a key nor the quantity)
    for name, kind in columns:
        if name not in out:
            out[name] = [rnd.randint(1, 100) for _ in range(n)]

    missing = [c for c in names if c not in out]
    assert not missing, f"{fact}: generator left {missing} unset"
    return out


def _returns_money(amt, tax, fee, ship, cash, rnd):
    """The shared refund model, all inputs already quantized to decimal(7,2) so
    the composite columns are EXACT sums of their parts (a per-column rounding
    drift would make the MV disagree with the source by a cent per row).

    cash<=0.6*amt and rev<=0.5*(amt-cash) keep store_credit/account_credit
    non-negative and keep total refunds well under the sale, so q64's
    `sum(ext_list_price) > 2*sum(refunds)` HAVING band still admits the groups."""
    rev = [_q(float(a - c) * rnd.uniform(0, 0.5)) for a, c in zip(amt, cash)]
    credit = [a - c - r for a, c, r in zip(amt, cash, rev)]
    return {
        "return_amt": amt, "return_amount": amt, "return_tax": tax,
        "return_amt_inc_tax": [a + t for a, t in zip(amt, tax)],
        "fee": fee, "return_ship_cost": ship, "refunded_cash": cash,
        "reversed_charge": rev, "store_credit": credit, "account_credit": credit,
        "net_loss": [a + f + s for a, f, s in zip(amt, fee, ship)],
    }


def gen_referential_returns(sales_fact, sales_cols, returns_columns, m, *,
                            date_lo, date_hi, dim_hi, rnd=random):
    """Returns rows that actually reference the sales rows just appended.

    Every returns fact joins its parent on (item_sk, ticket/order number). Rows
    generated INDEPENDENTLY of the sales (the old behaviour for store_returns,
    and web_returns had no generator at all) never match, so the delta merge
    scans both facts and produces nothing — measured on test2 2026-08-04 as
    q24's 15,233 ms, 0-row "merge". Dimension keys, dates and money are all
    derived from the parent sale so the pair is internally consistent."""
    rfact, keypairs = RETURNS_OF[sales_fact]
    names = [c[0] for c in returns_columns]
    n = len(next(iter(sales_cols.values())))
    m = max(0, min(m, n))
    idx = rnd.sample(range(n), m)

    # Start from a standalone generation so no column can be missed, then
    # overwrite everything that must agree with the parent sale. The standalone
    # generation's date report describes dates that are about to be replaced by
    # the parent's, so drop it and restate below.
    _mark = len(DATE_REPORTS)
    out = gen_fact_cols(rfact, returns_columns, m, date_lo=date_lo, date_hi=date_hi,
                        dim_hi=dim_hi, key_base=1, rnd=rnd)
    del DATE_REPORTS[_mark:]

    for scol, rcol in keypairs:
        if rcol in out and scol in sales_cols:
            out[rcol] = [sales_cols[scol][i] for i in idx]

    # Dimension keys shared with the sale, matched by which dimension they point
    # at — so cs_bill_customer_sk feeds cr_refunded_customer_sk and
    # cr_returning_customer_sk. A returns-only dimension (reason) keeps its own.
    by_dim_sales = {}
    for c in sales_cols:
        d = dim_for(c)
        if d and d[0] not in ("date_dim", "time_dim"):
            by_dim_sales.setdefault(d[0], c)
    for rcol in names:
        d = dim_for(rcol)
        if d and d[0] not in ("date_dim", "time_dim") and d[0] in by_dim_sales:
            src = by_dim_sales[d[0]]
            out[rcol] = [sales_cols[src][i] for i in idx]

    # returned date: strictly after the sale
    sold_col = next((c for c in sales_cols if c.endswith("_sold_date_sk")), None)
    if sold_col:
        for rcol in names:
            if rcol.endswith("_returned_date_sk"):
                out[rcol] = [sales_cols[sold_col][i] + rnd.randint(1, 60) for i in idx]
                if out[rcol]:
                    DATE_REPORTS.append(
                        "   %s.%s: parent sale date + 1..60 days (%d..%d) — a query "
                        "that filters the RETURN date needs its window to extend past "
                        "the sale window" % (rfact, rcol, min(out[rcol]), max(out[rcol])))

    # money: refund a subset of what was actually paid on that sale
    sqty = next((c for c in sales_cols if c.endswith("_quantity")), None)
    sprice = next((c for c in sales_cols
                   if c.endswith("_sales_price") and "_ext_" not in c), None)
    if sqty and sprice and m:
        rq = [max(1, min(sales_cols[sqty][i], rnd.randint(1, 10))) for i in idx]
        unit = [float(sales_cols[sprice][i]) for i in idx]
        amt = [_q(q * u) for q, u in zip(rq, unit)]
        derived = _returns_money(amt, [_q(float(a) * 0.07) for a in amt],
                                 [_q(rnd.uniform(0, 20)) for _ in range(m)],
                                 [_q(rnd.uniform(0, 10)) for _ in range(m)],
                                 [_q(float(a) * rnd.uniform(0, 0.6)) for a in amt],
                                 rnd)
        derived["return_quantity"] = rq
        for name, kind in returns_columns:
            bare = name.split("_", 1)[1]
            if bare in derived:
                v = derived[bare]
                out[name] = [_q(x) for x in v] if kind == "d" else [int(x) for x in v]
    return out


# ---- backwards-compatible thin wrappers (older tests / callers) ------------
# SF1000's base store_sales tops out at ss_ticket_number=240,000,000 (measured).
# Only a default for the pure-python wrapper: the real append reads the CURRENT
# max from the catalog so repeat appends never reissue a ticket.
TICKET_BASE = 240_000_001

def gen_store_sales_cols(n, date_lo, date_hi, store_pool, ticket_base=TICKET_BASE,
                         rnd=random, dim_hi=None):
    cols = gen_fact_cols("store_sales", FACT_COLUMNS["store_sales"], n,
                         date_lo=date_lo, date_hi=date_hi,
                         dim_hi=dim_hi or {}, key_base=ticket_base, rnd=rnd)
    if store_pool and not KEY_POOLS["dims"].get("store"):
        cols["ss_store_sk"] = list(store_pool)
    return cols

def gen_referential_store_returns_cols(sales, m, rnd=random, dim_hi=None,
                                       date_lo=2451545, date_hi=2452640):
    return gen_referential_returns("store_sales", sales,
                                   FACT_COLUMNS["store_returns"], m,
                                   date_lo=date_lo, date_hi=date_hi,
                                   dim_hi=dim_hi or {}, rnd=rnd)


def plan_tick(base_rows, ratios=None, returns_ratio=DEFAULT_RETURNS_RATIO):
    """Rows to append to each of the six facts for one realistic CDC tick.

    Returns an ordered list of (sales_fact, sales_rows, returns_fact, returns_rows).
    Default at base_rows=50000: store 50000/5000, catalog 25000/2500,
    web 12500/1250 — the 4:2:1 sales mix with returns at 10% of their parent."""
    ratios = DEFAULT_RATIOS if ratios is None else ratios
    plan = []
    for f in SALES_FACTS:
        r = ratios.get(f)
        if not r:
            continue
        ns = int(round(base_rows * r))
        if ns <= 0:
            continue
        plan.append((f, ns, RETURNS_OF[f][0], int(round(ns * returns_ratio))))
    return plan


def parse_ratios(s):
    """'store_sales=1,catalog_sales=0.5,web_sales=0.25' -> dict."""
    if not s:
        return dict(DEFAULT_RATIOS)
    out = {}
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        k, _, v = part.partition("=")
        k = k.strip()
        if k not in SALES_FACTS:
            raise ValueError(f"--ratios: unknown fact {k!r} (want one of {SALES_FACTS})")
        out[k] = float(v)
    return out


# ===========================================================================
# Catalog-facing code below (needs pyiceberg / pyarrow).
# ===========================================================================
STEP_T = {}   # step -> seconds, summed over the tick (printed in the tick timing line)


def _step(name, t0):
    import time as _t
    STEP_T[name] = STEP_T.get(name, 0.0) + (_t.time() - t0)


# BOUNDS CACHE, keyed by the table's snapshot. catalog_bounds reads EVERY
# manifest of the table; the tick calls it for every table on every run, and a
# table gains one manifest per tick, so the cost grew without bound: 190.7 s of
# a 322 s tick on node 37 (2026-09-24). The (lo, hi) of a column only moves
# when rows are added, and the tick itself adds them — so the cache is updated
# from the appended rows (_bounds_after_append) and stamped with the snapshot
# that append produced. Any other writer moves the snapshot, which misses the
# cache and falls back to the manifest read. Integers only (keys, date_sk).
_BOUNDS_PATH = os.path.expanduser(os.environ.get("SEED_BOUNDS_CACHE", "~/.seed_bounds_cache.json"))
_BOUNDS = None


def _bounds_load():
    global _BOUNDS
    if _BOUNDS is None:
        try:
            import json as _j
            _BOUNDS = _j.load(open(_BOUNDS_PATH))
        except Exception:
            _BOUNDS = {}
    return _BOUNDS


def _bounds_save():
    try:
        import json as _j
        tmp = _BOUNDS_PATH + ".tmp"
        _j.dump(_BOUNDS or {}, open(tmp, "w"))
        os.replace(tmp, _BOUNDS_PATH)
    except Exception:
        pass


def _snap_id(t):
    cs = t.current_snapshot()
    return cs.snapshot_id if cs else None


def catalog_bounds(cat, namespace, table, col, min_rows=0):
    import time as _t
    _t0 = _t.time()
    try:
        key = "%s.%s|%s|%d" % (namespace, table, col, min_rows)
        try:
            snap = _snap_id(cat.load_table((namespace, table)))
        except Exception:
            snap = None
        c = _bounds_load().get(key)
        if snap is not None and c and c.get("snap") == snap:
            STEP_T["catalog_bounds_cached"] = STEP_T.get("catalog_bounds_cached", 0) + 1
            return c.get("lo"), c.get("hi")
        lo, hi = _catalog_bounds(cat, namespace, table, col, min_rows)
        if snap is not None and isinstance(lo, int) and isinstance(hi, int):
            _bounds_load()[key] = {"snap": snap, "lo": lo, "hi": hi}
            _bounds_save()
        return lo, hi
    finally:
        _step("catalog_bounds", _t0)


def _bounds_after_append(t, data, n):
    """Fold the appended rows into every cached bound of this table and stamp
    the snapshot the append produced, so the next tick hits the cache."""
    try:
        import pyarrow.compute as pc
        ns, tbl = t.name()[-2], t.name()[-1]
        snap = _snap_id(t)
        prefix = "%s.%s|" % (ns, tbl)
        b = _bounds_load()
        for key, c in list(b.items()):
            if not key.startswith(prefix):
                continue
            _, col, mr = key.split("|")
            if int(mr) and n < int(mr):
                c["snap"] = snap      # a small file is ignored by this bound
                continue
            if col not in data.column_names:
                del b[key]
                continue
            mm = pc.min_max(data.column(col)).as_py()
            lo, hi = mm.get("min"), mm.get("max")
            if not isinstance(lo, int) or not isinstance(hi, int):
                del b[key]
                continue
            c["lo"] = lo if c.get("lo") is None else min(c["lo"], lo)
            c["hi"] = hi if c.get("hi") is None else max(c["hi"], hi)
            c["snap"] = snap
        _bounds_save()
    except Exception:
        pass


def _catalog_bounds(cat, namespace, table, col, min_rows=0):
    """(min, max) of `col` read from ICEBERG MANIFEST STATISTICS — no data scan.

    Measured on test2 SF1000 (2026-08-04): 0.01-0.22s per table even for the
    450-file store_sales, because it only reads manifest lower/upper bounds.
    Returns (None, None) when the table or its stats are unavailable, so every
    caller must have a fallback."""
    try:
        from pyiceberg.conversions import from_bytes
        t = cat.load_table((namespace, table))
        f = t.schema().find_field(col)
        lo = hi = None
        for task in t.scan().plan_files():
            df = task.file
            # min_rows: ignore small files (earlier bench ticks; one 5,000-row
            # seed file spans d_date_sk 2415022..2488070 on the node) when the
            # caller wants the BASE data's bounds, e.g. the dataset's "now".
            if min_rows and (df.record_count or 0) < min_rows:
                continue
            b = (df.lower_bounds or {}).get(f.field_id)
            if b is not None:
                v = from_bytes(f.field_type, b)
                lo = v if lo is None else min(lo, v)
            b = (df.upper_bounds or {}).get(f.field_id)
            if b is not None:
                v = from_bytes(f.field_type, b)
                hi = v if hi is None else max(hi, v)
        return lo, hi
    except Exception:
        return None, None


def scan_dim_hi(cat, namespace, table, col):
    import time as _t
    _t0 = _t.time()
    try:
        return _scan_dim_hi(cat, namespace, table, col)
    finally:
        _step("scan_dim_hi", _t0)


def _scan_dim_hi(cat, namespace, table, col):
    """max(col) read from the DATA, for when manifest statistics are missing.

    Iceberg only records lower/upper bounds when the parquet files carry column
    statistics; files written without them leave catalog_bounds() blind (the
    same condition that prints "statistics missing for column N"). Scanning is
    the difference between a bound that is EXACT AT ANY SCALE FACTOR and a
    constant that is only right at the one scale it was measured on.

    Projects a SINGLE key column of a DIMENSION table, and streams it: the max
    is folded batch by batch so peak memory is one batch, not one column. That
    matters at the top of the range — customer is 2M rows at SF100 but 65M at
    SF10000 and 100M at SF100000, where materialising the column would be
    hundreds of MB for a single number."""
    try:
        t = cat.load_table((namespace, table))
        scan = t.scan(selected_fields=(col,))

        def _fold(colv, best):
            try:
                import pyarrow.compute as pc
                v = pc.max(colv).as_py()
            except Exception:
                # No pyarrow.compute, or a non-arrow column.
                vals = [x for x in colv.to_pylist() if x is not None]
                v = max(vals) if vals else None
            return v if best is None else (best if v is None else max(best, v))

        best = None
        reader = getattr(scan, "to_arrow_batch_reader", None)
        if reader is not None:
            try:
                for batch in reader():
                    if batch.num_rows:
                        best = _fold(batch.column(0), best)
                return int(best) if best is not None else None
            except Exception:
                best = None  # fall through to the whole-column read
        arrow = scan.to_arrow()
        if arrow.num_rows == 0:
            return None
        best = _fold(arrow.column(col), best)
        return int(best) if best is not None else None
    except Exception:
        return None


def load_dim_hi(cat, namespace, dimtables, verbose=True):
    """dimension table -> max surrogate key, read from the LIVE catalog.

    This is what makes the seeder scale-free. A key drawn above a dimension's
    real max cannot join it, and the old hardcoded ranges were both too high for
    SF1000 (ss_promo_sk 1..1800 vs promotion's real 1..1500) and wildly too high
    for SF1 (item 1..300000 vs 18000) — silently dropping ~20% and ~94% of the
    delta's joins respectively."""
    out = {}
    for d in dimtables:
        keycol = next((k for s, (t, k) in DIM_BY_SUFFIX if t == d), None)
        lo, hi = catalog_bounds(cat, namespace, d, keycol) if keycol else (None, None)
        src = "stats"
        if hi is None and keycol:
            # Manifest stats missing (not the table). Read the real max instead
            # of guessing: a scanned bound is correct at SF1, SF10, SF100 and
            # SF1000 alike, where the constant below is correct at exactly one.
            hi = scan_dim_hi(cat, namespace, d, keycol)
            src = "scan"
        if hi is None:
            hi = FALLBACK_DIM_HI.get(d)
            src = "SF1000 constant"
            if verbose:
                print(f"   WARN: {d}.{keycol} unreadable by stats AND by scan; "
                      f"falling back to the SF1000 constant {hi} — at a smaller "
                      f"scale factor most appended keys will NOT join")
        elif src == "scan" and verbose:
            print(f"   {d}.{keycol} <= {hi} (scanned; manifest stats missing)")
        out[d] = hi
    if verbose and out:
        print("   dim key ranges from catalog: "
              + ", ".join(f"{k}<={v}" for k, v in sorted(out.items())))
    return out


def load_geo_pairs(cat, namespace, verbose=True):
    """(customer_sk, addr_sk, store_sk) triples where the STORE IS IN THE
    CUSTOMER'S OWN ZIP — i.e. people shop near where they live.

    WHY: q24 requires `s_zip = ca_zip AND s_market_id = 8`. Drawing
    ss_customer_sk and ss_store_sk independently makes that pair essentially
    unreachable. MEASURED on the SF1000 catalog (2026-08-04): 1002 stores, 84 of
    them in market 8 covering 67 distinct zips; 6.78% of the 6,000,000 addresses
    sit in one of those zips, so a uniformly-random (customer, store) pair passes
    with probability 8.46e-05 — 0.42 expected rows in a 5,000-row store_returns
    delta, and ~11,821 rows needed for ONE expected hit. That is why q24's merge
    reported 11,408 ms over a 0-row delta part even after every column, date and
    dimension range had been fixed: the remaining defect was the INDEPENDENCE of
    two columns, not the value of either one.

    Done entirely in Arrow (two hash joins over projected key columns, no python
    dicts) so the 12M-row customer scan stays well under 200 MB.
    Returns None if anything is unavailable — the caller falls back to
    independent draws and says so."""
    try:
        import pyarrow as pa
        st = cat.load_table((namespace, "store")).scan(
            selected_fields=("s_store_sk", "s_zip")).to_arrow()
        ca = cat.load_table((namespace, "customer_address")).scan(
            selected_fields=("ca_address_sk", "ca_zip")).to_arrow()
        cu = cat.load_table((namespace, "customer")).scan(
            selected_fields=("c_customer_sk", "c_current_addr_sk")).to_arrow()
        cu_ca = cu.join(ca, keys="c_current_addr_sk", right_keys="ca_address_sk",
                        join_type="inner")
        pairs = cu_ca.join(st, keys="ca_zip", right_keys="s_zip", join_type="inner")
        cust = pairs["c_customer_sk"].to_pylist()
        addr = pairs["c_current_addr_sk"].to_pylist()
        store = pairs["s_store_sk"].to_pylist()
        if not cust:
            return None
        if verbose:
            print(f"   geo-correlated (customer,address,store) pairs sharing a zip: "
                  f"{len(cust)} — appended store_sales will be placed at a store in "
                  "the customer's own zip (q24 needs s_zip = ca_zip)")
        return cust, addr, store
    except Exception as e:
        if verbose:
            print(f"   WARN: geo correlation unavailable ({type(e).__name__}: {e}); "
                  "customer and store will be drawn INDEPENDENTLY, which makes "
                  "q24-class (s_zip = ca_zip) deltas effectively unreachable")
        return None


def conform_to_table_schema(tbl, table, n, strict=True):
    """Expand a synthesized delta to the target table's FULL physical schema.

    WITHOUT this the delta parquet carried only the ~12 measure/key columns; a
    query that reads the fact as a raw base+delta parquet glob (q64's cross_sales
    scans store_sales for ss_cdemo_sk etc.) then hits "schema mismatch in glob:
    column X was read from the original file ... but could not be found in
    <seed>.parquet" -> duckdb exit 1. Conforming to table.schema() makes every
    delta file identical in shape to the base chunks.

    strict=True additionally REFUSES to null-fill. With the schema-driven
    generator every physical column is synthesized, so a null-fill here means a
    column was missed — and a NULL join key silently empties the delta, which is
    precisely the bug that made three measured merges report a time for 0 rows.
    It must be loud, not silent."""
    import pyarrow as pa
    from pyiceberg.io.pyarrow import schema_to_pyarrow
    aschema = schema_to_pyarrow(table.schema())
    # schema_to_pyarrow stamps PARQUET:field_id on every field, and pq.write_table
    # then writes those IDs into the file — which add_files refuses outright:
    # "Cannot add file ... because it has field IDs. `add_files` only supports
    # addition of files without field_ids". That aborted the CDC append for all
    # five facts (AWS SF1, 2026-08-01), so every delta-merge measured 0 rows.
    # Only the metadata is a problem; names/types/nullability are what both the
    # physical glob and the iceberg-schema read actually need, so keep those.
    aschema = pa.schema([pa.field(f.name, f.type, f.nullable) for f in aschema])
    have = {name: tbl[name] for name in tbl.column_names}
    cols, filled = [], []
    for field in aschema:
        if field.name in have:
            arr = have[field.name]
            if not arr.type.equals(field.type):
                # CLAMP A DECIMAL INTO THE TARGET PRECISION BEFORE CASTING.
                #
                # The row generator emits every decimal as decimal(7,2) (see the
                # kinds map), but the real Iceberg schema has narrower decimals —
                # ca_gmt_offset / s_gmt_offset / w_gmt_offset are decimal(5,2),
                # max 999.99. A (7,2) value above that made arr.cast() raise
                # "Decimal value does not fit in precision 5", which killed the
                # CDC tick outright: "CDC TICK FAILED (exit 1) — no rows added",
                # so EVERY merge measurement was skipped (2026-09-11).
                #
                # Generic: derived from the target type, no column names.
                if pa.types.is_decimal(field.type) and pa.types.is_decimal(arr.type):
                    import decimal as _dec
                    lim = _dec.Decimal(10) ** (field.type.precision - field.type.scale)
                    q = _dec.Decimal(1).scaleb(-field.type.scale)
                    def _fit(v):
                        if v is None:
                            return None
                        v = _dec.Decimal(v)
                        if v >= lim:
                            v = lim - q
                        elif v <= -lim:
                            v = -lim + q
                        return v.quantize(q, rounding=_dec.ROUND_DOWN)
                    arr = pa.array([_fit(v) for v in arr.to_pylist()], type=field.type)
                else:
                    try:
                        arr = arr.cast(field.type)
                    except Exception as e:
                        raise SystemExit(
                            "FATAL: cannot cast column %s of %s from %s to %s: %s"
                            % (field.name, table.name(), arr.type, field.type, e))
            cols.append(arr)
        else:
            filled.append(field.name)
            cols.append(pa.nulls(n, field.type))
    if filled:
        msg = (f"NULL-FILLED {len(filled)} column(s) of {table.name()}: {filled}. "
               "A NULL join key silently empties the delta — every merge measured "
               "against it reports a time for zero rows.")
        if strict:
            raise SystemExit("FATAL: " + msg)
        print("   WARN: " + msg)
    return pa.table(cols, schema=aschema)


def _pa_schema(columns):
    import pyarrow as pa
    m = {"i": pa.int32(), "l": pa.int64(), "d": pa.decimal128(7, 2),
         "s": pa.string(), "t": pa.date32()}
    return pa.schema([(n, m[k]) for n, k in columns])


def _columns_of(t, fact):
    """The fact's physical columns as (name, kind) — from the LIVE table schema
    when it exists (so a schema-evolved table still gets every column) and from
    the canonical TPC-DS list otherwise."""
    if t is None:
        return list(FACT_COLUMNS[fact])
    out = []
    for f in t.schema().fields:
        s = str(f.field_type)
        kind = ("l" if s == "long" else "d" if s.startswith("decimal")
                else "s" if s == "string" else "t" if s == "date" else "i")
        out.append((f.name, kind))
    return out


def _write_and_add(fs, t, data, n, label, strict=True):
    """Conform, write the delta parquet ourselves, and add_files it."""
    import uuid, pyarrow as pa, pyarrow.parquet as pq
    data = conform_to_table_schema(data, t, n, strict=strict)
    key = f"{t.location().rstrip('/')}/data/seed-{uuid.uuid4().hex}.parquet"
    # write_statistics=False: historically the delta carried all-null decimal
    # columns whose parquet stats have min_raw=None, and pyiceberg's stats reader
    # calls Decimal(None) on that unguarded -> "conversion from NoneType to
    # Decimal is not supported" (pyiceberg/io/pyarrow.py:2344), aborting
    # add_files. The generator no longer emits all-null columns, but the delta
    # files are tiny so the lost row-group pruning still costs nothing.
    #
    # store_decimal_as_integer matches the base tables' physical encoding: the
    # default FIXED_LEN_BYTE_ARRAY clashes with a table registered from
    # int32-backed parquet ("Unexpected physical type FIXED_LEN_BYTE_ARRAY ...
    # expected INT32", pyiceberg 0.9).
    # SELF-CHECK BEFORE THE APPEND IS VISIBLE. A delta with a NULL join key is
    # not a smaller delta, it is a delta the merge cannot see — and once it is
    # in the table there is nothing downstream that will tell you, because the
    # merge reports a normal merge_ms over zero rows. Refuse to publish one.
    nulls = {c: data[c].null_count for c in data.column_names if data[c].null_count}
    if nulls:
        raise SystemExit(f"FATAL: {label}: NULL values in {nulls} — a NULL join key "
                         "silently empties the delta; refusing to add_files")
    if data.num_rows != n:
        raise SystemExit(f"FATAL: {label}: built {data.num_rows} rows, expected {n}")
    # STATISTICS ON FOR THE KEY COLUMNS, OFF FOR THE DECIMALS.
    #
    # write_statistics=False everywhere was needed when the delta carried all-null
    # decimal columns: parquet writes their stats with min_raw=None and
    # pyiceberg's reader calls Decimal(None) on it unguarded ->
    # "conversion from NoneType to Decimal is not supported"
    # (pyiceberg/io/pyarrow.py:2344), aborting add_files.
    #
    # But blanket-off broke the thing that keeps successive appends from
    # colliding: the manifest then carries NO bounds for the appended files, so
    # catalog_bounds() below sees only the BASE table's max and every tick
    # reissues the previous tick's ticket/order numbers. Duplicate keys make the
    # sales x returns join fan out — a WRONG delta, which is worse than an empty
    # one because it looks like it worked.
    #
    # The generator no longer emits an all-null column, so the Decimal(None) path
    # is unreachable; keeping decimals excluded anyway costs nothing (the delta
    # files are tiny) and makes that impossible by construction.
    #
    # store_decimal_as_integer matches the base tables' physical encoding: the
    # default FIXED_LEN_BYTE_ARRAY clashes with a table registered from
    # int32-backed parquet ("Unexpected physical type FIXED_LEN_BYTE_ARRAY ...
    # expected INT32", pyiceberg 0.9).
    stat_cols = [f.name for f in data.schema if not pa.types.is_decimal(f.type)]
    if DRY_RUN:
        print(f"{label}: DRY RUN — built {n} rows, {data.num_columns} cols, 0 nulls; nothing written")
        return None
    import time as _t
    _t0 = _t.time()
    with fs.open(key.replace("s3://", "", 1), "wb") as f:
        pq.write_table(data, f, store_decimal_as_integer=True,
                       write_statistics=stat_cols)
    _step("write_parquet", _t0); _t0 = _t.time()
    # The key is seed-<uuid4>.parquet, written just above: it cannot already be
    # in the table. pyiceberg's duplicate check reads EVERY manifest of the
    # table to prove that, once per table per tick — O(ticks) and growing.
    t.add_files(file_paths=[key], check_duplicate_files=False); _step("add_files", _t0); _t0 = _t.time()
    t.refresh(); _step("refresh", _t0)
    _bounds_after_append(t, data, n)
    print(f"{label}: +{n} rows, {data.num_columns} cols, 0 nulls "
          f"-> snapshot {t.current_snapshot().snapshot_id}")
    return key


def _load_or_create(cat, namespace, fact):
    try:
        return cat.load_table((namespace, fact))
    except Exception:
        return cat.create_table((namespace, fact), schema=_pa_schema(FACT_COLUMNS[fact]))


def apply_geo_correlation(cols, fact, geo, rnd=random):
    """Overwrite (customer_sk, addr_sk, store_sk) with a zip-consistent triple.

    Only store_sales has a store to correlate against. Applied AFTER generation
    so every other column keeps its independent draw."""
    if not geo or fact != "store_sales":
        return 0
    cust, addr, store = geo
    n = len(cols["ss_customer_sk"])
    pick = [rnd.randrange(len(cust)) for _ in range(n)]
    cols["ss_customer_sk"] = [cust[i] for i in pick]
    cols["ss_addr_sk"] = [addr[i] for i in pick]
    cols["ss_store_sk"] = [store[i] for i in pick]
    return n


# CROSS-FACT correlation (2026-09-06). q29 joins store_sales ⋈ store_returns ⋈
# catalog_sales on the SAME customer and item — a customer who bought in the
# store, returned it, then bought it from the catalog. Facts appended
# independently never produce such a triple, so q29's delta terms fold to 0
# rows every tick (40s of proving emptiness). Every tick now records the
# (customer, item) pairs of the RETURNED sales and re-issues a share of them
# (CDC_CROSS_FACT, default 0.5) as the bill customer/item of the sales facts
# appended after it. CDC_CROSS_FACT=0 restores independent draws.
CROSS_PAIRS = []
CROSS_FRACTION = float(os.environ.get("CDC_CROSS_FACT", "0.5") or 0)


def _dim_col(names, dim, prefer=()):
    """First column among `names` pointing at dimension `dim`; a `prefer`
    substring (e.g. "bill_customer", "refunded_customer") wins when present."""
    hits = [c for c in names if (dim_for(c) or (None,))[0] == dim]
    for p in prefer:
        for c in hits:
            if p in c:
                return c
    return hits[0] if hits else None


def apply_cross_fact(cols, fact, rnd=random):
    """Overwrite a share of (customer, item) with pairs recorded from earlier
    returns in this tick. Only sales facts; nothing to do without pairs."""
    if not CROSS_PAIRS or CROSS_FRACTION <= 0 or fact not in SALES_FACTS:
        return 0
    ccol = _dim_col(list(cols), "customer", prefer=("bill_customer", "customer_sk"))
    icol = _dim_col(list(cols), "item")
    if not ccol or not icol:
        return 0
    n = len(cols[icol])
    m = int(n * min(1.0, CROSS_FRACTION))
    for i in rnd.sample(range(n), m):
        c, it = rnd.choice(CROSS_PAIRS)
        cols[ccol][i] = c
        cols[icol][i] = it
    return m


def record_cross_pairs(rcols):
    ccol = _dim_col(list(rcols), "customer", prefer=("refunded_customer", "customer_sk"))
    icol = _dim_col(list(rcols), "item")
    if not ccol or not icol:
        return 0
    pairs = [(c, i) for c, i in zip(rcols[ccol], rcols[icol]) if c is not None and i is not None]
    CROSS_PAIRS.extend(pairs)
    return len(pairs)


def append_fact(cat, fs, namespace, fact, n, *, date_lo, date_hi, dim_hi_cache,
                store_pool=None, returns_rows=0, strict=True, verbose=True,
                geo=None):
    """Append `n` fully-populated rows to `fact`, plus `returns_rows` REFERENTIAL
    rows to its returns fact. Returns the number of returns rows written."""
    import pyarrow as pa
    t = _load_or_create(cat, namespace, fact)
    columns = _columns_of(t, fact)

    need = [d for d in dims_needed([c[0] for c in columns]) if d not in dim_hi_cache]
    if need:
        dim_hi_cache.update(load_dim_hi(cat, namespace, need, verbose=verbose))

    # key base = current max + 1: never reuse a ticket/order number, never
    # collide with the base table or with a previous append.
    kb = 1
    keycol = FACT_KEY_COL.get(fact)
    if keycol:
        _, mx = catalog_bounds(cat, namespace, fact, keycol)
        kb = (mx or 0) + 1
        if verbose:
            print(f"   {fact}.{keycol}: current max={mx} -> issuing {kb}..{kb+n-1}")

    cols = gen_fact_cols(fact, columns, n, date_lo=date_lo, date_hi=date_hi,
                         dim_hi=dim_hi_cache, key_base=kb)
    if verbose:
        flush_date_reports()
    if apply_geo_correlation(cols, fact, geo) and verbose:
        print(f"   {fact}: customer/address/store drawn as zip-consistent triples")
    if (xm := apply_cross_fact(cols, fact)) and verbose:
        print(f"   {fact}: {xm} rows re-issue (customer, item) pairs returned earlier this tick (cross-fact)")
    if store_pool is not None:
        sc = _slice_col(columns)
        if sc:
            cols[sc] = list(store_pool)
    data = pa.table({c: cols[c] for c, _ in columns}, schema=_pa_schema(columns))
    _write_and_add(fs, t, data, n, f"{namespace}.{fact}", strict=strict)

    # PROVE the manifest can see the keys we just issued. If it cannot, the NEXT
    # append reads a stale max and reissues these exact ticket/order numbers, and
    # nothing downstream would ever tell you — the duplicate-key delta merges to a
    # plausible-looking wrong number.
    if keycol and verbose and not DRY_RUN:
        _, mx2 = catalog_bounds(cat, namespace, fact, keycol)
        if mx2 is None or mx2 < kb + n - 1:
            print(f"   WARN: {fact}.{keycol} manifest max is now {mx2}, expected "
                  f">= {kb+n-1} — the appended file carries no bounds, so the next "
                  "append WILL reissue these keys")

    if not returns_rows or fact not in RETURNS_OF:
        return 0
    rfact = RETURNS_OF[fact][0]
    rt = _load_or_create(cat, namespace, rfact)
    rcolumns = _columns_of(rt, rfact)
    need = [d for d in dims_needed([c[0] for c in rcolumns]) if d not in dim_hi_cache]
    if need:
        dim_hi_cache.update(load_dim_hi(cat, namespace, need, verbose=verbose))
    rcols = gen_referential_returns(fact, cols, rcolumns, returns_rows,
                                    date_lo=date_lo, date_hi=date_hi,
                                    dim_hi=dim_hi_cache)
    if verbose:
        flush_date_reports()
    rw = len(next(iter(rcols.values())))
    if (xp := record_cross_pairs(rcols)) and verbose:
        print(f"   {rfact}: {xp} (customer, item) pairs recorded for the cross-fact draw")
    rdata = pa.table({c: rcols[c] for c, _ in rcolumns}, schema=_pa_schema(rcolumns))
    _write_and_add(fs, rt, rdata, rw,
                   f"{namespace}.{rfact} (referential to {fact})", strict=strict)
    return rw


def load_string_domains(t, columns, *, sample_rows=50000, max_ratio=0.2, verbose=True):
    """column -> list of existing values, for every STRING column whose sampled
    distinct count is at most `max_ratio` of the sampled rows (a categorical
    attribute: i_category, ca_state, s_gmt_offset, p_channel_dmail ...).

    WHY: appended dimension rows used to get '<col>:<key>' in EVERY string column,
    unique per row. Each 5,000-row item append therefore added 5,000 brand-new
    categories, brands and classes: after a day of ticks the node's item table
    held 662,000 rows with 120,010 distinct i_category (the real domain is 10).
    That changes what the benchmark queries mean (categories no query names) and
    prices every grain over a dimension attribute at the fact size — q61's
    [ca_gmt_offset d_moy d_year i_category s_gmt_offset] branch was refused at an
    estimated 1.44B groups (node 1788402989672, 2026-09-05). Categorical columns
    now draw from the live domain; id-like columns (near-unique in the sample)
    keep the unique-per-row value. Returns {} when the table cannot be sampled."""
    import pyarrow.compute as pc
    scols = [c for c, k in columns if k == "s"]
    if t is None or not scols:
        return {}
    try:
        arr = t.scan(selected_fields=tuple(scols), limit=sample_rows).to_arrow()
    except Exception as e:
        if verbose:
            print(f"   WARN: string-domain sample unavailable ({e}); appended strings stay unique per row")
        return {}
    out = {}
    n = arr.num_rows
    if n <= 0:
        return out
    for c in scols:
        try:
            col = arr.column(c).drop_null()
            vals = pc.unique(col).to_pylist()
        except Exception:
            continue
        if not vals:
            continue
        if len(vals) <= max(1, int(n * max_ratio)):
            out[c] = vals
    if verbose and out:
        print("   string domains from the live table (categorical): "
              + ", ".join(f"{c}={len(v)}" for c, v in sorted(out.items())))
    return out


def gen_table_cols(table, columns, n, *, date_lo, date_hi, dim_hi, key_base, rnd=random,
                   str_domains=None):
    """Generate EVERY column of a NON-sales table (inventory, customer, item, ...)
    from its live schema. No per-table row template: the internal 9-table wave
    (bench/sf1/sf1000_append_test.py) hand-wrote 4-10 columns per table and let
    the gateway null-fill the rest; here every physical column is synthesized so
    the strict null guard holds for these tables too.

      own surrogate key (c_customer_sk on customer) -> key_base+i, never reused
      foreign _sk                                  -> uniform over the catalog range
      string, categorical (see load_string_domains) -> drawn from the live domain
      string, id-like                               -> '<col>:<key>' (unique per row)
      date / decimal / int / long                  -> in-range noise"""
    import datetime
    names = [c[0] for c in columns]
    out = {}

    def hi(dimtbl):
        return dim_hi.get(dimtbl) or FALLBACK_DIM_HI.get(dimtbl) or 1000

    d0 = datetime.date(1998, 1, 1)
    owners, _qs = row_owners(names, n)
    for name, kind in columns:
        d = dim_for(name)
        if name == own_key_of(table) or (d is not None and d[0] == table):
            out[name] = [key_base + i for i in range(n)]            # own PK
        elif d is not None and d[0] == "date_dim":
            prov = {}
            out[name] = draw_dates(name, n, owners, date_lo, date_hi, rnd, prov)
            DATE_REPORTS.append(date_window_report(table, name, out[name], prov))
        elif d is not None and d[0] == "time_dim":
            out[name] = [rnd.randint(0, hi("time_dim")) for _ in range(n)]
        elif d is not None:
            h = hi(d[0]); out[name] = draw_dim(d[0], n, owners, 1, h, rnd)
        elif kind == "s" and str_domains and str_domains.get(name):
            dom = str_domains[name]
            out[name] = [rnd.choice(dom) for _ in range(n)]
        elif kind == "s":
            out[name] = [f"{name}:{key_base + i}" for i in range(n)]
        elif kind == "t":
            out[name] = [d0 + datetime.timedelta(days=rnd.randint(0, 1800)) for _ in range(n)]
        elif kind == "d":
            out[name] = [_q(rnd.uniform(1, 1000)) for _ in range(n)]
        elif kind == "l":
            out[name] = [rnd.randint(1, 10**9) for _ in range(n)]
        else:
            out[name] = [rnd.randint(1, 1000) for _ in range(n)]
    missing = [c for c in names if c not in out]
    assert not missing, f"{table}: generator left {missing} unset"
    return out


def append_table(cat, fs, namespace, table, n, *, date_lo, date_hi, dim_hi_cache,
                 strict=True, verbose=True):
    """Append `n` fully-populated rows to any non-sales table that EXISTS in the
    catalog (dimensions and inventory). The table's own surrogate key continues
    from the catalog max so successive ticks never collide."""
    import pyarrow as pa
    try:
        t = cat.load_table((namespace, table))
    except Exception as e:
        raise SystemExit(f"FATAL: {namespace}.{table}: not in the catalog ({e}); "
                         "extra tables are appended, never created")
    columns = _columns_of(t, table)
    need = [d for d in dims_needed([c[0] for c in columns])
            if d != table and d not in dim_hi_cache]
    if need:
        dim_hi_cache.update(load_dim_hi(cat, namespace, need, verbose=verbose))
    kb = 1
    # A DIMENSION'S OWN KEY IS NOT A FACT COLUMN. dim_for() resolves FACT
    # columns by suffix (cs_bill_hdemo_sk -> household_demographics), and the
    # dimension's own key does not carry that suffix: "hd_demo_sk" ends in
    # "_demo_sk", not "hdemo_sk", so dim_for("hd_demo_sk") is None. keycol was
    # therefore None for household_demographics and customer_demographics, the
    # whole bounds block below was skipped — including the scan_dim_hi fallback
    # and its SystemExit guard — and kb stayed 1, so EVERY tick re-issued keys
    # from 1. Dims whose own key does match a suffix (i_item_sk, w_warehouse_sk,
    # d_date_sk) were never affected, which is why only the two *demo dims show
    # it.
    #
    # Measured on the node (2026-09-21): household_demographics held 62,320 rows
    # over 7,200 distinct hd_demo_sk (8.66x duplication, max never past 7200),
    # customer_demographics 1,979,760 over 1,920,800. A re-used key IS reachable
    # from pre-append facts, so the RI-prune gate must refuse the dim delta —
    # q72's merge then scanned catalog_sales x inventory for 785 delta rows and
    # took 95-170 s. Look the key up by VALUE, the way load_dim_hi already does.
    keycol = next((c for c, _ in columns if (dim_for(c) or ("",))[0] == table), None)
    if keycol is None:
        own = own_key_of(table)
        if own and any(c == own for c, _ in columns):
            keycol = own
    if keycol:
        _, mx = catalog_bounds(cat, namespace, table, keycol)
        if mx is None:
            # NEVER RE-ISSUE A DIMENSION KEY FROM 1.
            #
            # `kb = (mx or 0) + 1` turned a missing manifest bound into
            # key_base=1, so the append re-issued keys that already exist. That
            # breaks the premise the whole incremental design rests on, stated
            # in bench_cdc.sh: "seed_tpcds.py issues each dimension's surrogate
            # key as max(existing)+1 (never reused) ... that is the exact
            # premise the RI-prune gate asserts when it drops an insert-only dim
            # delta as zero-contribution". A duplicate key IS reachable from
            # pre-append facts, so the gate must refuse, and the merge pays a
            # full fact scan instead of a metadata comparison.
            #
            # Measured on q72 (node 37.27.65.188, 2026-09-20): the gate reported
            # "old catalog_sales rows reach cs_bill_hdemo_sk=7200, appended
            # household_demographics keys start at 1 — a probe decides", and the
            # tick spent 71.5 s scanning catalog_sales for a delta that should
            # have been discharged from manifests alone.
            #
            # scan_dim_hi() already solves this for the fact-FK path
            # (load_dim_hi); the dim-append path simply never called it.
            mx = scan_dim_hi(cat, namespace, table, keycol)
            if verbose and mx is not None:
                print(f"   {table}.{keycol}: manifest bound missing — scanned max={mx}")
        if mx is None:
            # Still unknown: appending here would silently duplicate keys and
            # poison every later merge for this table. Refuse loudly instead.
            raise SystemExit(
                f"FATAL: {namespace}.{table}.{keycol}: no manifest bound and no "
                f"scannable max, so a fresh key cannot be issued above the "
                f"existing ones. Appending would REUSE keys and break the "
                f"referential-integrity premise the delta merge depends on "
                f"(a re-used key is reachable from pre-append facts). Fix the "
                f"table's statistics, or exclude it from CDC_EXTRA_TABLES.")
        kb = mx + 1
        if verbose:
            print(f"   {table}.{keycol}: current max={mx} -> issuing {kb}..{kb+n-1}")
    cols = gen_table_cols(table, columns, n, date_lo=date_lo, date_hi=date_hi,
                          dim_hi=dim_hi_cache, key_base=kb,
                          str_domains=load_string_domains(t, columns, verbose=verbose))
    if verbose:
        flush_date_reports()
    data = pa.table({c: cols[c] for c, _ in columns}, schema=_pa_schema(columns))
    _write_and_add(fs, t, data, n, f"{namespace}.{table}", strict=strict)
    # the facts appended after this may now reference the new keys
    dim_hi_cache.pop(table, None)


def _slice_col(columns):
    """The column --upsert-store-sk slices on: the fact's own 'channel' key."""
    return next((c for c, _ in columns
                 if c.endswith("_store_sk") or c.endswith("_call_center_sk")
                 or c.endswith("_web_site_sk")), None)


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--catalog",required=True); ap.add_argument("--warehouse",required=True)
    ap.add_argument("--namespace",default="tpcds"); ap.add_argument("--table",default="store_returns")
    ap.add_argument("--rows",type=int,default=100000); ap.add_argument("--s3-region",default="us-east-1")
    ap.add_argument("--s3-endpoint",default="",help="custom S3 endpoint (MinIO etc.); blank = AWS (same convention as register_tpcds_tables.py)")
    ap.add_argument("--mode",choices=["append","upsert"],default="append",
        help="append = add_files only (incremental-mergeable). upsert = copy-on-write "
             "DELETE of a store/call-center/web-site slice + replacement rows: rewrites "
             "data files, so the snapshot has REMOVED files and the gateway must "
             "full-rematerialize.")
    ap.add_argument("--upsert-store-sk",type=int,default=7,help="slice replaced in upsert mode")
    ap.add_argument("--stream-days",type=int,default=int(os.environ.get("CDC_STREAM_DAYS","0") or 0),
        help="REALISTIC-STREAM mode: appended fact rows are dated in the last N days of the "
             "dataset's own 'now' (the sales facts' max sold date from catalog bounds), the "
             "date pool from --key-pools is ignored, returns still reference the appended sales "
             "and keys stay monotone. 0 (default) keeps the query-aware backfill: dates drawn "
             "from --years / the target query's own predicates, which lands rows in old "
             "periods for old random keys — the case that forces partner-fact reads on merge.")
    ap.add_argument("--years",default="2000,2001,2002",
        help="comma-separated d_year values the delta's date_sk must span. The delta is "
             "invisible to any MV whose body filters a year outside this set — q4 filters "
             "d_year IN (2001,2002) and got a 0-row merge from the old 2000-only default.")
    ap.add_argument("--key-pools",default="",help="JSON from query_pools.py: draw appended keys/dates from the target query's pools")
    ap.add_argument("--tick",action="store_true",
        help="append ONE realistic CDC tick across all six facts in proportion "
             "(see --ratios) instead of a flat --rows into a single --table. That "
             "flat shape made web_sales as busy as store_sales and returns as busy "
             "as sales, which is not a workload anyone runs.")
    ap.add_argument("--ratios",default="",
        help="sales-fact mix for --tick, e.g. 'store_sales=1,catalog_sales=0.5,"
             "web_sales=0.25' (the default 4:2:1 TPC-DS shape). rows = --rows * ratio.")
    ap.add_argument("--returns-ratio",type=float,default=DEFAULT_RETURNS_RATIO,
        help="fraction of each sales append that also gets a REFERENTIAL returns row, "
             "keyed on (item_sk, ticket/order number). Default 0.1 matches TPC-DS. "
             "Set 0 for sales only — but note every sales x returns query (q24, q64, "
             "...) then has a provably empty delta.")
    ap.add_argument("--extra-tables",default="",
        help="with --tick: space/comma-separated NON-sales tables to append as well "
             "(e.g. 'inventory customer item' — the internal 9-table wave). Every "
             "physical column is generated from the live schema; the table's own "
             "surrogate key continues from the catalog max. Appended BEFORE the "
             "facts so the tick's sales rows can reference the new keys.")
    ap.add_argument("--dim-rate",type=float,default=0.0001,
        help="with --tick: rows appended to each --extra-tables table as a FRACTION "
             "of its current row count (default 1e-4: store 1,002 -> 1 row, item "
             "300,000 -> 30, customer 12,000,000 -> 1,200), minimum 1. A flat "
             "--rows per dimension grew store to 1,428,002 rows in ~290 ticks on "
             "SF1000 (2026-09-19); dimensions change slowly, facts do not. "
             "--extra-rows overrides.")
    ap.add_argument("--extra-rows",type=int,default=0,
        help="rows per extra table (default: same as --rows, the flat count the "
             "internal wave used)")
    ap.add_argument("--dry-run",action="store_true",
        help="build + schema-conform every delta, write NOTHING (validates the "
             "generator against the live catalog)")
    ap.add_argument("--no-strict",action="store_true",
        help="downgrade the null-fill guard from fatal to a warning (debug only)")
    ap.add_argument("--no-geo",action="store_true",
        help="draw ss_customer_sk and ss_store_sk INDEPENDENTLY instead of as a "
             "zip-consistent (customer, address, store) triple. Independence is "
             "what leaves q24 (s_zip = ca_zip AND s_market_id = 8) with a 0-row "
             "delta: measured on SF1000, a random pair passes with p=8.46e-05, so "
             "a 5,000-row store_returns delta expects 0.42 eligible rows.")
    a=ap.parse_args()
    if a.key_pools:
        load_key_pools(a.key_pools)
    global DRY_RUN; DRY_RUN=a.dry_run
    try:
        years=[int(y) for y in a.years.split(",") if y.strip()]
        date_lo,date_hi=date_sk_bounds(years)
    except KeyError as e:
        raise SystemExit(f"--years: no date_sk bounds known for {e}; known: {sorted(YEAR_DATE_SK)}")
    if not a.s3_endpoint:
        # env fallback so callers that export AWS_ENDPOINT_URL (bench launchers)
        # get MinIO routing without a CLI change
        a.s3_endpoint=os.environ.get("S3_ENDPOINT") or os.environ.get("AWS_ENDPOINT_URL") or ""
    from pyiceberg.catalog.rest import RestCatalog
    import s3fs
    props={"s3.region":a.s3_region}
    if a.s3_endpoint:
        # Custom endpoint: point BOTH pyiceberg's file IO and the writer at it
        # (without this, PyArrow S3 IO resolves bucket names against real AWS ->
        # ACCESS_DENIED on a local-only bucket; observed q64 bench 2026-07-29).
        # Same convention as register_tpcds_tables.py: s3.endpoint always; path-
        # style ONLY for non-AWS endpoints (MinIO needs it; forcing it on real
        # AWS would regress newer buckets — S3_ENDPOINT is commonly the AWS
        # regional URL on blimp nodes, per standup_data.sh).
        props["s3.endpoint"]=a.s3_endpoint
        if "amazonaws.com" not in a.s3_endpoint:
            props["s3.path-style-access"]="true"
    cat=RestCatalog("kit",uri=a.catalog,warehouse=a.warehouse,**props)
    try: cat.create_namespace((a.namespace,))
    except Exception: pass

    fs_kwargs={"region_name":a.s3_region}
    if a.s3_endpoint: fs_kwargs["endpoint_url"]=a.s3_endpoint
    fs=s3fs.S3FileSystem(client_kwargs=fs_kwargs)
    dim_hi_cache={}
    strict=not a.no_strict
    if a.stream_days and a.stream_days>0:
        now=None
        for f_,dcol in FACT_DATE_COL.items():
            _,hi=catalog_bounds(cat,a.namespace,f_,dcol,min_rows=100000)
            if hi is not None: now=hi if now is None else max(now,hi)
        if now is None:
            raise SystemExit("--stream-days: no sold-date bounds in the catalog for the sales facts")
        date_lo,date_hi=now-a.stream_days+1,now
        clear_key_pools()
        print(f"== STREAM mode: dataset now = d_date_sk {now}; appended rows dated {date_lo}..{date_hi} (last {a.stream_days} days); ALL key pools ignored ==")
    if KEY_POOLS["queries"] or KEY_POOLS["date_sk"] or KEY_POOLS["date_by_col"]:
        print(f"== delta date span: per-query windows from --key-pools; "
              f"{date_lo}..{date_hi} (years {years}) is the fallback for any column "
              f"no query constrains ==")
    else:
        print(f"== delta date span: d_date_sk {date_lo}..{date_hi} (years {years}) — "
              f"UNIFORM, no --key-pools: a query whose MV bakes a date filter outside "
              f"this span merges 0 rows ==")

    # ---------------- one realistic tick across all six facts ---------------
    if a.tick:
        if a.mode!="append":
            raise SystemExit("--tick is append-only (upsert rewrites files; use --table)")
        plan=plan_tick(a.rows,parse_ratios(a.ratios),a.returns_ratio)
        extras=[x for x in a.extra_tables.replace(","," ").split() if x]
        def extra_rows_for(x):
            if a.extra_rows: return a.extra_rows
            try:
                cs=cat.load_table((a.namespace,x)).current_snapshot()
                n=int((cs.summary or {}).get("total-records",0)) if cs else 0
            except Exception:
                n=0
            return max(1,int(round(n*a.dim_rate))) if n>0 else 1
        xrows={x:extra_rows_for(x) for x in extras}
        print("== CDC tick plan (base=%d rows, returns_ratio=%.3f, dim_rate=%g) =="%(a.rows,a.returns_ratio,a.dim_rate))
        for x in extras: print(f"   {x}: +{xrows[x]}")
        for sf,sn,rf,rn in plan: print(f"   {sf}: +{sn}    {rf}: +{rn}")
        # ALL-OR-NOTHING TICK. Iceberg commits per table (add_files -> one snapshot
        # each), so a tick that appends 24 tables and then FAILS on table N leaves
        # the first N-1 appended and the rest not — a partial, referentially-broken
        # dataset (dims advanced, their facts didn't, or vice versa) that no
        # rollback ever undid. Snapshot every table this tick will touch BEFORE
        # writing; on any failure, roll each advanced table back to that snapshot so
        # the dataset ends EXACTLY as it started, then re-raise so the caller sees
        # the non-zero exit (bench_cdc.sh prints "CDC TICK FAILED").
        touched=list(dict.fromkeys(
            extras + [t for sf,sn,rf,rn in plan for t in (sf,rf) if t]))
        pre_snap={}
        for tbl in touched:
            try:
                cs=cat.load_table((a.namespace,tbl)).current_snapshot()
                pre_snap[tbl]=cs.snapshot_id if cs else None
            except Exception:
                pre_snap[tbl]=None   # table doesn't exist yet — created this tick
        import time as _time
        _tt0=_time.time(); _tt=[]
        try:
            for x in extras:
                _t=_time.time()
                append_table(cat,fs,a.namespace,x,xrows[x],date_lo=date_lo,date_hi=date_hi,
                             dim_hi_cache=dim_hi_cache,strict=strict)
                _tt.append((x,_time.time()-_t))
            _t=_time.time()
            geo=None if a.no_geo else load_geo_pairs(cat,a.namespace)
            _tt.append(("geo_pairs",_time.time()-_t))
            for sf,sn,rf,rn in plan:
                _t=_time.time()
                append_fact(cat,fs,a.namespace,sf,sn,date_lo=date_lo,date_hi=date_hi,
                            dim_hi_cache=dim_hi_cache,returns_rows=rn,strict=strict,geo=geo)
                _tt.append((sf,_time.time()-_t))
            # WHERE THE TICK'S TIME GOES — measured per table, so the slowest
            # step is named instead of guessed (the tick was ~80-95 s, 2026-09-24).
            print("== tick timing: total %.1fs | %s"%(_time.time()-_tt0,
                  ", ".join("%s %.1fs"%(k,v) for k,v in sorted(_tt,key=lambda kv:-kv[1]))))
            print("== tick steps: %s"%", ".join("%s %.1fs"%(k,v) for k,v in sorted(STEP_T.items(),key=lambda kv:-kv[1])))
        except Exception as tick_err:
            import sys as _sys
            print(f"!! CDC tick FAILED ({tick_err}) — rolling back partial appends "
                  f"so the dataset is left unchanged",file=_sys.stderr)
            for tbl,snap in pre_snap.items():
                if snap is None:
                    continue   # nothing to roll back to (table was new this tick)
                try:
                    t=cat.load_table((a.namespace,tbl)); t.refresh()
                    cur=t.current_snapshot()
                    if cur is not None and cur.snapshot_id!=snap:
                        t.manage_snapshots().rollback_to_snapshot(snap).commit()
                        print(f"   rolled back {tbl} -> snapshot {snap}",file=_sys.stderr)
                except Exception as rb_err:
                    print(f"   WARN: could not roll back {tbl}: {rb_err}",file=_sys.stderr)
            raise
        return

    # ---------------- single-table mode (legacy / upsert) -------------------
    n=a.rows
    if a.table not in FACT_COLUMNS:
        raise SystemExit(f"--table {a.table}: unknown fact; known: {sorted(FACT_COLUMNS)}")
    t=_load_or_create(cat,a.namespace,a.table)
    columns=_columns_of(t,a.table)
    store_pool=None
    if a.mode=="upsert":
        sc=_slice_col(columns)
        if not sc: raise SystemExit(f"--mode upsert: {a.table} has no sliceable channel key")
        store_pool=[a.upsert_store_sk]*n
        # Copy-on-write delete of the slice being replaced: pyiceberg rewrites
        # every data file containing matching rows, so the resulting snapshot
        # REMOVES files — exactly what real upserts/overwrites/compactions do.
        # The gateway must detect that (added_files.removed_files) and take the
        # full re-materialize path; an added-only merge would double-count.
        from pyiceberg.expressions import EqualTo
        t.delete(EqualTo(sc,a.upsert_store_sk)); t.refresh()
        print(f"upsert: deleted {sc}={a.upsert_store_sk} slice -> snapshot {t.current_snapshot().snapshot_id}")

    rn=int(round(n*a.returns_ratio)) if (a.mode=="append" and a.table in RETURNS_OF) else 0
    geo=None if (a.no_geo or a.table!="store_sales") else load_geo_pairs(cat,a.namespace)
    append_fact(cat,fs,a.namespace,a.table,n,date_lo=date_lo,date_hi=date_hi,
                dim_hi_cache=dim_hi_cache,store_pool=store_pool,returns_rows=rn,
                strict=strict,geo=geo)

if __name__=="__main__": main()
