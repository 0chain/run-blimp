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
import argparse, random, decimal, os
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

# Fallback dimension key ranges, used ONLY when the catalog cannot be read.
# These are SF1000 values (measured on test2 2026-08-04) and are WRONG at other
# scale factors — which is exactly why the catalog is the primary source.
FALLBACK_DIM_HI = {
    "date_dim": 2488070, "time_dim": 86399, "item": 300000, "customer": 12000000,
    "customer_demographics": 1920800, "household_demographics": 7200,
    "customer_address": 6000000, "store": 1002, "promotion": 1500,
    "call_center": 42, "catalog_page": 30000, "ship_mode": 20, "warehouse": 20,
    "web_page": 3000, "web_site": 54, "reason": 65,
}

def dim_for(col):
    """(dimension_table, key_column) a fact column references, or None."""
    if not col.endswith("_sk"):
        return None
    for suffix, dim in DIM_BY_SUFFIX:
        if col.endswith(suffix):
            return dim
    return None

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
    # One primary date per row drives every other date on that row.
    sold = [rnd.randint(date_lo, date_hi) for _ in range(n)]
    for name, kind in columns:
        d = dim_for(name)
        if d is None:
            continue
        dimtbl = d[0]
        if dimtbl == "date_dim":
            # ship dates trail the sale; sold/returned dates ARE the primary date
            out[name] = ([s + rnd.randint(2, 90) for s in sold]
                         if "ship" in name else list(sold))
        elif dimtbl == "time_dim":
            out[name] = [rnd.randint(0, hi("time_dim")) for _ in range(n)]
        else:
            h = hi(dimtbl)
            out[name] = [rnd.randint(1, h) for _ in range(n)]

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
    # overwrite everything that must agree with the parent sale.
    out = gen_fact_cols(rfact, returns_columns, m, date_lo=date_lo, date_hi=date_hi,
                        dim_hi=dim_hi, key_base=1, rnd=rnd)

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
    if store_pool:
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
def catalog_bounds(cat, namespace, table, col):
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
        if hi is None:
            hi = FALLBACK_DIM_HI.get(d)
            if verbose:
                print(f"   WARN: {d}.{keycol} bounds unreadable; falling back to "
                      f"SF1000 value {hi} — keys may not join at this scale factor")
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
                arr = arr.cast(field.type)
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
    m = {"i": pa.int32(), "l": pa.int64(), "d": pa.decimal128(7, 2)}
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
        out.append((f.name, "l" if s == "long" else ("d" if s.startswith("decimal") else "i")))
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
    with fs.open(key.replace("s3://", "", 1), "wb") as f:
        pq.write_table(data, f, store_decimal_as_integer=True,
                       write_statistics=stat_cols)
    t.add_files(file_paths=[key]); t.refresh()
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
    if apply_geo_correlation(cols, fact, geo) and verbose:
        print(f"   {fact}: customer/address/store drawn as zip-consistent triples")
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
    if keycol and verbose:
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
    rw = len(next(iter(rcols.values())))
    rdata = pa.table({c: rcols[c] for c, _ in rcolumns}, schema=_pa_schema(rcolumns))
    _write_and_add(fs, rt, rdata, rw,
                   f"{namespace}.{rfact} (referential to {fact})", strict=strict)
    return rw


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
    ap.add_argument("--years",default="2000,2001,2002",
        help="comma-separated d_year values the delta's date_sk must span. The delta is "
             "invisible to any MV whose body filters a year outside this set — q4 filters "
             "d_year IN (2001,2002) and got a 0-row merge from the old 2000-only default.")
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
    ap.add_argument("--no-strict",action="store_true",
        help="downgrade the null-fill guard from fatal to a warning (debug only)")
    ap.add_argument("--no-geo",action="store_true",
        help="draw ss_customer_sk and ss_store_sk INDEPENDENTLY instead of as a "
             "zip-consistent (customer, address, store) triple. Independence is "
             "what leaves q24 (s_zip = ca_zip AND s_market_id = 8) with a 0-row "
             "delta: measured on SF1000, a random pair passes with p=8.46e-05, so "
             "a 5,000-row store_returns delta expects 0.42 eligible rows.")
    a=ap.parse_args()
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
    print(f"== delta date span: d_date_sk {date_lo}..{date_hi} (years {years}) ==")

    # ---------------- one realistic tick across all six facts ---------------
    if a.tick:
        if a.mode!="append":
            raise SystemExit("--tick is append-only (upsert rewrites files; use --table)")
        plan=plan_tick(a.rows,parse_ratios(a.ratios),a.returns_ratio)
        print("== CDC tick plan (base=%d rows, returns_ratio=%.3f) =="%(a.rows,a.returns_ratio))
        for sf,sn,rf,rn in plan: print(f"   {sf}: +{sn}    {rf}: +{rn}")
        geo=None if a.no_geo else load_geo_pairs(cat,a.namespace)
        for sf,sn,rf,rn in plan:
            append_fact(cat,fs,a.namespace,sf,sn,date_lo=date_lo,date_hi=date_hi,
                        dim_hi_cache=dim_hi_cache,returns_rows=rn,strict=strict,geo=geo)
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
