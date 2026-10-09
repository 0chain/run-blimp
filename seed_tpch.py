#!/usr/bin/env python3
"""One CDC tick for a TPC-H namespace: append new ORDERS and their LINEITEMS.

The TPC-DS seeder (seed_tpcds.py) keys everything off TPC-DS conventions
(_sk surrogate keys, date_dim). TPC-H has its own rules, and a delta that breaks
them is a wrong delta, not a smaller one:
  - o_orderkey continues above the table's current max (never reused);
  - every new order gets 1..7 lineitems, l_linenumber 1..k;
  - (l_partkey, l_suppkey) is a pair that EXISTS in partsupp — dbgen's own
    formula, so q2/q9/q11/q16/q20's partsupp joins see the new lines;
  - l_extendedprice = l_quantity x p_retailprice(partkey) (dbgen's formula);
  - flags, statuses and dates follow dbgen (returnflag/linestatus by the
    1995-06-17 cut, ship/commit/receipt offsets from the order date);
  - o_totalprice and o_orderstatus are derived from the order's own lines.
Dimension tables (customer, part, supplier, partsupp, nation, region) are not
appended: every key the new rows reference already exists.

Catalog/write plumbing is seed_tpcds.py's (catalog_bounds, scan_dim_hi,
_write_and_add): same bounds cache, same null guard, same schema conform.

bench_cdc.sh calls this instead of seed_tpcds.py when the namespace holds a
lineitem table; TPC-DS-only options it passes are accepted and ignored.
"""
import argparse, datetime, decimal, os, random, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import seed_tpcds as sd  # noqa: E402

CUT = datetime.date(1995, 6, 17)         # dbgen CURRENTDATE
START = datetime.date(1992, 1, 1)
END_ORDER = datetime.date(1998, 8, 2)    # last orderdate dbgen issues
PRIORITIES = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]
INSTRUCT = ["DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"]
MODES = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"]
D2 = decimal.Decimal("0.01")


def retail_price(pk):
    return (decimal.Decimal(90000 + ((pk // 10) % 20001) + 100 * (pk % 1000)) / 100).quantize(D2)


def supp_for(pk, i, S):
    """dbgen PART_SUPP_BRIDGE: the i-th (0..3) supplier of part pk."""
    return (pk + i * (S // 4 + (pk - 1) // S)) % S + 1


def key_max(cat, ns, table, col):
    _, mx = sd.catalog_bounds(cat, ns, table, col)
    if mx is None:
        mx = sd.scan_dim_hi(cat, ns, table, col)
    if mx is None:
        raise SystemExit(f"FATAL: {ns}.{table}.{col}: no manifest bound and no scannable max — "
                         "a fresh key cannot be issued above the existing ones")
    return int(mx)


def build(n_orders, okey0, C, P, S, clerks, rnd):
    o = {k: [] for k in ("o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate",
                         "o_orderpriority", "o_clerk", "o_shippriority", "o_comment")}
    l = {k: [] for k in ("l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity",
                         "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus",
                         "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct",
                         "l_shipmode", "l_comment")}
    span = (END_ORDER - START).days
    for j in range(n_orders):
        ok = okey0 + j
        od = START + datetime.timedelta(days=rnd.randint(0, span))
        ck = rnd.randint(1, C)
        while ck % 3 == 0 and C > 2:      # dbgen: a third of customers never order
            ck = rnd.randint(1, C)
        total = decimal.Decimal(0)
        stats = set()
        for ln in range(1, rnd.randint(1, 7) + 1):
            pk = rnd.randint(1, P)
            q = rnd.randint(1, 50)
            ext = (retail_price(pk) * q).quantize(D2)
            disc = (decimal.Decimal(rnd.randint(0, 10)) / 100).quantize(D2)
            tax = (decimal.Decimal(rnd.randint(0, 8)) / 100).quantize(D2)
            sd_ = od + datetime.timedelta(days=rnd.randint(1, 121))
            cd = od + datetime.timedelta(days=rnd.randint(30, 90))
            rd = sd_ + datetime.timedelta(days=rnd.randint(1, 30))
            rf = rnd.choice("RA") if rd <= CUT else "N"
            ls = "O" if sd_ > CUT else "F"
            stats.add(ls)
            total += (ext * (1 + tax) * (1 - disc)).quantize(D2)
            for k, v in (("l_orderkey", ok), ("l_partkey", pk), ("l_suppkey", supp_for(pk, rnd.randint(0, 3), S)),
                         ("l_linenumber", ln), ("l_quantity", decimal.Decimal(q).quantize(D2)),
                         ("l_extendedprice", ext), ("l_discount", disc), ("l_tax", tax),
                         ("l_returnflag", rf), ("l_linestatus", ls), ("l_shipdate", sd_),
                         ("l_commitdate", cd), ("l_receiptdate", rd),
                         ("l_shipinstruct", rnd.choice(INSTRUCT)), ("l_shipmode", rnd.choice(MODES)),
                         ("l_comment", f"cdc line {ok}-{ln}")):
                l[k].append(v)
        status = "F" if stats == {"F"} else "O" if stats == {"O"} else "P"
        for k, v in (("o_orderkey", ok), ("o_custkey", ck), ("o_orderstatus", status),
                     ("o_totalprice", total.quantize(D2)), ("o_orderdate", od),
                     ("o_orderpriority", rnd.choice(PRIORITIES)),
                     ("o_clerk", "Clerk#%09d" % rnd.randint(1, clerks)), ("o_shippriority", 0),
                     ("o_comment", f"cdc order {ok}")):
            o[k].append(v)
    return o, l


def to_table(cols, t):
    import pyarrow as pa
    from pyiceberg.io.pyarrow import schema_to_pyarrow
    target = {f.name: f.type for f in schema_to_pyarrow(t.schema())}
    arrs = {}
    for name, vals in cols.items():
        ty = target.get(name)
        if ty is not None and pa.types.is_decimal(ty):
            arrs[name] = pa.array(vals, type=pa.decimal128(18, 2))
        elif ty is not None and pa.types.is_date(ty):
            arrs[name] = pa.array(vals, type=pa.date32())
        else:
            arrs[name] = pa.array(vals)
    return pa.table(arrs)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True); ap.add_argument("--warehouse", required=True)
    ap.add_argument("--namespace", required=True)
    ap.add_argument("--tick", action="store_true")
    ap.add_argument("--rows", type=int, default=5000, help="lineitem rows per tick (orders ~ rows/4)")
    ap.add_argument("--s3-region", default="us-east-1"); ap.add_argument("--s3-endpoint", default="")
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true")
    a, ignored = ap.parse_known_args()
    if ignored:
        print(f"   (TPC-DS-only options ignored: {' '.join(x for x in ignored if x.startswith('--'))})")
    sd.DRY_RUN = a.dry_run
    if not a.s3_endpoint:
        a.s3_endpoint = os.environ.get("S3_ENDPOINT") or os.environ.get("AWS_ENDPOINT_URL") or ""
    from pyiceberg.catalog.rest import RestCatalog
    import s3fs
    props = {"s3.region": a.s3_region}
    if a.s3_endpoint:
        props["s3.endpoint"] = a.s3_endpoint
        if "amazonaws.com" not in a.s3_endpoint:
            props["s3.path-style-access"] = "true"
    cat = RestCatalog("kit", uri=a.catalog, warehouse=a.warehouse, **props)
    fs_kwargs = {"region_name": a.s3_region}
    if a.s3_endpoint:
        fs_kwargs["endpoint_url"] = a.s3_endpoint
    fs = s3fs.S3FileSystem(client_kwargs=fs_kwargs)
    ns = a.namespace
    okey0 = key_max(cat, ns, "orders", "o_orderkey") + 1
    C = key_max(cat, ns, "customer", "c_custkey")
    P = key_max(cat, ns, "part", "p_partkey")
    S = key_max(cat, ns, "supplier", "s_suppkey")
    clerks = max(1000, S // 10)           # dbgen: SF x 1000 clerks, S = SF x 10000
    n_orders = max(1, a.rows // 4)
    rnd = random.Random(a.seed)
    print(f"== TPC-H CDC tick: +{n_orders} orders (keys {okey0}..{okey0 + n_orders - 1}) over "
          f"customer 1..{C}, part 1..{P}, supplier 1..{S} ==")
    o, l = build(n_orders, okey0, C, P, S, clerks, rnd)
    import time
    t0 = time.time()
    for table, cols in (("orders", o), ("lineitem", l)):
        t = cat.load_table((ns, table))
        data = to_table(cols, t)
        sd._write_and_add(fs, t, data, data.num_rows, f"{ns}.{table}")
    print(f"   == tick timing: total {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
