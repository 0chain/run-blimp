#!/usr/bin/env python3
"""gen_incr_delta.py — pseudo-random store_returns delta for the incremental-MV experiment.

WHY: the incremental leg appends a chunk to the source, then compares the Blimp
delta-merge result against a Spark full-recompute. If the appended rows are a
UNIFORM fill (all same customer/store/amount) the aggregate barely moves and the
query's top-N is unchanged — so "Blimp == Spark" is trivially true and proves
nothing. This generates a delta that is:

  * DETERMINISTIC  — a fixed --seed drives a single random.Random(seed); the same
                     seed produces byte-identical rows, so a surprising result is
                     always reproducible and diffable. (No wall-clock, no os.urandom.)
  * RANDOMIZED     — customer, store, date and amount are all drawn from
                     distributions, not constants.
  * RESULT-MOVING  — amounts are lognormal (a realistic long tail), and a tunable
                     fraction land on the query's target stores (e.g. TN for q1),
                     so per-customer totals actually cross selection thresholds and
                     the top-N shifts. That makes the Spark-vs-incremental compare
                     a real test.
  * DEBUGGABLE     — writes a sidecar <chunk>.manifest.json with the seed, the exact
                     knobs, and summary stats (distinct customers, store histogram,
                     amount min/mean/p99, and how many rows landed on target stores)
                     so you can see what the delta *should* do before you merge it.

Rows carry the full TPC-DS store_returns schema so `add_files` matches the table;
only the q1 drivers are randomized, the rest are deterministic derivations of the
row so they're stable and never null-surprise a scan.

Usage:
  python3 gen_incr_delta.py --seed 42 --rows 20000 \
      --out s3://blimp-tpcds1000-aps1/store_returns/chunkINCR_s42/data.parquet \
      --customers 2000000 --target-store-sks 15,42,77 --target-frac 0.35
  # then add_files that parquet into the customer catalog's store_returns and
  # POST /admin/source/snapshot_changed.

Needs: pip install "pyarrow" "s3fs" (s3fs only if --out is an s3:// path).
"""
import argparse, json, math, random, statistics, sys

# TPC-DS store_returns columns, in table order.
SCHEMA = [
    "sr_returned_date_sk", "sr_return_time_sk", "sr_item_sk", "sr_customer_sk",
    "sr_cdemo_sk", "sr_hdemo_sk", "sr_addr_sk", "sr_store_sk", "sr_reason_sk",
    "sr_ticket_number", "sr_return_quantity", "sr_return_amt", "sr_return_tax",
    "sr_return_amt_inc_tax", "sr_fee", "sr_return_ship_cost", "sr_refunded_cash",
    "sr_reversed_charge", "sr_store_credit", "sr_net_loss",
]

def build(args):
    rng = random.Random(args.seed)  # the ONLY entropy source — fully reproducible.

    # d_year=2000 → d_date_sk range (TPC-DS julian convention: 2000-01-01..12-31).
    date_lo, date_hi = args.date_sk_min, args.date_sk_max
    target_stores = [int(x) for x in args.target_store_sks.split(",") if x.strip()]
    # A larger pool of "other" stores so the delta is realistically spread, but a
    # tunable --target-frac deliberately lands on the query's target stores so it
    # bites the specific query under test.
    other_stores = [s for s in range(1, args.n_stores + 1) if s not in target_stores]

    cols = {c: [] for c in SCHEMA}
    hit_target = 0
    for i in range(args.rows):
        cust = rng.randint(1, args.customers)
        if rng.random() < args.target_frac and target_stores:
            store = rng.choice(target_stores); hit_target += 1
        else:
            store = rng.choice(other_stores)
        date_sk = rng.randint(date_lo, date_hi)
        # lognormal amount: median ~e^mu, heavy upper tail → some returns are large
        # enough to push a customer's SUM over a 1.2*avg threshold. Clamp to sane $.
        amt = round(min(max(rng.lognormvariate(args.amt_mu, args.amt_sigma), 1.0), 100000.0), 2)
        qty = rng.randint(1, 12)
        tax = round(amt * 0.07, 2)

        cols["sr_returned_date_sk"].append(date_sk)
        cols["sr_return_time_sk"].append(rng.randint(28800, 61200))   # 8h..17h in secs
        cols["sr_item_sk"].append(rng.randint(1, args.n_items))
        cols["sr_customer_sk"].append(cust)
        cols["sr_cdemo_sk"].append(rng.randint(1, 1920800))
        cols["sr_hdemo_sk"].append(rng.randint(1, 7200))
        cols["sr_addr_sk"].append(rng.randint(1, max(1, args.customers // 2)))
        cols["sr_store_sk"].append(store)
        cols["sr_reason_sk"].append(rng.randint(1, 55))
        # ticket_number: deterministic-per-row (seed+index) so it's unique & stable.
        cols["sr_ticket_number"].append(args.seed * 10_000_000 + i)
        cols["sr_return_quantity"].append(qty)
        cols["sr_return_amt"].append(amt)
        cols["sr_return_tax"].append(tax)
        cols["sr_return_amt_inc_tax"].append(round(amt + tax, 2))
        cols["sr_fee"].append(round(rng.uniform(0, 50), 2))
        cols["sr_return_ship_cost"].append(round(rng.uniform(0, amt * 0.1), 2))
        refunded = round(amt * rng.uniform(0.5, 1.0), 2)
        cols["sr_refunded_cash"].append(refunded)
        cols["sr_reversed_charge"].append(round((amt - refunded) * 0.5, 2))
        cols["sr_store_credit"].append(round((amt - refunded) * 0.5, 2))
        cols["sr_net_loss"].append(round(amt * rng.uniform(0.05, 0.3), 2))

    amts = cols["sr_return_amt"]
    amts_sorted = sorted(amts)
    manifest = {
        "seed": args.seed, "rows": args.rows,
        "knobs": {
            "customers": args.customers, "target_store_sks": target_stores,
            "target_frac": args.target_frac, "n_stores": args.n_stores,
            "date_sk_min": date_lo, "date_sk_max": date_hi,
            "amt_mu": args.amt_mu, "amt_sigma": args.amt_sigma,
        },
        "summary": {
            "distinct_customers": len(set(cols["sr_customer_sk"])),
            "rows_on_target_stores": hit_target,
            "amt_min": amts_sorted[0], "amt_mean": round(statistics.fmean(amts), 2),
            "amt_p99": amts_sorted[int(0.99 * (len(amts) - 1))], "amt_max": amts_sorted[-1],
        },
    }
    return cols, manifest

def main():
    p = argparse.ArgumentParser(description="pseudo-random store_returns delta (seeded)")
    p.add_argument("--seed", type=int, default=42, help="RNG seed — same seed = identical data")
    p.add_argument("--rows", type=int, default=20000)
    p.add_argument("--out", required=True, help="output parquet path (local or s3://...)")
    p.add_argument("--customers", type=int, default=2_000_000, help="SF1000 customer count")
    p.add_argument("--n-stores", type=int, default=1002)
    p.add_argument("--n-items", type=int, default=300_000)
    p.add_argument("--target-store-sks", default="15",
                   help="comma list of store_sks the query targets (e.g. TN stores for q1)")
    p.add_argument("--target-frac", type=float, default=0.35,
                   help="fraction of rows forced onto target stores (0..1)")
    p.add_argument("--date-sk-min", type=int, default=2451545, help="d_year=2000 lower d_date_sk")
    p.add_argument("--date-sk-max", type=int, default=2451910, help="d_year=2000 upper d_date_sk")
    p.add_argument("--amt-mu", type=float, default=5.0, help="lognormal mu (median amt ~= e^mu)")
    p.add_argument("--amt-sigma", type=float, default=1.1, help="lognormal sigma (tail heaviness)")
    args = p.parse_args()

    try:
        import pyarrow as pa, pyarrow.parquet as pq
    except ImportError:
        sys.exit("need: pip install pyarrow  (and s3fs for s3:// output)")

    cols, manifest = build(args)
    # int64 for *_sk / quantity / ticket; float64 for amounts — matches TPC-DS.
    int_cols = {"sr_returned_date_sk","sr_return_time_sk","sr_item_sk","sr_customer_sk",
                "sr_cdemo_sk","sr_hdemo_sk","sr_addr_sk","sr_store_sk","sr_reason_sk",
                "sr_ticket_number","sr_return_quantity"}
    arrays = [pa.array(cols[c], type=pa.int64() if c in int_cols else pa.float64()) for c in SCHEMA]
    table = pa.table(arrays, names=SCHEMA)

    fs_kw = {}
    if args.out.startswith("s3://"):
        import s3fs  # noqa: F401  (pyarrow uses it via the s3 filesystem)
    pq.write_table(table, args.out)
    man_path = args.out + ".manifest.json"
    if args.out.startswith("s3://"):
        import s3fs
        with s3fs.S3FileSystem().open(man_path, "w") as f:
            json.dump(manifest, f, indent=2)
    else:
        with open(man_path, "w") as f:
            json.dump(manifest, f, indent=2)

    print(json.dumps(manifest, indent=2))
    print(f"\nwrote {args.rows} rows -> {args.out}\nmanifest -> {man_path}", file=sys.stderr)

if __name__ == "__main__":
    main()
