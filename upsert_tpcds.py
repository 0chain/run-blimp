#!/usr/bin/env python3
"""Upsert (delete + re-insert the SAME keys) into the kit's store_returns.

WHY: the CDC append leg only ADDS data files — the incremental engine's easy
case. A real pipeline also UPDATES rows (MERGE/upsert), which on Iceberg is a
delete + insert: the snapshot removes/rewrites existing data files. This
exercises that path so the append-vs-upsert incremental refresh cost can be
compared (test_query.sh stage 3d/3e).

What it does, deterministically (seeded):
  1. DELETE  rows where sr_customer_sk is in [--key-min, --key-max]
             (pyiceberg row-level delete -> copy-on-write file rewrite).
  2. INSERT  one revised row per key in the same band, with new amounts
             (random.Random(seed)) — same keys, different values = an upsert.
  3. Print per-step wall times and the snapshot ids, then the caller POSTs
     /admin/source/snapshot_changed once.

Requires: pip install "pyiceberg[s3fs]" pyarrow (same venv as seed_tpcds.py).
"""
import argparse, decimal, random, time

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True)
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--namespace", default="tpcds")
    ap.add_argument("--table", default="store_returns")
    ap.add_argument("--key-min", type=int, default=1)
    ap.add_argument("--key-max", type=int, default=2000,
                    help="upsert band: every sr_customer_sk in [min,max] is rewritten")
    ap.add_argument("--seed", type=int, default=7, help="drives the revised amounts")
    ap.add_argument("--s3-region", default="us-east-1")
    a = ap.parse_args()

    from pyiceberg.catalog.rest import RestCatalog
    import pyarrow as pa

    cat = RestCatalog("kit", uri=a.catalog, warehouse=a.warehouse, **{"s3.region": a.s3_region})
    t = cat.load_table((a.namespace, a.table))
    snap_before = t.current_snapshot().snapshot_id

    # 1. row-level delete of the key band (copy-on-write: rewrites data files)
    t0 = time.time()
    t.delete(delete_filter=f"sr_customer_sk >= {a.key_min} and sr_customer_sk <= {a.key_max}")
    t.refresh()
    del_s = time.time() - t0
    snap_del = t.current_snapshot().snapshot_id

    # 2. re-insert the SAME keys with revised values (same physical parquet
    #    encoding as seed_tpcds.py: int-backed decimals + add_files)
    rng = random.Random(a.seed)
    keys = list(range(a.key_min, a.key_max + 1))
    schema = pa.schema([("sr_customer_sk", pa.int32()), ("sr_store_sk", pa.int32()),
                        ("sr_returned_date_sk", pa.int32()), ("sr_return_amt", pa.decimal128(7, 2))])
    data = pa.table({
        "sr_customer_sk": keys,
        "sr_store_sk": [rng.randint(1, 50) for _ in keys],
        "sr_returned_date_sk": [2451545 for _ in keys],
        "sr_return_amt": [decimal.Decimal(str(round(rng.uniform(1, 5000), 2))) for _ in keys],
    }, schema=schema)

    import uuid, s3fs, pyarrow.parquet as pq
    loc = t.location().rstrip("/")
    key = f"{loc}/data/upsert-{uuid.uuid4().hex}.parquet"
    fs = s3fs.S3FileSystem(client_kwargs={"region_name": a.s3_region})
    t0 = time.time()
    with fs.open(key.replace("s3://", "", 1), "wb") as f:
        pq.write_table(data, f, store_decimal_as_integer=True)
    t.add_files(file_paths=[key]); t.refresh()
    ins_s = time.time() - t0
    snap_ins = t.current_snapshot().snapshot_id

    print(f"{a.namespace}.{a.table}: upsert keys [{a.key_min},{a.key_max}] "
          f"delete {del_s:.1f}s (snap {snap_before}->{snap_del}) + "
          f"insert {len(keys)} rows {ins_s:.1f}s (snap ->{snap_ins})")

if __name__ == "__main__":
    main()
