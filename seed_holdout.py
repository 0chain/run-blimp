#!/usr/bin/env python3
"""One CDC tick for a ClickBench namespace: append hits rows held out of the base.

The base table `hits` is registered from the first files of the ClickBench
dataset (tools/clickbench_load.py); the rest are kept under hits_holdout/ in the
same bucket, already converted to the table's types. A tick appends --rows rows
taken from a random held-out file at a random offset: real ClickBench rows, so
every query's filters (CounterID = 62, the July 2013 dates, non-empty
SearchPhrase, …) see appended rows in the same proportions as the base.

Catalog/write plumbing is seed_tpcds.py's (load_rest_catalog, _write_and_add).
bench_cdc.sh calls this instead of seed_tpcds.py when the namespace holds a
hits table; TPC-DS-only options it passes are accepted and ignored.
"""
import argparse, os, random, sys, time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import seed_tpcds as sd  # noqa: E402


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True); ap.add_argument("--warehouse", required=True)
    ap.add_argument("--namespace", required=True)
    ap.add_argument("--tick", action="store_true")
    ap.add_argument("--rows", type=int, default=5000, help="hits rows appended per tick")
    ap.add_argument("--holdout", default="", help="s3://bucket/prefix of the held-out files (default: <table source bucket>/hits_holdout)")
    ap.add_argument("--s3-region", default="us-east-1"); ap.add_argument("--s3-endpoint", default="")
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true")
    a, ignored = ap.parse_known_args()
    if ignored:
        print(f"   (TPC-DS-only options ignored: {' '.join(x for x in ignored if x.startswith('--'))})")
    sd.DRY_RUN = a.dry_run
    if not a.s3_endpoint:
        a.s3_endpoint = os.environ.get("S3_ENDPOINT") or os.environ.get("AWS_ENDPOINT_URL") or ""
    import s3fs, pyarrow.parquet as pq
    cat = sd.load_rest_catalog("kit", a.catalog, a.warehouse, s3_endpoint=a.s3_endpoint, region=a.s3_region)
    fs_kwargs = {"region_name": a.s3_region}
    if a.s3_endpoint:
        fs_kwargs["endpoint_url"] = a.s3_endpoint
    fs = s3fs.S3FileSystem(client_kwargs=fs_kwargs)
    t = cat.load_table((a.namespace, "hits"))
    holdout = a.holdout or os.environ.get("CLICKBENCH_HOLDOUT", "")
    if not holdout:
        raise SystemExit("FATAL: no --holdout / CLICKBENCH_HOLDOUT: where the held-out hits files are")
    files = sorted(fs.glob(holdout.rstrip("/").replace("s3://", "") + "/*.parquet"))
    if not files:
        raise SystemExit(f"FATAL: no held-out parquet under {holdout}")
    rnd = random.Random(a.seed)
    f = rnd.choice(files)
    pf = pq.ParquetFile(fs.open(f, "rb"))
    n = pf.metadata.num_rows
    off = rnd.randrange(0, max(1, n - a.rows))
    # read only the row groups that hold [off, off+rows)
    rows, start, groups = [], 0, []
    for g in range(pf.metadata.num_row_groups):
        gn = pf.metadata.row_group(g).num_rows
        if start + gn > off and start < off + a.rows:
            groups.append(g)
        start += gn
    data = pf.read_row_groups(groups)
    first = sum(pf.metadata.row_group(g).num_rows for g in range(groups[0]))
    data = data.slice(off - first, a.rows)
    print(f"== ClickBench CDC tick: +{data.num_rows} hits rows from {f.rsplit('/', 1)[-1]} @ {off} ==")
    t0 = time.time()
    sd._write_and_add(fs, t, data, data.num_rows, f"{a.namespace}.hits", strict=False)
    print(f"   == tick timing: total {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
