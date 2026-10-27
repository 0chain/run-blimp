#!/usr/bin/env python3
"""One CDC tick for a namespace loaded with held-out files (ClickBench, Nexmark).

A loader (tools/clickbench_load.sh, tools/nexmark_load.sh) registers each
table from most of its files and keeps the rest under <root>/<table>_holdout/
in the same bucket, already in the table's types. A tick appends --rows rows in
all, split across the namespace's tables that have a holdout in proportion to
their held-out row counts (Nexmark's person:auction:bid stays ~1:3:46). Each
table's slice is CONTIGUOUS: it starts where the table's appends so far end
(its current rows minus its bench_baseline tag's rows, both from the catalog),
over the held-out files in name order, written in event order by the loader.
So every table advances through the same stretch of later events: appended
bids name the auctions and persons appended with them, and no row is appended
twice. A rollback to bench_baseline starts the stream over.

Catalog/write plumbing is seed_tpcds.py's (load_rest_catalog, _write_and_add).
bench_cdc.sh calls this for a namespace that holds neither store_sales nor
lineitem (HOLDOUT_ROOT, or the older CLICKBENCH_HOLDOUT, names the held-out
files); TPC-DS-only options it passes are accepted and ignored.
"""
import argparse, os, sys, time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import seed_tpcds as sd  # noqa: E402


def _rows_of(t, ref=None):
    snap = t.snapshot_by_name(ref) if ref else t.current_snapshot()
    if snap is None or snap.summary is None:
        return None
    v = snap.summary.get("total-records")
    return int(v) if v is not None else None


def _contiguous(fs, pq, files, start, rows):
    """rows rows of the held-out files (name order) from row start on."""
    import pyarrow as pa
    out, pos = [], 0
    for f in files:
        pf = pq.ParquetFile(fs.open(f, "rb"))
        n = pf.metadata.num_rows
        if pos + n <= start:
            pos += n
            continue
        lo = max(0, start - pos)
        take = min(n - lo, rows - sum(x.num_rows for x in out))
        gstart, groups = 0, []
        for g in range(pf.metadata.num_row_groups):
            gn = pf.metadata.row_group(g).num_rows
            if gstart + gn > lo and gstart < lo + take:
                groups.append(g)
            gstart += gn
        first = sum(pf.metadata.row_group(g).num_rows for g in range(groups[0]))
        out.append(pf.read_row_groups(groups).slice(lo - first, take))
        pos += n
        if sum(x.num_rows for x in out) >= rows:
            break
    return pa.concat_tables(out) if out else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True); ap.add_argument("--warehouse", required=True)
    ap.add_argument("--namespace", required=True)
    ap.add_argument("--tick", action="store_true")
    ap.add_argument("--rows", type=int, default=5000, help="rows appended per tick, across all held-out tables")
    ap.add_argument("--holdout", default="", help="s3://bucket[/prefix] holding <table>_holdout/ dirs (or a single <table>_holdout dir)")
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
    root = (a.holdout or os.environ.get("HOLDOUT_ROOT", "") or os.environ.get("CLICKBENCH_HOLDOUT", "")).rstrip("/")
    if not root:
        raise SystemExit("FATAL: no --holdout / HOLDOUT_ROOT: where the held-out files are")
    root = root.replace("s3://", "")
    if root.endswith("_holdout"):  # the older single-table form: s3://clickbench/hits_holdout
        root = root.rsplit("/", 1)[0]
    held = {}
    for d in fs.ls(root):
        name = d.rstrip("/").rsplit("/", 1)[-1]
        if not name.endswith("_holdout"):
            continue
        files = sorted(fs.glob(d.rstrip("/") + "/*.parquet"))
        if files:
            n = sum(pq.ParquetFile(fs.open(f, "rb")).metadata.num_rows for f in files[:3]) * len(files) / min(3, len(files))
            held[name[:-len("_holdout")]] = (files, n)
    if not held:
        raise SystemExit(f"FATAL: no <table>_holdout/ parquet under s3://{root}")
    # only the namespace's own tables: a bucket may hold another dataset's
    # holdouts beside this one's
    tables = {}
    for tbl in sorted(held):
        try:
            tables[tbl] = cat.load_table((a.namespace, tbl))
        except Exception:
            print(f"   {tbl}_holdout/: no {a.namespace}.{tbl} — not this namespace's table, skipped")
    if not tables:
        raise SystemExit(f"FATAL: none of the held-out tables ({', '.join(sorted(held))}) is in namespace {a.namespace}")
    total = sum(held[t][1] for t in tables)
    print(f"== holdout CDC tick: +{a.rows} rows across {', '.join(sorted(tables))} ==")
    t0 = time.time()
    appended = 0
    for tbl, t in sorted(tables.items()):
        files, n = held[tbl]
        rows = max(1, round(a.rows * n / total))
        now, base = _rows_of(t), _rows_of(t, "bench_baseline")
        if now is None or base is None:
            raise SystemExit(f"FATAL: {a.namespace}.{tbl}: no row count for the current snapshot or the bench_baseline tag — the cursor needs both")
        start = now - base
        data = _contiguous(fs, pq, files, start, rows)
        if data is None or data.num_rows == 0:
            print(f"   {a.namespace}.{tbl}: held-out rows exhausted at {start} — nothing appended")
            continue
        print(f"   {a.namespace}.{tbl}: +{data.num_rows} held-out rows @ {start}")
        sd._write_and_add(fs, t, data, data.num_rows, f"{a.namespace}.{tbl}", strict=False)
        appended += data.num_rows
    print(f"   == tick timing: total {time.time() - t0:.1f}s")
    if appended == 0:
        # every held-out stream is used up: a tick that appends nothing must
        # fail, not merge 0 rows and report a fast MATCH
        raise SystemExit("FATAL: held-out rows exhausted for every table — nothing appended (roll back to bench_baseline to restart the stream)")


if __name__ == "__main__":
    main()
