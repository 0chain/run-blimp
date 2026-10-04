#!/usr/bin/env python3
"""Prove a delta-merge actually folded rows in — the acceptance gate for any
merge_ms number.

WHY THIS EXISTS
---------------
A merge against an EMPTY delta still runs and still reports a merge_ms, so the
timing alone cannot tell a real merge from one that did nothing. Timings from a
run that has not passed this gate must not be reported as results.

HOW IT WORKS
------------
An append merge writes each delta as
    s3://<mv-namespace-with-dashes>/<mv_table>/delta-<unix_ms>.parquet
listed in <mv_table>/parts.json, alongside the base <mv_table>/data.parquet.

So:
    snapshot  -> record each MV's current part list + data.parquet ETag
    <append + re-run the queries>
    verdict   -> count rows in the parts that appeared since the snapshot

VERDICT VOCABULARY (a merge can be real without writing a part)
    merged        new delta part(s) with rows > 0            -> PROVEN non-empty
    rebaselined   no new part but data.parquet ETag changed  -> the full
                  re-aggregation lane ran and produced different content
    EMPTY         new delta part(s) totalling 0 rows         -> merge measured nothing
    UNCHANGED     no new part and identical ETag             -> nothing happened

Usage:
    mv_delta_rows.py snapshot --out pre.json  --tables mv_a mv_b
    mv_delta_rows.py verdict  --pre pre.json  --tables mv_a mv_b
Connection: --endpoint/--bucket, or MV_S3_ENDPOINT / MV_BUCKET / AWS_* env.
"""
import argparse, json, os, sys


def _fs(endpoint, key, secret, region):
    import s3fs
    ck = {"region_name": region}
    if endpoint:
        ck["endpoint_url"] = endpoint
    return s3fs.S3FileSystem(key=key or None, secret=secret or None, client_kwargs=ck)


def _parts(fs, bucket, table):
    """The MV's delta part names. parts.json is authoritative (the gateway writes
    it transactionally with the part); fall back to a LIST on the delta- prefix,
    which is what the gateway itself does when the manifest is unreadable."""
    try:
        with fs.open(f"{bucket}/{table}/parts.json", "rb") as f:
            return list(json.load(f).get("parts") or [])
    except Exception:
        try:
            return sorted(p.rsplit("/", 1)[-1]
                          for p in fs.ls(f"{bucket}/{table}", detail=False)
                          if "/delta-" in p and p.endswith(".parquet"))
        except Exception:
            return []


def _etag(fs, bucket, table):
    """Content signature of the base MV parquet. Row count alone cannot tell you
    whether a merge did anything: these are additive aggregates at a fixed grain,
    so appended rows land in EXISTING groups and the count does not move even
    when every measure changed."""
    try:
        return (fs.info(f"{bucket}/{table}/data.parquet").get("ETag") or "").strip('"')
    except Exception:
        return ""


def _rows(fs, bucket, table, part):
    """Row count straight from the parquet footer — no data pages are read."""
    import pyarrow.parquet as pq
    with fs.open(f"{bucket}/{table}/{part}", "rb") as f:
        return pq.ParquetFile(f).metadata.num_rows


def snapshot(fs, bucket, tables):
    return {t: {"parts": _parts(fs, bucket, t), "etag": _etag(fs, bucket, t)}
            for t in tables}


def verdict(fs, bucket, tables, pre):
    out = {}
    for t in tables:
        before = pre.get(t) or {"parts": [], "etag": ""}
        now_parts, now_etag = _parts(fs, bucket, t), _etag(fs, bucket, t)
        new = [p for p in now_parts if p not in set(before["parts"])]
        rows, per = 0, {}
        for p in new:
            try:
                per[p] = _rows(fs, bucket, t, p)
            except Exception as e:
                per[p] = f"unreadable: {e}"
                continue
            rows += per[p]
        # No baseline ETag means this MV was not in the snapshot at all, so a
        # "changed" verdict would be manufactured out of nothing — exactly the
        # kind of false green this gate exists to prevent.
        changed = bool(now_etag) and bool(before["etag"]) and now_etag != before["etag"]
        if new and rows > 0:
            v = "merged"
        elif new:
            v = "EMPTY"
        elif not before["etag"]:
            v = "NO-BASELINE"
        elif changed:
            # A compaction merge rewrites data.parquet and PURGES the parts, so
            # "parts went away + content changed" is also a real merge.
            v = "rebaselined"
        else:
            v = "UNCHANGED"
        out[t] = {"verdict": v, "delta_rows": rows, "new_parts": per,
                  "etag_changed": changed}
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["snapshot", "verdict"])
    ap.add_argument("--tables", nargs="+", required=True,
                    help="MV table names (the mv_table field of /admin/query/run)")
    ap.add_argument("--bucket", default=os.environ.get("MV_BUCKET", ""),
                    help="MV bucket = the MV namespace with _ replaced by - "
                         "(tpcds_mv -> tpcds-mv)")
    ap.add_argument("--endpoint", default=os.environ.get("MV_S3_ENDPOINT", "")
                    or os.environ.get("S3_ENDPOINT", ""))
    ap.add_argument("--key", default=os.environ.get("MV_S3_KEY", "")
                    or os.environ.get("AWS_ACCESS_KEY_ID", ""))
    ap.add_argument("--secret", default=os.environ.get("MV_S3_SECRET", "")
                    or os.environ.get("AWS_SECRET_ACCESS_KEY", ""))
    ap.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"))
    ap.add_argument("--out", default="")
    ap.add_argument("--pre", default="")
    ap.add_argument("--strict", action="store_true",
                    help="exit 1 unless EVERY table merged or rebaselined — use "
                         "this to fail a bench run whose timings measured nothing")
    a = ap.parse_args()
    if not a.bucket:
        sys.exit("--bucket (or MV_BUCKET) required: the MV namespace with '_'->'-'")
    tables = [t for t in a.tables if t and t != "none"]
    fs = _fs(a.endpoint, a.key, a.secret, a.region)

    if a.action == "snapshot":
        snap = snapshot(fs, a.bucket, tables)
        txt = json.dumps(snap, indent=1)
        if a.out:
            open(a.out, "w").write(txt)
        else:
            print(txt)
        return

    pre = json.load(open(a.pre)) if a.pre else {}
    res = verdict(fs, a.bucket, tables, pre)
    print(json.dumps(res, indent=1))
    if a.strict:
        bad = [t for t, r in res.items()
               if r["verdict"] in ("EMPTY", "UNCHANGED", "NO-BASELINE")]
        if bad:
            sys.exit(f"DELTA GATE FAILED for {bad} — their merge_ms measured an "
                     "empty delta and must not be reported as a result")


if __name__ == "__main__":
    main()
