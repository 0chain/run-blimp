#!/usr/bin/env python3
"""bench_reset.py <envfile> dry|apply [--no-prune] [--no-rollback]

Reset a bench namespace to its base data between query runs:

  1. ROLLBACK  every table's current snapshot to its FIRST snapshot (the base
               load). Backup of the current ids → /root/bench_reset_<ts>.json.
  2. EXPIRE    the snapshots the rollback abandoned (every snapshot that is
               neither current nor an ancestor). Metadata only.
  3. PRUNE     the data files those snapshots appended: objects under the
               table's OWN data prefix (<location>/data/) that NO remaining
               snapshot references and that are older than PRUNE_MIN_AGE_S
               (default 1800 s — never a file a writer has not committed yet).
               Base files outside the table location (s3://tpcds10/…,
               s3://tpcds1000/tpch100/…) are never listed; base files inside
               it are referenced by the first snapshot and so are kept.
               The deleted keys are written to /root/bench_prune_<ts>.txt.

  4. METADATA  delete the table's metadata.json files that are neither its current
               metadata nor in its metadata log, older than PRUNE_MIN_AGE_S.
               Every commit writes one and Iceberg never deletes the old ones
               unless write.metadata.delete-after-commit.enabled is set (node
               37, 2026-10-25: ~8,700 per fact table, ~33 GB).

--no-rollback keeps every table at its current snapshot (safe while a batch is
running): only snapshots off the current lineage and older than
PRUNE_MIN_AGE_S are expired, then steps 3-4 run against the current lineage.

dry (default) only prints what each step would do. Without the prune the
appended files stay as unreferenced orphans forever: a 10-hour batch leaves
~160 ticks of appends per fact table on disk.
"""
import sys, json, time, os
from datetime import datetime, timezone
from pyiceberg.catalog import load_catalog

if len(sys.argv) < 3 or sys.argv[2] not in ("dry", "apply"):
    sys.exit(__doc__)
envf, mode = sys.argv[1], sys.argv[2]
prune = "--no-prune" not in sys.argv[3:]
no_rollback = "--no-rollback" in sys.argv[3:]
env = {}
for l in open(envf):
    l = l.strip()
    if "=" in l and not l.startswith("#"):
        k, v = l.split("=", 1)
        env[k.replace("export ", "").strip()] = v.strip().strip('"').strip("'")
props = {"s3.endpoint": env.get("S3_ENDPOINT", ""), "s3.region": env.get("REGION", "us-east-1"),
         "s3.access-key-id": env.get("S3_KEY", ""), "s3.secret-access-key": env.get("S3_SECRET", "")}
cat = load_catalog("c", type="rest", uri=env["ICEBERG_URL"], warehouse=env["WAREHOUSE"], **props)
ns = env.get("NAMESPACE", "tpcds")
apply = mode == "apply"
ts = time.strftime("%Y%m%d_%H%M%S")
min_age = int(os.environ.get("PRUNE_MIN_AGE_S", "1800"))

# 1. rollback
cur, first = {}, {}
for tid in sorted(cat.list_tables(ns)):
    t = cat.load_table(tid)
    c = t.current_snapshot()
    if c is None:
        continue
    f = sorted(t.snapshots(), key=lambda s: s.timestamp_ms)[0]
    cur[tid[-1]], first[tid[-1]] = c.snapshot_id, f.snapshot_id
if apply:
    bf = f"/root/bench_reset_{ts}.json"
    json.dump({"ns": ns, "current": cur, "first": first}, open(bf, "w"), indent=1)
    print("backup", bf)
moved = 0
for name, sid in ({} if no_rollback else first).items():
    if cur[name] == sid:
        continue
    moved += 1
    print(f"rollback {name:<24} {cur[name]} -> {sid}")
    if apply:
        t = cat.load_table((ns, name))
        t.manage_snapshots().set_current_snapshot(snapshot_id=sid).commit()
print(f"rolled back {moved} table(s)")
rolled = [] if no_rollback else [n for n in first if cur[n] != first[n]]
cutoff_ms = (time.time() - min_age) * 1000

# 2. expire abandoned snapshots (metadata only)
from pyiceberg.table.update import RemoveSnapshotsUpdate
for name in sorted(first):
    t = cat.load_table((ns, name))
    c = t.current_snapshot()
    keep, sid = set(), (first[name] if not (apply or no_rollback) else (c.snapshot_id if c else None))
    while sid is not None:
        keep.add(sid)
        s = t.snapshot_by_id(sid)
        sid = s.parent_snapshot_id if s else None
    drop = [s.snapshot_id for s in t.snapshots() if s.snapshot_id not in keep
            and not (no_rollback and s.timestamp_ms > cutoff_ms)]
    if drop:
        print(f"expire   {name:<24} drop {len(drop)} snapshot(s), keep {len(keep)}")
        if apply:
            for i in range(0, len(drop), 200):
                t = cat.load_table((ns, name))
                cat.commit_table(t, requirements=(), updates=tuple(RemoveSnapshotsUpdate(snapshot_ids=[d]) for d in drop[i:i + 200]))

# 3. prune data files no remaining snapshot references
if not prune:
    print("prune skipped (--no-prune)")
    print("tables rolled back:", " ".join(rolled))
    sys.exit(0)
import boto3
s3 = boto3.client("s3", endpoint_url=env.get("S3_ENDPOINT") or None, region_name=env.get("REGION", "us-east-1"),
                  aws_access_key_id=env.get("S3_KEY") or None, aws_secret_access_key=env.get("S3_SECRET") or None)
now = datetime.now(timezone.utc)
log = open(f"/root/bench_prune_{ts}.txt", "w") if apply else None
tot_n = tot_b = 0
for name in sorted(first):
    t = cat.load_table((ns, name))
    loc = t.location().rstrip("/")
    if not loc.startswith("s3://"):
        continue
    bucket, _, prefix = loc[5:].partition("/")
    prefix = prefix + "/data/"
    # After the expire every snapshot left is kept. In dry mode the expire did
    # not happen: what WOULD remain is the first snapshot's lineage (or, with
    # --no-rollback, the current lineage plus the snapshots too young to expire).
    if apply:
        keep_ids = {s.snapshot_id for s in t.snapshots()}
    else:
        keep_ids = set()
        sid = t.current_snapshot().snapshot_id if no_rollback else first[name]
        while sid is not None:
            keep_ids.add(sid)
            s = t.snapshot_by_id(sid)
            sid = s.parent_snapshot_id if s else None
        if no_rollback:
            keep_ids |= {s.snapshot_id for s in t.snapshots() if s.timestamp_ms > cutoff_ms}
    ref, seen = set(), {}
    for s in t.snapshots():
        if s.snapshot_id not in keep_ids:
            continue
        for m in s.manifests(t.io):
            seen.setdefault(m.manifest_path, m)
    for m in seen.values():
        for e in m.fetch_manifest_entry(t.io, discard_deleted=False):
            p = e.data_file.file_path
            if p.startswith("s3://"):
                b, _, k = p[5:].partition("/")
                ref.add((b, k))
    cand = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            if (bucket, o["Key"]) in ref:
                continue
            if (now - o["LastModified"]).total_seconds() < min_age:
                continue
            cand.append(o)
    b = sum(o["Size"] for o in cand)
    tot_n += len(cand); tot_b += b
    if cand:
        print(f"prune    {name:<24} {len(cand)} unreferenced file(s), {b/1e9:.2f} GB (referenced {len(ref)})")
    if apply and cand:
        for i in range(0, len(cand), 1000):
            chunk = cand[i:i + 1000]
            s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": o["Key"]} for o in chunk], "Quiet": True})
            for o in chunk:
                log.write(f"s3://{bucket}/{o['Key']}\t{o['Size']}\n")
    # 4. metadata files off the log
    mkeep = {t.metadata_location} | {e.metadata_file for e in t.metadata.metadata_log}
    mkeep = {k[5:].partition("/")[2] for k in mkeep if k and k.startswith("s3://")}
    mcand = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=loc[5:].partition("/")[2] + "/metadata/"):
        for o in page.get("Contents", []):
            if not o["Key"].endswith(".metadata.json") or o["Key"] in mkeep:
                continue
            if (now - o["LastModified"]).total_seconds() < min_age:
                continue
            mcand.append(o)
    mb = sum(o["Size"] for o in mcand)
    tot_n += len(mcand); tot_b += mb
    if mcand:
        print(f"metadata {name:<24} {len(mcand)} file(s) off the log, {mb/1e9:.2f} GB (kept {len(mkeep)})")
    if apply and mcand:
        for i in range(0, len(mcand), 1000):
            chunk = mcand[i:i + 1000]
            s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": o["Key"]} for o in chunk], "Quiet": True})
            for o in chunk:
                log.write(f"s3://{bucket}/{o['Key']}\t{o['Size']}\n")
print(f"{'pruned' if apply else 'would prune'} {tot_n} file(s), {tot_b/1e9:.2f} GB")
print("tables rolled back:", " ".join(rolled))
