# expire_abandoned.py <envfile> dry|apply : after a bench rollback, drop the
# snapshots the rollback abandoned (every snapshot that is not the current one
# or one of its ancestors). Metadata only (pyiceberg expire_snapshots does not
# delete files). Backs up each table's snapshot list first.
import sys, json, time
from pyiceberg.catalog import load_catalog
envf, mode = sys.argv[1], sys.argv[2]
env = {}
for l in open(envf):
    l = l.strip()
    if '=' in l and not l.startswith('#'):
        k, v = l.split('=', 1); env[k.replace('export ', '').strip()] = v.strip().strip('"').strip("'")
cat = load_catalog('c', type='rest', uri=env['ICEBERG_URL'], warehouse=env['WAREHOUSE'],
                   **{'s3.endpoint': env.get('S3_ENDPOINT', ''), 's3.region': env.get('REGION', 'us-east-1')})
ns = env.get('NAMESPACE', 'tpcds')
bk = {}
for tid in sorted(cat.list_tables(ns)):
    t = cat.load_table(tid)
    cur = t.current_snapshot()
    if cur is None:
        continue
    keep, sid = set(), cur.snapshot_id
    while sid is not None:
        keep.add(sid)
        s = t.snapshot_by_id(sid)
        sid = s.parent_snapshot_id if s else None
    drop = [s.snapshot_id for s in t.snapshots() if s.snapshot_id not in keep]
    bk[tid[-1]] = {'current': cur.snapshot_id, 'keep': sorted(keep), 'dropped': drop}
    print(f"{tid[-1]:<24} snapshots={len(t.snapshots())} keep={len(keep)} drop={len(drop)}")
    if mode in ('apply', 'one') and drop:
        # the REST server takes ONE id per remove-snapshots update: send one
        # update per id, in commits of 200
        from pyiceberg.table.update import RemoveSnapshotsUpdate
        for i in range(0, len(drop), 200):
            t = cat.load_table(tid)
            cat.commit_table(t, requirements=(), updates=tuple(RemoveSnapshotsUpdate(snapshot_ids=[d]) for d in drop[i:i+200]))
        print(f"  expired -> {len(cat.load_table(tid).snapshots())} left")
        if mode == 'one':
            break
if mode in ('apply','one'):
    bf = f"/root/expire_backup_{time.strftime('%Y%m%d_%H%M%S')}.json"
    json.dump(bk, open(bf, 'w'), indent=1); print('backup', bf)
