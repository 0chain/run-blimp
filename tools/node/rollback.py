import sys,json,time
from pyiceberg.catalog import load_catalog
envf,mode=sys.argv[1],sys.argv[2]   # mode: dry | apply | restore <backup.json>
env={}
for l in open(envf):
    l=l.strip()
    if '=' in l and not l.startswith('#'):
        k,v=l.split('=',1); env[k.replace('export ','').strip()]=v.strip().strip('"').strip("'")
cat=load_catalog('c',type='rest',uri=env['ICEBERG_URL'],warehouse=env['WAREHOUSE'],**{'s3.endpoint':env.get('S3_ENDPOINT',''),'s3.region':env.get('REGION','us-east-1')})
ns=env.get('NAMESPACE','tpcds')
if mode=='restore':
    bk=json.load(open(sys.argv[3]))
    for name,sid in bk['current'].items():
        t=cat.load_table((ns,name)); t.manage_snapshots().set_current_snapshot(snapshot_id=sid).commit(); print('restored',name,sid)
    sys.exit()

# THE BASELINE IS ONE POINT IN TIME, TAGGED. Rolling each table to its own
# earliest retained snapshot stopped meaning "the dataset as loaded" once
# snapshot expiry ran (source maintenance, 2026-10-24): every table's earliest
# kept snapshot was a different post-load moment, so web_returns kept returns
# of orders the web_sales rollback removed, and the next appends collided with
# them. Now: the first run picks ONE time T (the latest of the tables'
# earliest retained snapshots, so every table has history at T), rolls each
# table to its last ancestor at or before T, and tags it bench_baseline with an
# explicit max-ref-age so expiry keeps it (iceberg-go drops a tag older than
# the cutoff when the tag has no max-ref-age of its own). Later runs roll to
# the tag.
TAG='bench_baseline'
TAG_AGE_MS=100*365*24*3600*1000

def ancestors(t):
    snaps={s.snapshot_id:s for s in t.snapshots()}
    c=t.current_snapshot(); out=[]
    while c is not None:
        out.append(c)
        p=c.parent_snapshot_id
        c=snaps.get(p) if p is not None else None
    return out  # newest first

tables={tid[-1]:cat.load_table(tid) for tid in sorted(cat.list_tables(ns))}
tagged={n:t.metadata.refs[TAG].snapshot_id for n,t in tables.items() if TAG in t.metadata.refs}
T=None
if len(tagged)<len(tables):
    T=max(min(s.timestamp_ms for s in ancestors(t)) for t in tables.values())
plan={};cur={}
def tot(s): return s.summary.additional_properties.get('total-records') if s and s.summary else None
for name,t in tables.items():
    c=t.current_snapshot(); cur[name]=c.snapshot_id
    if name in tagged:
        plan[name]=tagged[name]; how='tag'
    else:
        anc=[s for s in ancestors(t) if s.timestamp_ms<=T]
        if not anc:
            print(f"{name:<24} NO ANCESTOR AT OR BEFORE {T} — False"); sys.exit(1)
        plan[name]=anc[0].snapshot_id; how='T=%d'%T
    s=t.snapshot_by_id(plan[name])
    print(f"{name:<24} current {c.snapshot_id} (tot={tot(c)}) -> first {plan[name]} (tot={tot(s)}) [{how}]")
if mode=='dry':
    sys.exit()
bf=f"/root/snapshot_backup_{time.strftime('%Y%m%d_%H%M%S')}.json"; json.dump({'ns':ns,'current':cur,'first':plan},open(bf,'w'),indent=1); print('backup',bf)
for name,sid in plan.items():
    t=cat.load_table((ns,name))
    if name not in tagged:
        t.manage_snapshots().create_tag(snapshot_id=sid,tag_name=TAG,max_ref_age_ms=TAG_AGE_MS).commit()
        t=cat.load_table((ns,name))
    t.manage_snapshots().set_current_snapshot(snapshot_id=sid).commit()
    t=cat.load_table((ns,name)); print('rolled',name,t.current_snapshot().snapshot_id==sid, tot(t.current_snapshot()))
