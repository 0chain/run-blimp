import sys, json, time
env={}
for l in open(sys.argv[1]):
    l=l.strip()
    if "=" in l and not l.startswith("#"):
        k,v=l.split("=",1); env[k.replace("export ","").strip()]=v.strip().strip('"').strip("'")
from pyiceberg.catalog import load_catalog
cat=load_catalog("c",type="rest",uri=env["ICEBERG_URL"],warehouse=env["WAREHOUSE"],**{"s3.endpoint":env.get("S3_ENDPOINT",""),"s3.access-key-id":env.get("S3_KEY",""),"s3.secret-access-key":env.get("S3_SECRET",""),"s3.region":env.get("REGION","us-east-1")})
ns=env.get("NAMESPACE","tpcds"); out={"ns":ns,"tables":{}}
for tid in sorted(cat.list_tables(ns)):
    t=cat.load_table(tid); c=t.current_snapshot()
    out["tables"][tid[-1]]={"metadata_location":t.metadata_location,"current":c.snapshot_id if c else None,
        "refs":{k:v.snapshot_id for k,v in t.metadata.refs.items()}}
f=f"/root/bench_ns_backup_{ns}_{time.strftime('%Y%m%d_%H%M%S')}.json"
json.dump(out,open(f,"w"),indent=1); print(f, len(out["tables"]), "tables")
