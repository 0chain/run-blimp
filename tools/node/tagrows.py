import sys
env={}
for l in open(sys.argv[1]):
    l=l.strip()
    if "=" in l and not l.startswith("#"):
        k,v=l.split("=",1); env[k.replace("export ","").strip()]=v.strip().strip('"').strip("'")
from pyiceberg.catalog import load_catalog
cat=load_catalog("c",type="rest",uri=env["ICEBERG_URL"],warehouse=env["WAREHOUSE"],**{"s3.endpoint":env.get("S3_ENDPOINT",""),"s3.access-key-id":env.get("S3_KEY",""),"s3.secret-access-key":env.get("S3_SECRET",""),"s3.region":env.get("REGION","us-east-1")})
ns=env.get("NAMESPACE","tpcds")
for t in sys.argv[2:]:
    tb=cat.load_table((ns,t)); r=tb.metadata.refs.get("bench_baseline")
    s=tb.snapshot_by_id(r.snapshot_id) if r else None
    print(t,"tag",s.summary.get("total-records") if s else None,"current",tb.current_snapshot().summary.get("total-records"))
