#!/usr/bin/env python3
"""Register/append TPC-DS-style tables into your Iceberg REST catalog so a Blimp
cluster has something to query. Requires: pip install "pyiceberg[s3fs]" pyarrow.

  python3 seed_tpcds.py --catalog http://localhost:8181 \
    --warehouse s3://YOUR-BUCKET/iceberg_wh --namespace tpcds \
    --table store_returns --rows 100000

Point Blimp's Production tab at:  Iceberg REST URL = http://<this-host>:8181,
Warehouse = your warehouse, Namespace = tpcds. Leave S3 keys blank if the Blimp
cluster runs in the same AWS account (it reads via its instance role)."""
import argparse, random, decimal
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--catalog",required=True); ap.add_argument("--warehouse",required=True)
    ap.add_argument("--namespace",default="tpcds"); ap.add_argument("--table",default="store_returns")
    ap.add_argument("--rows",type=int,default=100000); ap.add_argument("--s3-region",default="us-east-1")
    ap.add_argument("--mode",choices=["append","upsert"],default="append",
        help="append = add_files only (incremental-mergeable). upsert = copy-on-write "
             "DELETE of an sr_store_sk slice + replacement rows: rewrites data files, so "
             "the snapshot has REMOVED files and the gateway must full-rematerialize.")
    ap.add_argument("--upsert-store-sk",type=int,default=7,help="store slice replaced in upsert mode")
    a=ap.parse_args()
    from pyiceberg.catalog.rest import RestCatalog
    import pyarrow as pa
    cat=RestCatalog("kit",uri=a.catalog,warehouse=a.warehouse,**{"s3.region":a.s3_region})
    try: cat.create_namespace((a.namespace,))
    except Exception: pass
    # per-table minimal schema — just the measure/key columns the CDC queries
    # aggregate. A partial-column delta is fine: add_files nulls the rest, and the
    # single-fact SUM group-by merges on the columns present. d_year=2000 date_sk
    # range so the delta lands inside the queries' d_year=2000 filters.
    n=a.rows; D=lambda lo,hi:[decimal.Decimal(str(round(random.uniform(lo,hi),2))) for _ in range(n)]
    if a.table=="store_sales":
        store_col="ss_store_sk"
        schema=pa.schema([("ss_sold_date_sk",pa.int32()),("ss_item_sk",pa.int32()),
            ("ss_store_sk",pa.int32()),("ss_customer_sk",pa.int32()),("ss_quantity",pa.int32()),
            ("ss_ext_sales_price",pa.decimal128(7,2)),("ss_ext_wholesale_cost",pa.decimal128(7,2)),
            ("ss_net_profit",pa.decimal128(7,2)),("ss_sales_price",pa.decimal128(7,2)),
            ("ss_list_price",pa.decimal128(7,2)),("ss_ext_discount_amt",pa.decimal128(7,2)),
            ("ss_ext_list_price",pa.decimal128(7,2))])
        store_pool=[a.upsert_store_sk]*n if a.mode=="upsert" else [random.randint(1,50) for _ in range(n)]
        tbl_data=pa.table({
            "ss_sold_date_sk":[random.randint(2451545,2451910) for _ in range(n)],
            "ss_item_sk":[random.randint(1,300000) for _ in range(n)],
            "ss_store_sk":store_pool,
            "ss_customer_sk":[random.randint(1,2000000) for _ in range(n)],
            "ss_quantity":[random.randint(1,100) for _ in range(n)],
            "ss_ext_sales_price":D(1,5000),"ss_ext_wholesale_cost":D(1,3000),"ss_net_profit":D(-500,4000),
            "ss_sales_price":D(1,300),"ss_list_price":D(1,400),"ss_ext_discount_amt":D(0,1000),
            "ss_ext_list_price":D(1,6000)},schema=schema)
    else:  # store_returns (default)
        store_col="sr_store_sk"
        schema=pa.schema([("sr_customer_sk",pa.int32()),("sr_store_sk",pa.int32()),
                          ("sr_returned_date_sk",pa.int32()),("sr_return_amt",pa.decimal128(7,2))])
        store_pool=[a.upsert_store_sk]*n if a.mode=="upsert" else [random.randint(1,50) for _ in range(n)]
        tbl_data=pa.table({
            "sr_customer_sk":[random.randint(1,100000) for _ in range(n)],
            "sr_store_sk":store_pool,
            "sr_returned_date_sk":[random.randint(2451545,2451910) for _ in range(n)],
            "sr_return_amt":D(1,5000)},schema=schema)
    ident=(a.namespace,a.table)
    try: t=cat.load_table(ident)
    except Exception: t=cat.create_table(ident,schema=schema)
    if a.mode=="upsert":
        # Copy-on-write delete of the slice being replaced: pyiceberg rewrites
        # every data file containing matching rows, so the resulting snapshot
        # REMOVES files — exactly what real upserts/overwrites/compactions do.
        # The gateway must detect that (added_files.removed_files) and take the
        # full re-materialize path; an added-only merge would double-count.
        from pyiceberg.expressions import EqualTo
        t.delete(EqualTo(store_col,a.upsert_store_sk)); t.refresh()
        print(f"upsert: deleted {store_col}={a.upsert_store_sk} slice -> snapshot {t.current_snapshot().snapshot_id}")
    # Write the delta parquet OURSELVES with int-backed decimals and add_files it.
    # (t.append's writer emits FIXED_LEN_BYTE_ARRAY decimals, which clashes with a
    # table registered from int32-backed parquet — pyiceberg 0.9 raises
    # "Unexpected physical type FIXED_LEN_BYTE_ARRAY ... expected INT32".
    # store_decimal_as_integer matches the table's physical encoding exactly.)
    import uuid, s3fs, pyarrow.parquet as pq
    loc=t.location().rstrip("/")
    key=f"{loc}/data/seed-{uuid.uuid4().hex}.parquet"
    fs=s3fs.S3FileSystem(client_kwargs={"region_name":a.s3_region})
    with fs.open(key.replace("s3://","",1),"wb") as f:
        pq.write_table(tbl_data,f,store_decimal_as_integer=True)
    t.add_files(file_paths=[key]); t.refresh()
    print(f"{a.namespace}.{a.table}: +{n} rows -> snapshot {t.current_snapshot().snapshot_id}")
if __name__=="__main__": main()
