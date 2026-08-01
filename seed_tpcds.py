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
def conform_to_table_schema(tbl, table, n):
    """Expand a synthesized subset delta to the target table's FULL physical
    schema, null-filling the columns we don't synthesize. WITHOUT this the delta
    parquet carried only the ~12 measure/key columns; a query that reads the fact
    as a raw base+delta parquet glob (q64's cross_sales scans store_sales for
    ss_cdemo_sk etc.) then hits "schema mismatch in glob: column X was read from
    the original file ... but could not be found in <seed>.parquet" -> duckdb exit
    1. add_files null-fills for iceberg-schema reads, but a physical glob needs the
    files uniform. Conforming to table.schema() makes every delta file identical
    in shape to the base chunks."""
    import pyarrow as pa
    from pyiceberg.io.pyarrow import schema_to_pyarrow
    aschema = schema_to_pyarrow(table.schema())
    # schema_to_pyarrow stamps PARQUET:field_id on every field, and pq.write_table
    # then writes those IDs into the file — which add_files refuses outright:
    # "Cannot add file ... because it has field IDs. `add_files` only supports
    # addition of files without field_ids". That aborted the CDC append for all
    # five facts (AWS SF1, 2026-08-01), so every delta-merge measured 0 rows.
    # Only the metadata is a problem; names/types/nullability are what both the
    # physical glob and the iceberg-schema read actually need, so keep those.
    aschema = pa.schema([pa.field(f.name, f.type, f.nullable) for f in aschema])
    have = {name: tbl[name] for name in tbl.column_names}
    cols = []
    for field in aschema:
        if field.name in have:
            arr = have[field.name]
            if not arr.type.equals(field.type):
                arr = arr.cast(field.type)
            cols.append(arr)
        else:
            cols.append(pa.nulls(n, field.type))
    return pa.table(cols, schema=aschema)
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--catalog",required=True); ap.add_argument("--warehouse",required=True)
    ap.add_argument("--namespace",default="tpcds"); ap.add_argument("--table",default="store_returns")
    ap.add_argument("--rows",type=int,default=100000); ap.add_argument("--s3-region",default="us-east-1")
    ap.add_argument("--s3-endpoint",default="",help="custom S3 endpoint (MinIO etc.); blank = AWS (same convention as register_tpcds_tables.py)")
    ap.add_argument("--mode",choices=["append","upsert"],default="append",
        help="append = add_files only (incremental-mergeable). upsert = copy-on-write "
             "DELETE of an sr_store_sk slice + replacement rows: rewrites data files, so "
             "the snapshot has REMOVED files and the gateway must full-rematerialize.")
    ap.add_argument("--upsert-store-sk",type=int,default=7,help="store slice replaced in upsert mode")
    a=ap.parse_args()
    if not a.s3_endpoint:
        # env fallback so callers that export AWS_ENDPOINT_URL (bench launchers)
        # get MinIO routing without a CLI change
        import os
        a.s3_endpoint=os.environ.get("S3_ENDPOINT") or os.environ.get("AWS_ENDPOINT_URL") or ""
    from pyiceberg.catalog.rest import RestCatalog
    import pyarrow as pa
    props={"s3.region":a.s3_region}
    if a.s3_endpoint:
        # Custom endpoint: point BOTH pyiceberg's file IO and the writer at it
        # (without this, PyArrow S3 IO resolves bucket names against real AWS ->
        # ACCESS_DENIED on a local-only bucket; observed q64 bench 2026-07-29).
        # Same convention as register_tpcds_tables.py: s3.endpoint always; path-
        # style ONLY for non-AWS endpoints (MinIO needs it; forcing it on real
        # AWS would regress newer buckets — S3_ENDPOINT is commonly the AWS
        # regional URL on blimp nodes, per standup_data.sh).
        props["s3.endpoint"]=a.s3_endpoint
        if "amazonaws.com" not in a.s3_endpoint:
            props["s3.path-style-access"]="true"
    cat=RestCatalog("kit",uri=a.catalog,warehouse=a.warehouse,**props)
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
    elif a.table=="catalog_sales":
        # keys+measures for the catalog-fact CDC queries (q15: bill_customer/
        # sold_date/sales_price; q99: ship_date/warehouse/ship_mode/call_center)
        store_col="cs_call_center_sk"
        schema=pa.schema([("cs_sold_date_sk",pa.int32()),("cs_ship_date_sk",pa.int32()),
            ("cs_item_sk",pa.int32()),("cs_bill_customer_sk",pa.int32()),
            ("cs_warehouse_sk",pa.int32()),("cs_ship_mode_sk",pa.int32()),
            ("cs_call_center_sk",pa.int32()),("cs_quantity",pa.int32()),
            ("cs_order_number",pa.int64()),
            ("cs_sales_price",pa.decimal128(7,2)),("cs_ext_sales_price",pa.decimal128(7,2)),
            ("cs_ext_list_price",pa.decimal128(7,2)),
            ("cs_net_profit",pa.decimal128(7,2))])
        cc_pool=[a.upsert_store_sk]*n if a.mode=="upsert" else [random.randint(1,6) for _ in range(n)]
        sold=[random.randint(2451545,2451910) for _ in range(n)]
        ordbase=random.randint(10**9,9*10**9)
        tbl_data=pa.table({
            "cs_sold_date_sk":sold,
            "cs_ship_date_sk":[d+random.randint(2,90) for d in sold],
            "cs_item_sk":[random.randint(1,300000) for _ in range(n)],
            "cs_bill_customer_sk":[random.randint(1,2000000) for _ in range(n)],
            "cs_warehouse_sk":[random.randint(1,20) for _ in range(n)],
            "cs_ship_mode_sk":[random.randint(1,20) for _ in range(n)],
            "cs_call_center_sk":cc_pool,
            "cs_quantity":[random.randint(1,100) for _ in range(n)],
            "cs_order_number":[ordbase+i for i in range(n)],
            "cs_sales_price":D(1,300),"cs_ext_sales_price":D(1,5000),
            "cs_ext_list_price":D(100,6000),
            "cs_net_profit":D(-500,4000)},schema=schema)
    elif a.table=="web_sales":
        # q45: bill_customer/sold_date/item/sales_price; q62: ship_date/warehouse/
        # ship_mode/web_site
        store_col="ws_web_site_sk"
        schema=pa.schema([("ws_sold_date_sk",pa.int32()),("ws_ship_date_sk",pa.int32()),
            ("ws_item_sk",pa.int32()),("ws_bill_customer_sk",pa.int32()),
            ("ws_warehouse_sk",pa.int32()),("ws_ship_mode_sk",pa.int32()),
            ("ws_web_site_sk",pa.int32()),("ws_quantity",pa.int32()),
            ("ws_sales_price",pa.decimal128(7,2)),("ws_ext_sales_price",pa.decimal128(7,2)),
            ("ws_net_profit",pa.decimal128(7,2))])
        ws_pool=[a.upsert_store_sk]*n if a.mode=="upsert" else [random.randint(1,30) for _ in range(n)]
        sold=[random.randint(2451545,2451910) for _ in range(n)]
        tbl_data=pa.table({
            "ws_sold_date_sk":sold,
            "ws_ship_date_sk":[d+random.randint(2,90) for d in sold],
            "ws_item_sk":[random.randint(1,300000) for _ in range(n)],
            "ws_bill_customer_sk":[random.randint(1,2000000) for _ in range(n)],
            "ws_warehouse_sk":[random.randint(1,20) for _ in range(n)],
            "ws_ship_mode_sk":[random.randint(1,20) for _ in range(n)],
            "ws_web_site_sk":ws_pool,
            "ws_quantity":[random.randint(1,100) for _ in range(n)],
            "ws_sales_price":D(1,300),"ws_ext_sales_price":D(1,5000),"ws_net_profit":D(-500,4000)},schema=schema)
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
    # write_statistics=False: conform_to_table_schema null-fills every column we
    # don't synthesize, so the delta carries all-null decimal columns. parquet
    # writes their stats with min_raw=None, and pyiceberg's stats reader calls
    # Decimal(None) on it unguarded -> "conversion from NoneType to Decimal is not
    # supported" (pyiceberg/io/pyarrow.py:2344), aborting add_files for every fact
    # whose synthesized set doesn't cover its decimals. The delta files are tiny,
    # so the lost row-group pruning costs nothing.
    #
    # Write the delta parquet OURSELVES with int-backed decimals and add_files it.
    # (t.append's writer emits FIXED_LEN_BYTE_ARRAY decimals, which clashes with a
    # table registered from int32-backed parquet — pyiceberg 0.9 raises
    # "Unexpected physical type FIXED_LEN_BYTE_ARRAY ... expected INT32".
    # store_decimal_as_integer matches the table's physical encoding exactly.)
    import uuid, s3fs, pyarrow.parquet as pq
    # Full-schema delta: null-fill every column the fact has but we didn't
    # synthesize, so base+delta parquet are physically uniform (glob-safe).
    tbl_data=conform_to_table_schema(tbl_data,t,n)
    loc=t.location().rstrip("/")
    key=f"{loc}/data/seed-{uuid.uuid4().hex}.parquet"
    fs_kwargs={"region_name":a.s3_region}
    if a.s3_endpoint: fs_kwargs["endpoint_url"]=a.s3_endpoint
    fs=s3fs.S3FileSystem(client_kwargs=fs_kwargs)
    with fs.open(key.replace("s3://","",1),"wb") as f:
        pq.write_table(tbl_data,f,store_decimal_as_integer=True,write_statistics=False)
    t.add_files(file_paths=[key]); t.refresh()
    print(f"{a.namespace}.{a.table}: +{n} rows -> snapshot {t.current_snapshot().snapshot_id}")
    if a.table=="catalog_sales" and a.mode=="append":
        # Referential CDC (q64-class): cs_ui joins catalog_sales×catalog_returns on
        # (item_sk, order_number) — a sales-only append contributes NO cs_ui delta.
        # Append matching returns for ~1/3 of the new sales, refunds small enough
        # that the HAVING sale > 2*refund band keeps the groups.
        m=max(1,n//3)
        ridx=random.sample(range(n),m)
        r_item=[tbl_data["cs_item_sk"][i].as_py() for i in ridx]
        r_ord=[tbl_data["cs_order_number"][i].as_py() for i in ridx]
        RD=lambda lo,hi:[decimal.Decimal(str(round(random.uniform(lo,hi),2))) for _ in range(m)]
        rschema=pa.schema([("cr_item_sk",pa.int32()),("cr_order_number",pa.int64()),
            ("cr_refunded_cash",pa.decimal128(7,2)),("cr_reversed_charge",pa.decimal128(7,2)),
            ("cr_store_credit",pa.decimal128(7,2))])
        rdata=pa.table({"cr_item_sk":r_item,"cr_order_number":r_ord,
            "cr_refunded_cash":RD(0,20),"cr_reversed_charge":RD(0,10),
            "cr_store_credit":RD(0,10)},schema=rschema)
        rt=cat.load_table((a.namespace,"catalog_returns"))
        rdata=conform_to_table_schema(rdata,rt,m)
        rkey=f"{rt.location().rstrip('/')}/data/seed-{uuid.uuid4().hex}.parquet"
        with fs.open(rkey.replace("s3://","",1),"wb") as f:
            pq.write_table(rdata,f,store_decimal_as_integer=True,write_statistics=False)
        rt.add_files(file_paths=[rkey]); rt.refresh()
        print(f"{a.namespace}.catalog_returns: +{m} referential rows -> snapshot {rt.current_snapshot().snapshot_id}")
if __name__=="__main__": main()
