#!/usr/bin/env python3
# register_tpcds_tables.py — register ALL 24 TPC-DS tables into the kit's Iceberg REST
# catalog from EXISTING parquet in an S3 bucket (e.g. blimp-tpcds1000-aps1, SF1000).
#
# No data copy: pyiceberg `add_files` registers the existing parquet files into a new
# Iceberg table (schema inferred from the parquet). Blimp (source=customer) then pulls
# these for MV generation; CDC appends land in the catalog's own warehouse.
#
# Usage:
#   pip install "pyiceberg[s3fs]>=0.9,<0.10" "pyarrow>=14,<17" s3fs boto3
#   (>=0.9 required: the sf1000 parquet stores small decimals as INT32/INT64,
#    which pyiceberg 0.7/0.8 add_files rejects)
#   python3 register_tpcds_tables.py \
#     --catalog http://<client-priv-ip>:8181 \
#     --warehouse s3://<your-warehouse-bucket>/wh \
#     --source-bucket blimp-tpcds1000-aps1 --region ap-south-1 \
#     --namespace tpcds
#
# The 24 TPC-DS tables live at s3://<source-bucket>/<table>/**/*.parquet.
import argparse, sys
import boto3
import pyarrow.parquet as pq
import s3fs
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError


def bucket_region(bucket):
    """The bucket's ACTUAL region (get-bucket-location; None constraint = us-east-1).
    Returns None if it can't be determined (e.g. no perms)."""
    try:
        loc = boto3.client("s3").get_bucket_location(Bucket=bucket).get("LocationConstraint")
        return loc or "us-east-1"
    except Exception:
        return None

TPCDS_TABLES = [
    "call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer",
    "customer_address", "customer_demographics", "date_dim", "household_demographics",
    "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store",
    "store_returns", "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
    "web_sales", "web_site",
]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True, help="Iceberg REST catalog URL")
    ap.add_argument("--warehouse", required=True, help="s3://bucket/prefix for catalog metadata + CDC appends")
    ap.add_argument("--source-bucket", required=True, help="S3 bucket holding the TPC-DS parquet")
    ap.add_argument("--region", default="ap-south-1")
    ap.add_argument("--namespace", default="tpcds")
    ap.add_argument("--tables", default="", help="comma-separated subset (default: all 24)")
    ap.add_argument("--s3-endpoint", default="", help="custom S3 endpoint (MinIO etc.); blank = AWS")
    args = ap.parse_args()

    tables = [t.strip() for t in args.tables.split(",") if t.strip()] or TPCDS_TABLES

    # Fool-proofing for AWS S3 (skipped for MinIO / custom --s3-endpoint):
    # 1. Trust the SOURCE bucket's ACTUAL region over --region (customers get it
    #    wrong; a mismatch otherwise surfaces as a cryptic HTTP 301).
    # 2. HARD-FAIL if the warehouse bucket is in a different region than the source
    #    — a single S3 endpoint can't serve two regions, so pyiceberg's warehouse
    #    write would 301. Same-region is a hard requirement, so say so plainly.
    if not args.s3_endpoint:
        src_region = bucket_region(args.source_bucket)
        if src_region and src_region != args.region:
            print(f"  NOTE: source bucket {args.source_bucket} is in {src_region}, "
                  f"not --region {args.region} — using {src_region}")
            args.region = src_region
        wh_bkt = args.warehouse.split("/")[2] if args.warehouse.startswith("s3://") else ""
        wh_region = bucket_region(wh_bkt) if wh_bkt else None
        if wh_region and src_region and wh_region != src_region:
            sys.exit(
                f"FATAL: warehouse bucket '{wh_bkt}' is in {wh_region} but source "
                f"'{args.source_bucket}' is in {src_region}. The Iceberg warehouse "
                f"MUST be in the same region as your data — use e.g. "
                f"s3://{args.source_bucket}/wh")

    # DEFAULT to the region-specific S3 endpoint. Without it, s3fs and pyiceberg
    # fall back to the us-east-1 endpoint (s3.amazonaws.com), and any bucket in
    # another region returns HTTP 301. Customer only sets --s3-endpoint for MinIO.
    s3_endpoint = args.s3_endpoint or f"https://s3.{args.region}.amazonaws.com"
    print(f"  s3 endpoint: {s3_endpoint}  region: {args.region}")
    props = {"uri": args.catalog, "warehouse": args.warehouse,
             "s3.region": args.region, "s3.endpoint": s3_endpoint}
    cat = RestCatalog("zus", **props)

    try:
        cat.create_namespace(args.namespace)
    except NamespaceAlreadyExistsError:
        pass

    fs = s3fs.S3FileSystem(client_kwargs={"region_name": args.region, "endpoint_url": s3_endpoint})
    ok = 0
    for tbl in tables:
        prefix = f"{args.source_bucket}/{tbl}/"
        # exclude chunkINCR/: CDC-delta parquet written by gen_incr_delta.py with a
        # different physical layout (FLBA decimals); the CDC test appends those via
        # the API, they must not be in the base snapshot
        files = [f"s3://{p}" for p in fs.find(prefix)
                 if p.endswith(".parquet") and "chunkINCR" not in p]
        if not files:
            print(f"  {tbl:26s} SKIP (no parquet under s3://{prefix})"); continue
        ident = f"{args.namespace}.{tbl}"
        try:
            # infer the Iceberg schema from the first parquet file
            with fs.open(files[0].replace("s3://", ""), "rb") as fh:
                arrow_schema = pq.read_schema(fh)
            try:
                t = cat.create_table(ident, schema=arrow_schema)
            except TableAlreadyExistsError:
                t = cat.load_table(ident)
            # register the existing parquet files (no rewrite)
            t.add_files(file_paths=files)
            n = t.current_snapshot().summary.get("total-records", "?") if t.current_snapshot() else "?"
            print(f"  {tbl:26s} OK   {len(files):5d} files  rows={n}")
            ok += 1
        except Exception as e:
            print(f"  {tbl:26s} FAIL {str(e)[:120]}")
    print(f"\nregistered {ok}/{len(tables)} tables into {args.catalog} ns={args.namespace}")
    sys.exit(0 if ok == len(tables) else 1)

if __name__ == "__main__":
    main()
