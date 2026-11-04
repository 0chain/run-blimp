#!/usr/bin/env python3
# register_tpcds_tables.py — register ALL 24 TPC-DS tables into the kit's Iceberg REST
# catalog from EXISTING parquet in an S3 bucket (e.g. my-tpcds-bucket).
#
# No data copy: pyiceberg `add_files` registers the existing parquet files into a new
# Iceberg table (schema inferred from the parquet). Blimp (source=customer) then pulls
# these for MV generation; CDC appends land in the catalog's own warehouse.
#
# Usage:
#   pip install "pyiceberg[s3fs]>=0.10" "pyarrow>=14,<17" s3fs boto3
#   (>=0.9 required for parquet that stores small decimals as INT32/INT64,
#    which pyiceberg 0.7/0.8 add_files rejects)
#   python3 register_tpcds_tables.py \
#     --catalog http://<client-priv-ip>:8181 \
#     --warehouse s3://<your-warehouse-bucket>/wh \
#     --source-bucket my-tpcds-bucket --region us-east-1 \
#     --namespace tpcds
#
# The 24 TPC-DS tables live at s3://<source-bucket>/<table>/**/*.parquet.
import argparse, contextlib, io, sys
import boto3
import pyarrow.parquet as pq
import s3fs
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError
from seed_tpcds import load_rest_catalog  # the one client-side catalog loader


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

BASELINE_TAG_AGE_MS = 100 * 365 * 24 * 3600 * 1000  # rollback.py's TAG_AGE_MS


def _mask(k):
    """Last 4 characters only for a key long enough that 4 reveal little."""
    return "…" + k[-4:] if k and len(k) >= 16 else "***"


def print_s3_identity(endpoint, key, secret, region):
    """Name the credentials in use before the first S3 call (blimp --register
    once signed AWS requests with the node's MinIO keys and said nothing)."""
    aws = not endpoint or endpoint.rstrip("/").endswith(".amazonaws.com")
    if key and not aws:
        print(f"  S3 identity: access key {_mask(key)} at {endpoint}")
        return
    if not aws:
        print(f"  S3 identity: no keys (default credential chain) at {endpoint}")
        return
    if not key:
        try:
            c = boto3.Session().get_credentials()
            chain = f"{c.method} key {_mask(c.access_key)}" if c else "no credentials found"
        except Exception:
            chain = "no credentials found"
    try:
        kw = {"region_name": region}
        if key:
            kw.update(aws_access_key_id=key, aws_secret_access_key=secret)
        who = boto3.client("sts", **kw).get_caller_identity()
        src = f"access key {_mask(key)}" if key else f"the AWS default credential chain ({chain})"
        print(f"  S3 identity: AWS {who['Arn']} (account {who['Account']}) via {src}")
    except Exception as e:
        print(f"  S3 identity: AWS, {'access key ' + _mask(key) if key else 'default credential chain: ' + chain}"
              f" — could not be resolved ({str(e)[:160]})")


def nullability_gaps(t, fs, files):
    """{column: file} for every top-level column the catalog table REQUIRES while
    some parquet file declares it OPTIONAL — the one mismatch Iceberg resolves by
    itself (an optional field accepts required and optional files alike). Read
    only after add_files refused the files, so the happy path reads one footer."""
    req = {f.name for f in t.schema().fields if f.required}
    gaps = {}
    for f in files:
        with fs.open(f.replace("s3://", ""), "rb") as fh:
            for fld in pq.read_schema(fh):
                if fld.nullable and fld.name in req and fld.name not in gaps:
                    gaps[fld.name] = f
    return gaps


def schema_mismatch(t, fs, files):
    """One line naming the first column whose parquet type/presence differs from
    the table, in place of pyiceberg's whole-schema table."""
    tf = {f.name: f for f in t.schema().fields}
    for f in files:
        with fs.open(f.replace("s3://", ""), "rb") as fh:
            sch = pq.read_schema(fh)
        for fld in sch:
            if fld.name not in tf:
                return f"column {fld.name} is in {f} but not in the table"
        missing = [n for n, x in tf.items() if x.required and n not in sch.names]
        if missing:
            return f"required column {missing[0]} is missing from {f}"
    return ""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", required=True, help="Iceberg REST catalog URL")
    ap.add_argument("--warehouse", required=True, help="s3://bucket/prefix for catalog metadata + CDC appends")
    ap.add_argument("--source-bucket", required=True, help="S3 bucket holding the parquet (bucket[/prefix] accepted)")
    ap.add_argument("--source-prefix", default="", help="prefix inside the bucket that holds <table>/ folders (may be nested: runs/<id>/<dataset>)")
    # Iceberg has optional fields natively; a parquet column declared optional
    # (pyarrow's and pandas' default) is registered as optional. --strict-nullability
    # refuses instead, naming the column.
    ap.add_argument("--strict-nullability", action="store_true",
                    help="refuse a table whose parquet declares optional a column the table requires (default: make it optional)")
    ap.add_argument("--region", default="ap-south-1")
    ap.add_argument("--namespace", default="tpcds")
    ap.add_argument("--tables", default="", help="comma-separated tables (default: the 24 TPC-DS tables; 'auto' = every top-level prefix holding parquet)")
    ap.add_argument("--s3-endpoint", default="", help="custom S3 endpoint (MinIO etc.); blank = AWS")
    # Nessie serves Iceberg REST under a BRANCH prefix (default "main"); the
    # tabulario/iceberg-rest catalog serves the root and takes no prefix. Passing
    # one where it is not expected 404s every call, so this stays opt-in.
    # THE BASELINE IS SET HERE, ONCE, AT THE BASE LOAD. The node rollback
    # inferred it ("the latest of every table's oldest surviving ancestor")
    # once the true base snapshots had expired, so 144/37's "SF10" rolled back
    # to store 1,543-1,622 rows (102), store_sales 44.0M (28.8M) — every run
    # started bigger than the last and MVs grew run over run (v_q59_agg's
    # chart 13,624 -> 464,462 rows; 2026-10-25).
    ap.add_argument("--tag-baseline", action="store_true",
                    help="tag each table's base snapshot 'bench_baseline' (the rollback target) when it has none")
    ap.add_argument("--prefix", default="", help="Iceberg REST prefix (Nessie branch, e.g. main); blank = none")
    # Explicit keys for an S3 that is not reachable by this box's IAM role — the
    # fleet cache layer is key-authenticated, and pyiceberg/s3fs will otherwise
    # sign anonymously and 403.
    ap.add_argument("--s3-key", default="", help="S3 access key (blank = IAM role / env)")
    ap.add_argument("--s3-secret", default="", help="S3 secret key")
    args = ap.parse_args()

    # bucket[/prefix], parsed ONCE: the bucket for bucket calls (region, the
    # warehouse comparison), bucket/prefix for listing. A nested value used to be
    # taken whole as a bucket name by some calls and as a prefix by others.
    bkt, _, pfx0 = args.source_bucket.strip("/").partition("/")
    pfx = "/".join(x for x in (pfx0.strip("/"), args.source_prefix.strip("/")) if x)
    base = f"{bkt}/{pfx}" if pfx else bkt
    where = f"bucket '{bkt}', prefix '{pfx or '(none)'}'"

    auto = args.tables.strip() == "auto"
    tables = [] if auto else ([t.strip() for t in args.tables.split(",") if t.strip()] or TPCDS_TABLES)

    # Fool-proofing for AWS S3 (skipped for MinIO / custom --s3-endpoint):
    # 1. Trust the SOURCE bucket's ACTUAL region over --region (customers get it
    #    wrong; a mismatch otherwise surfaces as a cryptic HTTP 301).
    # 2. HARD-FAIL if the warehouse bucket is in a different region than the source
    #    — a single S3 endpoint can't serve two regions, so pyiceberg's warehouse
    #    write would 301. Same-region is a hard requirement, so say so plainly.
    if not args.s3_endpoint and not args.s3_key:
        src_region = bucket_region(bkt)
        if src_region and src_region != args.region:
            print(f"  NOTE: source bucket {bkt} is in {src_region}, "
                  f"not --region {args.region} — using {src_region}")
            args.region = src_region
        wh_bkt = args.warehouse.split("/")[2] if args.warehouse.startswith("s3://") else ""
        wh_region = bucket_region(wh_bkt) if wh_bkt else None
        if wh_region and src_region and wh_region != src_region:
            sys.exit(
                f"FATAL: warehouse bucket '{wh_bkt}' is in {wh_region} but source "
                f"bucket '{bkt}' is in {src_region}. The Iceberg warehouse "
                f"MUST be in the same region as your data — use e.g. "
                f"s3://{bkt}/wh")

    # DEFAULT to the region-specific S3 endpoint. Without it, s3fs and pyiceberg
    # fall back to the us-east-1 endpoint (s3.amazonaws.com), and any bucket in
    # another region returns HTTP 301. Customer only sets --s3-endpoint for MinIO.
    s3_endpoint = args.s3_endpoint or f"https://s3.{args.region}.amazonaws.com"
    print(f"  s3 endpoint: {s3_endpoint}  region: {args.region}  {where}")
    print_s3_identity(args.s3_endpoint, args.s3_key, args.s3_secret, args.region)
    cat = load_rest_catalog("zus", args.catalog, args.warehouse, s3_endpoint=s3_endpoint,
                            region=args.region, key=args.s3_key, secret=args.s3_secret,
                            prefix=args.prefix)

    fs_kw = {"client_kwargs": {"region_name": args.region, "endpoint_url": s3_endpoint}}
    if args.s3_key:
        fs_kw["key"] = args.s3_key
        fs_kw["secret"] = args.s3_secret
    fs = s3fs.S3FileSystem(**fs_kw)
    if auto:
        # every top-level prefix of the bucket that holds parquet is a table;
        # the warehouse prefix (catalog metadata) is not
        wh_dir = args.warehouse[len(f"s3://{base}/"):].split("/")[0] if args.warehouse.startswith(f"s3://{base}/") else ""
        try:
            listing = fs.ls(base, detail=True)
        except Exception as e:
            sys.exit(f"FATAL: cannot list s3://{base}/ ({where}, endpoint {s3_endpoint}): {str(e)[:200]}")
        for d in listing:
            name = d["name"].rstrip("/").split("/")[-1]
            if d.get("type") != "directory" or name == wh_dir or name.startswith((".", "_")):
                continue
            found = [p for p in fs.find(d["name"], maxdepth=None) if p.endswith(".parquet")]
            if found:
                tables.append(name)
                # parquet only in sub-folders that are not partitions (k=v):
                # the folder is probably one more prefix level, not a table
                subs = sorted({p[len(d["name"].rstrip("/")) + 1:].split("/")[0] for p in found if p.count("/") > d["name"].rstrip("/").count("/") + 1})
                if subs and all("=" not in x for x in subs) and not any(p.count("/") == d["name"].rstrip("/").count("/") + 1 for p in found):
                    print(f"  NOTE: {name}/ holds parquet only in sub-folders ({', '.join(subs[:5])}{', …' if len(subs) > 5 else ''})"
                          f" — if those are the tables, pass --bucket {base}/{name}")
        if not tables:
            sys.exit(f"no table prefixes with parquet under s3://{base}/ ({where})")
        print(f"  tables found: {', '.join(tables)}")
    else:
        # --tables given: still prove S3 access before touching the catalog.
        try:
            fs.ls(base)
        except Exception as e:
            sys.exit(f"FATAL: cannot list s3://{base}/ ({where}, endpoint {s3_endpoint}): {str(e)[:200]}")
    # Only once the data is listed: a bad key or an empty prefix must leave no
    # namespace behind in the catalog.
    try:
        cat.create_namespace(args.namespace)
    except NamespaceAlreadyExistsError:
        pass
    ok = 0
    for tbl in tables:
        prefix = f"{base}/{tbl}/"
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
                # IDEMPOTENT PER TABLE: a table that already has a snapshot holds
                # its data — adding the files again would double every row. One
                # without a snapshot is a registration that failed after
                # create_table (Olyad's tpcds_clean: 24 tables at their 00000
                # metadata, current-snapshot-id -1, 2026-10-24): finish it.
                if t.current_snapshot() is not None:
                    n = t.current_snapshot().summary.get("total-records", "?")
                    print(f"  {tbl:26s} OK   already registered  rows={n}")
                    ok += 1
                    continue
            # register the existing parquet files (no rewrite)
            try:
                # pyiceberg prints its whole-schema compatibility table to stdout
                # before raising; the one-line message below replaces it
                with contextlib.redirect_stdout(io.StringIO()):
                    t.add_files(file_paths=files)
            except ValueError as e:
                if "Mismatch in fields" not in str(e):
                    raise
                gaps = nullability_gaps(t, fs, files)
                if not gaps:
                    raise RuntimeError(f"schema mismatch: {schema_mismatch(t, fs, files) or str(e).splitlines()[0]}") from None
                col, f0 = next(iter(gaps.items()))
                if args.strict_nullability:
                    raise RuntimeError(f"{tbl}.{col} is optional in parquet ({f0}) and required by the catalog table"
                                       f"{f' (+{len(gaps) - 1} more: ' + ', '.join(list(gaps)[1:]) + ')' if len(gaps) > 1 else ''}"
                                       " — rewrite it non-nullable, or register without --strict-nullability") from None
                # the files disagree on nullability (or the table came from a
                # required-only first file): the column is optional
                with t.update_schema() as u:
                    for c in gaps:
                        u.make_column_optional(c)
                print(f"  {tbl:26s} NOTE optional in parquet, made optional in the table: {', '.join(gaps)} (e.g. {f0})")
                t = cat.load_table(ident)
                t.add_files(file_paths=files)
            if t.current_snapshot() is None:
                raise RuntimeError("add_files committed no snapshot — the catalog table is still empty")
            n = t.current_snapshot().summary.get("total-records", "?") if t.current_snapshot() else "?"
            tagged = ""
            if args.tag_baseline and "bench_baseline" not in t.metadata.refs:
                # only the snapshot add_files just committed — never a later
                # state of an already-registered table. A max ref age of its
                # own (rollback.py's 100 years): without one, iceberg-go's
                # expiry drops a tag older than its cutoff with the snapshots.
                t.manage_snapshots().create_tag(snapshot_id=t.current_snapshot().snapshot_id, tag_name="bench_baseline",
                                                max_ref_age_ms=BASELINE_TAG_AGE_MS).commit()
                tagged = "  tagged bench_baseline"
            print(f"  {tbl:26s} OK   {len(files):5d} files  rows={n}{tagged}")
            ok += 1
        except Exception as e:
            print(f"  {tbl:26s} FAIL {str(e).splitlines()[0][:300] if str(e) else type(e).__name__} (s3://{prefix})")
    print(f"\nregistered {ok}/{len(tables)} tables into {args.catalog} ns={args.namespace}")
    sys.exit(0 if ok == len(tables) else 1)

if __name__ == "__main__":
    main()
