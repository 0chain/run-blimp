#!/bin/sh
# clickbench_load.sh <env file> <download dir> <bucket> [namespace] [base files]
#
# Loads the standard ClickBench dataset (100 hits_N.parquet files, 100M rows)
# for the bench: each file is converted to the hits table's types — strings as
# VARCHAR (the files store BINARY), EventDate as DATE (stored as days since
# 1970), the one unsigned column widened (Iceberg has no unsigned types) — and
# written to s3://<bucket>/hits/ (the first [base files], default 90: the table)
# or s3://<bucket>/hits_holdout/ (the rest: what seed_holdout.py appends one
# slice per tick). The table is then registered from hits/ with
# register_tpcds_tables.py (add_files, no copy) and tagged bench_baseline.
set -e
ENVF=$1; SRC=$2; BUCKET=$3; NS=${4:-clickbench}; BASE=${5:-90}
. "$ENVF"
HERE=$(cd "$(dirname "$0")/.." && pwd)
E=$(echo "$S3_ENDPOINT" | sed -E 's#https?://##')
case "$S3_ENDPOINT" in https:*) SSL=true;; *) SSL=false;; esac
for i in $(seq 0 99); do
  f="$SRC/hits_$i.parquet"
  [ -f "$f" ] || { echo "missing $f"; exit 1; }
  dest=hits; [ "$i" -ge "$BASE" ] && dest=hits_holdout
  cols=$(duckdb -list -noheader -c "SELECT string_agg(CASE WHEN column_name='EventDate' THEN 'DATE ''1970-01-01'' + CAST(EventDate AS INTEGER) AS EventDate' WHEN column_type LIKE 'U%' THEN 'CAST(\"' || column_name || '\" AS BIGINT) AS \"' || column_name || '\"' ELSE '\"' || column_name || '\"' END, ', ') FROM (DESCRIBE SELECT * FROM read_parquet('$f', binary_as_string=true))")
  duckdb -c "INSTALL httpfs; LOAD httpfs; CREATE SECRET s (TYPE S3, KEY_ID '$S3_KEY', SECRET '$S3_SECRET', ENDPOINT '$E', URL_STYLE 'path', USE_SSL $SSL, REGION '${REGION:-us-east-1}'); SET threads=4; COPY (SELECT $cols FROM read_parquet('$f', binary_as_string=true)) TO 's3://$BUCKET/$dest/hits_$i.parquet' (FORMAT parquet, COMPRESSION zstd);"
  echo "hits_$i -> $dest"
done
AWS_ACCESS_KEY_ID=$S3_KEY AWS_SECRET_ACCESS_KEY=$S3_SECRET "${BLIMP_PY:-python3}" "$HERE/register_tpcds_tables.py" \
  --catalog "${ICEBERG_URL_LOCAL:-$ICEBERG_URL}" --warehouse "$WAREHOUSE" --source-bucket "$BUCKET" \
  --namespace "$NS" --tables hits --tag-baseline --s3-endpoint "$S3_ENDPOINT" --region "${REGION:-us-east-1}"
