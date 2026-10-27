#!/bin/sh
# vdb_load.sh <env file> <dataset dir> <bucket> [namespace] [table]
#
# Loads a VectorDBBench dataset (train.parquet: id BIGINT, emb FLOAT[];
# test.parquet: the query vectors; neighbors.parquet: each test id's true
# top-100 by the dataset's metric) for the bench: train is written to
# s3://<bucket>/<table>/ (each vector checked to be <dim> long; parquet stores
# it as a list, read back as FLOAT[] — the search casts) and registered as <namespace>.<table>
# (add_files, tagged bench_baseline); test and neighbors go to
# s3://<bucket>/_<table>_test/ and _<table>_neighbors/ for tools/vdb_bench.py.
set -e
ENVF=$1; SRC=$2; BUCKET=$3; NS=${4:-vdb}; TBL=${5:-cohere_1m}
. "$ENVF"
HERE=$(cd "$(dirname "$0")/.." && pwd)
E=$(echo "$S3_ENDPOINT" | sed -E 's#https?://##')
case "$S3_ENDPOINT" in https:*) SSL=true;; *) SSL=false;; esac
DIM=$(duckdb -list -noheader -c "SELECT len(emb) FROM read_parquet('$SRC/train.parquet') LIMIT 1")
duckdb <<SQL
INSTALL httpfs; LOAD httpfs;
CREATE SECRET s (TYPE S3, KEY_ID '$S3_KEY', SECRET '$S3_SECRET', ENDPOINT '$E', URL_STYLE 'path', USE_SSL $SSL, REGION '${REGION:-us-east-1}');
SET threads=8;
COPY (SELECT id, emb::FLOAT[$DIM] AS emb FROM read_parquet('$SRC/train.parquet') ORDER BY id) TO 's3://$BUCKET/$TBL/data.parquet' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000);
COPY (SELECT id, emb::FLOAT[$DIM] AS emb FROM read_parquet('$SRC/test.parquet') ORDER BY id) TO 's3://$BUCKET/_${TBL}_test/data.parquet' (FORMAT parquet);
COPY (SELECT * FROM read_parquet('$SRC/neighbors.parquet') ORDER BY id) TO 's3://$BUCKET/_${TBL}_neighbors/data.parquet' (FORMAT parquet);
SQL
AWS_ACCESS_KEY_ID=$S3_KEY AWS_SECRET_ACCESS_KEY=$S3_SECRET "${BLIMP_PY:-python3}" "$HERE/register_tpcds_tables.py" \
  --catalog "${ICEBERG_URL_LOCAL:-$ICEBERG_URL}" --warehouse "$WAREHOUSE" --source-bucket "$BUCKET" \
  --namespace "$NS" --tables "$TBL" --tag-baseline --s3-endpoint "$S3_ENDPOINT" --region "${REGION:-us-east-1}"
echo "loaded $NS.$TBL dim=$DIM"
