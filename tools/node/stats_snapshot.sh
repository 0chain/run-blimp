#!/bin/bash
# stats_snapshot.sh <envfile> <bucket> <out.json>: exact rows + approx NDV per column of the base files
set -a; . "$1"; set +a
B=$2; OUT=$3
EP=${S3_ENDPOINT#http://}; EP=${EP#https://}
PRE="INSTALL httpfs; LOAD httpfs; CREATE SECRET s (TYPE S3, KEY_ID '$S3_KEY', SECRET '$S3_SECRET', ENDPOINT '$EP', URL_STYLE 'path', USE_SSL false, REGION '${REGION:-us-east-1}');"
echo "{" > $OUT; first=1
for t in call_center catalog_page catalog_returns catalog_sales customer customer_address customer_demographics date_dim household_demographics income_band inventory item promotion reason ship_mode store store_returns store_sales time_dim warehouse web_page web_returns web_sales web_site; do
  r=$(duckdb -json -c "$PRE SELECT count(*) AS __rows, approx_count_distinct(COLUMNS(*)) FROM read_parquet('s3://$B/$t/*.parquet');" 2>/dev/null | tr -d '\n' | sed 's/^\[//; s/\]$//')
  [ -z "$r" ] && { echo "FAIL $t" >&2; continue; }
  [ $first = 1 ] || echo "," >> $OUT; first=0
  echo "\"$t\": $r" >> $OUT
done
echo "}" >> $OUT
