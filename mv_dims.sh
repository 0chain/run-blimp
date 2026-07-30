#!/bin/sh
# mv_dims.sh <mv_table> -> "<rows> <cols>"
#
# The dimensions of an MV that a WARM serve does not report. bench_cdc.sh calls this
# via MV_DIMS_CMD when the query reused an existing MV (the normal production case),
# where the response carries no mv_rows/mv_cols because nothing was authored.
#
# Reads the MV parquet directly, which also proves the file exists — a banked
# catalog row whose parquet has been deleted is a real failure mode (test2
# 2026-07-30: two MVs reported "warm — no rebuild" for directories that were gone).
T="$1"
[ -n "$T" ] || { echo "? ?"; exit 0; }
P="/data/zs3/tpcds_mv/${T}/**/*.parquet"
R=$(docker exec blimp-gw /opt/bin/duckdb -noheader -list -c \
      "SELECT count(*) FROM read_parquet('$P');" 2>/dev/null | tail -1 | tr -d '\r')
C=$(docker exec blimp-gw /opt/bin/duckdb -noheader -list -c \
      "SELECT count(*) FROM (DESCRIBE SELECT * FROM read_parquet('$P'));" 2>/dev/null | tail -1 | tr -d '\r')
echo "${R:-?} ${C:-?}"
