#!/usr/bin/env bash
# test_query.sh — Blimp QUERY/CDC test: the independently-verified CDC suite.
#
# This IS bench_cdc.sh over the delta-merge set (q13 q59 q88 q9 — single-fact
# additive store_sales group-bys, proven to merge at SF1000, ordered by base
# wall time 2.2s → 3.2s → 19.5s → 23.2s; see CDC_INCREMENTAL.md):
#   phase 1  cold-author every query (force_author → graft-widen MV build)
#   phase 2  one live append to store_sales + snapshot_changed
#   phase 3  re-run every query → incremental delta-merge (mode=incremental)
#   summary  the CDC CONTRIBUTIONS table + per-query state (no pass/fail gate)
#
# The TPC-DS query files are generated ON THIS BOX from duckdb's tpcds
# extension into $Q_DIR (once; idempotent) — no shipping step, no drift.
#
# Env (same as bench_cdc): GW CLUSTER_ID ICEBERG_URL WAREHOUSE
#   [NAMESPACE=tpcds] [REGION=ap-south-1] [CDC_ROWS=5000] [QNRS="13 59 88 9"]
set -uo pipefail
: "${GW:?}" "${CLUSTER_ID:?}" "${ICEBERG_URL:?}" "${WAREHOUSE:?}"
HERE="$(cd "$(dirname "$0")" && pwd)"
Q_DIR="${Q_DIR:-$HOME/tpcds_queries}"

# ---- 0. generate the TPC-DS 99 query files (idempotent) ---------------------
if [ ! -f "$Q_DIR/q3.sql" ]; then
  command -v duckdb >/dev/null 2>&1 || {
    curl -fsSL https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip -o /tmp/d.zip \
      && (cd /tmp && unzip -oq d.zip && sudo mv duckdb /usr/local/bin/); }
  mkdir -p "$Q_DIR"
  duckdb -json -c "INSTALL tpcds; LOAD tpcds; SELECT query_nr AS nr, query FROM tpcds_queries();" > /tmp/tpcds_q.json
  python3 - "$Q_DIR" <<'PY'
import json, sys, os
qdir = sys.argv[1]
rows = json.load(open('/tmp/tpcds_q.json'))
for row in rows:
    open(os.path.join(qdir, f"q{row['nr']}.sql"), 'w').write(row['query'].strip().rstrip(';') + '\n')
print(f"generated {len(rows)} query files in {qdir}")
PY
fi
ls "$Q_DIR"/q*.sql >/dev/null 2>&1 || { echo "FATAL: query generation failed ($Q_DIR empty)"; exit 1; }

# ---- 1. the verified CDC suite (author → append → incremental → summary) ----
exec env Q_DIR="$Q_DIR" bash "$HERE/bench_cdc.sh"
