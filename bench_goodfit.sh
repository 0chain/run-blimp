#!/usr/bin/env bash
# bench_goodfit.sh — the good-fit subset of TPC-DS for the MV pipeline, ordered by
# CDC-refresh friendliness. Same driver/env as bench_tpcds99.sh; runs cold + warm
# per query, then ONE CDC commit and re-runs the CDC-refreshable tier to show the
# incremental-refresh time. Drive from the CLIENT box (needs GW/CLUSTER_ID + the
# per-query .sql files in $Q_DIR; the CDC leg needs venv_ib + seed_tpcds.py).
#
# Fit derivation (structural, confirmed empirically — q23 ok / q4 no_mv):
#   T1 additive single-agg (SUM/COUNT/MIN/MAX)  -> graft-widen, CDC-REFRESHABLE
#   T2 UNION-ALL of additive branches           -> multi-union (CTE ones recompute)
#   T3 AVG/COUNT-DISTINCT/STDDEV                 -> MV-able, refresh = full recompute
# Out of scope (not run): window/rank, scalar-subquery matrices (no GROUP BY),
# INTERSECT/EXCEPT, full-outer-of-distinct — 35 queries our additive MVs can't serve.
#
# Env: GW CLUSTER_ID [Q_DIR=~/tpcds_queries] [OUT=./goodfit_results.tsv]
#      [CDC_ROWS=5000] [NAMESPACE=tpcds] [REGION=ap-south-1] [TIMEOUT_S=1800]
#      [ICEBERG_URL WAREHOUSE] (only for the CDC leg)
set -u
: "${GW:?set GW to the gateway host}"; : "${CLUSTER_ID:?set CLUSTER_ID}"
Q_DIR="${Q_DIR:-$HOME/tpcds_queries}"; OUT="${OUT:-./goodfit_results.tsv}"
CDC_ROWS="${CDC_ROWS:-5000}"; NAMESPACE="${NAMESPACE:-tpcds}"; REGION="${REGION:-ap-south-1}"
TIMEOUT_S="${TIMEOUT_S:-1800}"
QAPI="http://$GW:9000"; TOKEN="zus-$CLUSTER_ID"; HERE="$(cd "$(dirname "$0")" && pwd)"

# CDC-friendliness-ordered fit set (52 of 99).
T1="3 10 15 19 21 25 29 34 37 40 42 43 45 46 50 52 55 62 68 69 72 73 79 82 91 93 99"
T2="2 4 5 11 23 33 54 56 60 66 71 74 76 77 80"
T3="6 7 17 18 22 26 35 65 85 27"

J(){ python3 -c "import json,sys;print(json.load(sys.stdin).get('$1',''))" 2>/dev/null; }
now(){ date +%s.%N; }; el(){ awk -v a="$1" -v z="$2" 'BEGIN{printf "%.0f",(z-a)*1000}'; }
run_sql(){ curl -s -m "$TIMEOUT_S" "$QAPI/admin/query/run" -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "$(python3 -c 'import json,sys;print(json.dumps({"original_sql":sys.argv[1],"source":"customer","label":sys.argv[2]}))' "$1" "$2")"; }

[ -d "$Q_DIR" ] || { echo "FATAL: $Q_DIR missing (generate per-query q*.sql via the duckdb tpcds extension)"; exit 1; }
printf 'tier\tqid\tstatus\tauthor_ms\tmaterialize_ms\tcold_ms\twarm_ms\trows\tmv_table\n' > "$OUT"

run_tier(){ local tier="$1"; shift
  for n in "$@"; do
    qid="q$n"; qf="$Q_DIR/$qid.sql"
    [ -f "$qf" ] || { echo "  $tier $qid: NO SQL in $Q_DIR"; continue; }
    SQL=$(cat "$qf")
    R=$(run_sql "$SQL" "$qid-gf")
    ST=$(echo "$R" | J status); [ -z "$ST" ] && ST="timeout/err"
    AU=$(echo "$R" | J author_ms); MA=$(echo "$R" | J materialize_ms)
    QM=$(echo "$R" | J query_ms); RW=$(echo "$R" | J rows); MV=$(echo "$R" | J mv_table)
    WQ="-"
    if [ "$ST" = "ok" ]; then R2=$(run_sql "$SQL" "$qid-warm"); WQ=$(echo "$R2" | J query_ms); fi
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
      "$tier" "$qid" "$ST" "${AU:-0}" "${MA:-0}" "${QM:-0}" "${WQ:-0}" "${RW:-0}" "${MV:--}" >> "$OUT"
    echo "  $tier $qid: $ST author=${AU:-0}ms mat=${MA:-0}ms cold=${QM:-0}ms warm=${WQ:-—}ms rows=${RW:-0}"
  done
}

echo "== good-fit suite (52 q, CDC-ordered) cluster=$CLUSTER_ID gw=$GW =="
run_tier T1 $T1
run_tier T2 $T2
run_tier T3 $T3

# ---- CDC leg: one commit, then re-run T1 (the CDC-refreshable tier) ----------------
if [ -n "${ICEBERG_URL:-}" ] && [ -n "${WAREHOUSE:-}" ] && [ -f "$HERE/seed_tpcds.py" ]; then
  echo
  echo "== CDC: +$CDC_ROWS rows to store_returns, snapshot_changed, re-run T1 (incremental) =="
  PY3="$HOME/venv_ib/bin/python3"; [ -x "$PY3" ] || PY3=python3
  "$PY3" "$HERE/seed_tpcds.py" --catalog "$ICEBERG_URL" --warehouse "$WAREHOUSE" \
    --namespace "$NAMESPACE" --table store_returns --rows "$CDC_ROWS" --s3-region "$REGION" 2>&1 | tail -1
  curl -s -m 60 "$QAPI/admin/source/snapshot_changed" -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"namespace\":\"$NAMESPACE\",\"table\":\"store_returns\",\"trigger\":\"goodfit-cdc\"}" >/dev/null
  for n in $T1; do
    qid="q$n"; qf="$Q_DIR/$qid.sql"; [ -f "$qf" ] || continue
    R=$(run_sql "$(cat "$qf")" "$qid-cdc")
    Q3=$(echo "$R" | J query_ms); M3=$(echo "$R" | J materialize_ms); E3=$(echo "$R" | J engine)
    echo "  cdc $qid: query=${Q3:-?}ms incremental_materialize=${M3:-0}ms engine=${E3:--}"
  done
else
  echo "(CDC leg skipped — set ICEBERG_URL + WAREHOUSE and provide seed_tpcds.py to measure incremental refresh)"
fi
echo "DONE — results in $OUT"
