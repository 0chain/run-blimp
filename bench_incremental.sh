#!/usr/bin/env bash
# bench_incremental.sh — statistical timing profile for the MV lifecycle:
#   A) one-time author+shape+materialize × AUTHOR_ITERS — STRUCTURALLY distinct
#      TPC-DS queries not yet on this cluster (constant-varied clones of an
#      existing query scheme-match to the same MV in ~50 ms and never author).
#   B) append → incremental refresh × ITERS
# Measured INLINE: commit + snapshot_changed, then POST query/run — the serve-time
# freshness gate re-materializes and reports materialize_ms + query_ms; the wave
# mode for the same window is recorded as a label. Prints min/median/avg/max.
# (Upsert → full re-materialize lives in the separate bench_upsert.sh.)
#
# Env (test_query.sh's): GW GW_AK GW_SK CLUSTER_ID ICEBERG_URL WAREHOUSE
#   [NAMESPACE=tpcds] [REGION=ap-south-1] [ITERS=5] [AUTHOR_ITERS=5]
#   [CDC_ROWS=50000] [Q_DIR=~/tpcds_queries]
set -u
NAMESPACE="${NAMESPACE:-tpcds}"; REGION="${REGION:-ap-south-1}"
ITERS="${ITERS:-5}"; AUTHOR_ITERS="${AUTHOR_ITERS:-5}"; CDC_ROWS="${CDC_ROWS:-50000}"
Q_DIR="${Q_DIR:-$HOME/tpcds_queries}"
QAPI="http://$GW:9000"; TOKEN="zus-$CLUSTER_ID"; HERE="$(cd "$(dirname "$0")" && pwd)"
PY3="$HOME/venv_ib/bin/python3"; [ -x "$PY3" ] || PY3=python3
now(){ date +%s.%N; }; el(){ awk -v a="$1" -v z="$2" 'BEGIN{printf "%.1f",(z-a)*1000}'; }
J(){ python3 -c "import json,sys;print(json.load(sys.stdin).get('$1',''))" 2>/dev/null; }

# TPC-DS q1. The optimizer's MV is the mergeable INNER aggregate
# (customer_total_return: sum(sr_return_amt) by customer/store over the appended
# store_returns) — a single-fact SUM group-by, so it is per-column decomposable
# and the freshness gate does an INCREMENTAL delta-merge (verified: DELTA-MERGED
# on the cluster). NOTE the merge only works for a SINGLE-fact delta; multi-fact
# queries (q25/q29: store_sales+store_returns+catalog_sales) can't merge a
# one-table delta and fall back to full re-author.
Q1='with customer_total_return as
 (select sr_customer_sk as ctr_customer_sk, sr_store_sk as ctr_store_sk,
         sum(sr_return_amt) as ctr_total_return
  from store_returns, date_dim
  where sr_returned_date_sk = d_date_sk and d_year = 2000
  group by sr_customer_sk, sr_store_sk)
select c_customer_id
from customer_total_return ctr1, store, customer
where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
        from customer_total_return ctr2 where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
  and s_store_sk = ctr1.ctr_store_sk and s_state = '\''TN'\''
  and ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id limit 100'

run_sql(){ curl -s -m 900 "$QAPI/admin/query/run" -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "$(python3 -c 'import json,sys;print(json.dumps({"original_sql":sys.argv[1],"source":"customer"}))' "$1")"; }

stats(){ python3 -c "
import sys,statistics as st
v=[float(x) for x in sys.argv[1:] if x]
v=[x for x in v if x>0]
if not v: print('n=0'); raise SystemExit
print('n=%d min=%.0f median=%.0f avg=%.0f max=%.0f (ms)'%(len(v),min(v),st.median(v),st.mean(v),max(v)))" "$@"; }

registered_sigs(){ curl -s -m 20 "$QAPI/admin/mv/registered_queries" -H "Authorization: Bearer $TOKEN" \
  | python3 -c "import json,sys;print(' '.join(q['sig'] for q in json.load(sys.stdin).get('queries',[])))" 2>/dev/null; }

echo "== MV lifecycle benchmark v2: cluster $CLUSTER_ID (authors=$AUTHOR_ITERS, appends=$ITERS, upserts=$ITERS, rows/cycle=$CDC_ROWS) =="

# ---- A) one-time author+shape+materialize on structurally distinct queries --------
# Pick TPC-DS queries not yet registered here; a fresh author reports
# author_ms>0 (fastpath reuse reports none and is EXCLUDED, printed as 'reused').
A_AUTH=(); A_MAT=(); A_WALL=(); A_DONE=0
if [ -d "$Q_DIR" ]; then
  for qf in $(ls "$Q_DIR"/q*.sql 2>/dev/null | sort -V); do
    [ "$A_DONE" -ge "$AUTHOR_ITERS" ] && break
    SQL=$(cat "$qf")
    T0=$(now); R=$(run_sql "$SQL"); T1=$(now)
    ST=$(echo "$R" | J status); AU=$(echo "$R" | J author_ms); MA=$(echo "$R" | J materialize_ms)
    W=$(el "$T0" "$T1")
    if [ -n "$AU" ] && [ "${AU%.*}" -gt 0 ] 2>/dev/null; then
      A_AUTH+=("$AU"); A_MAT+=("${MA:-0}"); A_WALL+=("$W"); A_DONE=$((A_DONE+1))
      echo "  author[$A_DONE] $(basename "$qf") status=$ST author_ms=$AU materialize_ms=${MA:-?} wall_ms=$W"
    else
      echo "  skip $(basename "$qf") status=$ST (reused/no-author, wall_ms=$W)"
    fi
  done
else
  echo "  WARN: $Q_DIR missing — generate with the duckdb tpcds extension; skipping author class"
fi

cycle(){ # cycle <label> <seed-extra-args...> — commit, webhook, inline re-serve
  local label="$1"; shift
  "$PY3" "$HERE/seed_tpcds.py" --catalog "$ICEBERG_URL" --warehouse "$WAREHOUSE" \
    --namespace "$NAMESPACE" --table store_returns --rows "$CDC_ROWS" \
    --s3-region "$REGION" "$@" >/dev/null 2>&1
  curl -s -m 60 "$QAPI/admin/source/snapshot_changed" -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"namespace\":\"$NAMESPACE\",\"table\":\"store_returns\",\"trigger\":\"bench-$label\"}" >/dev/null
  local T0 R T1
  T0=$(now); R=$(run_sql "$Q1"); T1=$(now)
  CY_MAT=$(echo "$R" | J materialize_ms); CY_Q=$(echo "$R" | J query_ms)
  CY_ENGINE=$(echo "$R" | J engine); CY_WALL=$(el "$T0" "$T1")
}

# ---- B) append → incremental refresh ---------------------------------------------
B_MAT=(); B_WALL=()
for i in $(seq 1 "$ITERS"); do
  cycle "append-$i"
  B_MAT+=("${CY_MAT:-0}"); B_WALL+=("$CY_WALL")
  echo "  append[$i] refresh materialize_ms=${CY_MAT:-?} query_ms=${CY_Q:-?} engine=$CY_ENGINE wall_ms=$CY_WALL"
done

echo
echo "== SUMMARY (q1-scale MV over ${CDC_ROWS}-row commits) =="
echo "  one-time author_ms (fresh queries):   $(stats "${A_AUTH[@]:-}")"
echo "  one-time materialize_ms:              $(stats "${A_MAT[@]:-}")"
echo "  one-time wall_ms:                     $(stats "${A_WALL[@]:-}")"
echo "  append  refresh materialize_ms:       $(stats "${B_MAT[@]:-}")"
echo "  append  commit→answer wall_ms:        $(stats "${B_WALL[@]:-}")"
