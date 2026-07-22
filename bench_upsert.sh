#!/usr/bin/env bash
# bench_upsert.sh — upsert → FULL re-materialize timing for the MV lifecycle.
# Split out of bench_incremental.sh (which now only measures append→incremental).
# Each cycle rewrites rows in store_returns (removed_files + added_files), notifies
# the cluster, and re-serves q1 inline; because the source was REWRITTEN (not just
# appended), the freshness gate must full-rebuild, so materialize_ms is the
# full-recompute cost. Prints min/median/avg/max.
#
# NOTE upsert full-rebuild CORRECTNESS needs the removed_files-aware cdc
# (zs3-init 2026-07-17); on an older cdc the upsert refresh may degrade to an
# added-only merge — timings then measure the wrong (cheaper) path.
#
# Env: GW CLUSTER_ID ICEBERG_URL WAREHOUSE
#   [NAMESPACE=tpcds] [REGION=ap-south-1] [ITERS=5] [CDC_ROWS=50000]
set -u
NAMESPACE="${NAMESPACE:-tpcds}"; REGION="${REGION:-ap-south-1}"
ITERS="${ITERS:-5}"; CDC_ROWS="${CDC_ROWS:-50000}"
QAPI="http://$GW:9000"; TOKEN="zus-$CLUSTER_ID"; HERE="$(cd "$(dirname "$0")" && pwd)"
PY3="$HOME/venv_ib/bin/python3"; [ -x "$PY3" ] || PY3=python3
now(){ date +%s.%N; }; el(){ awk -v a="$1" -v z="$2" 'BEGIN{printf "%.1f",(z-a)*1000}'; }
J(){ python3 -c "import json,sys;print(json.load(sys.stdin).get('$1',''))" 2>/dev/null; }

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

echo "== upsert → full re-materialize benchmark: cluster $CLUSTER_ID (upserts=$ITERS, rows/cycle=$CDC_ROWS) =="
# prime q1 so the first upsert measures a refresh, not a cold author
run_sql "$Q1" >/dev/null

C_MAT=(); C_WALL=()
for i in $(seq 1 "$ITERS"); do
  cycle "upsert-$i" --mode upsert --upsert-store-sk $((20+i))
  C_MAT+=("${CY_MAT:-0}"); C_WALL+=("$CY_WALL")
  echo "  upsert[$i] refresh materialize_ms=${CY_MAT:-?} query_ms=${CY_Q:-?} engine=$CY_ENGINE wall_ms=$CY_WALL"
done

echo
echo "== SUMMARY (q1-scale MV, upsert/full-rebuild over ${CDC_ROWS}-row commits) =="
echo "  upsert  refresh materialize_ms:       $(stats "${C_MAT[@]:-}")"
echo "  upsert  commit→answer wall_ms:        $(stats "${C_WALL[@]:-}")"
echo "  (valid FULL-rebuild timings only on a removed_files-aware cdc)"
