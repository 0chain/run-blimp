#!/usr/bin/env bash
# bench_incremental.sh — statistical timing profile for the MV lifecycle:
#   A) COLD author+shape+materialize of q1 × AUTHOR_ITERS — same query each time,
#      but the MV is EVICTED (deleted) after every build so the next iteration
#      re-authors from scratch (force_author). A repeatable cold-build measurement.
#   B) append → incremental refresh × ITERS
# Measured INLINE: commit + snapshot_changed, then POST query/run — the serve-time
# freshness gate re-materializes and reports materialize_ms + query_ms; the wave
# mode for the same window is recorded as a label. Prints min/median/avg/max.
# (Upsert → full re-materialize lives in the separate bench_upsert.sh.)
#
# Env (test_query.sh's): GW GW_AK GW_SK CLUSTER_ID ICEBERG_URL WAREHOUSE
#   [NAMESPACE=tpcds] [REGION=ap-south-1] [ITERS=3] [AUTHOR_ITERS=3]
#   [CDC_ROWS=50000] [Q_DIR=~/tpcds_queries]
set -u
NAMESPACE="${NAMESPACE:-tpcds}"; REGION="${REGION:-ap-south-1}"
ITERS="${ITERS:-3}"; AUTHOR_ITERS="${AUTHOR_ITERS:-3}"; CDC_ROWS="${CDC_ROWS:-50000}"
Q_DIR="${Q_DIR:-$HOME/tpcds_queries}"
# QAPI/TOKEN are overridable, like bench_cdc.sh. Hardcoding them sent every
# request to the CLUSTER gateway with a cluster token, so against a manual stack
# (test2: localhost:9100 + a harness token) all three refresh cycles returned
# delta_merge_ms=? in 33ms — the bench looked like it ran and measured nothing.
QAPI="${QAPI:-http://$GW:9000}"; TOKEN="${TOKEN:-zus-$CLUSTER_ID}"; HERE="$(cd "$(dirname "$0")" && pwd)"
PY3="${BLIMP_PY:-$HOME/.blimp_venv/bin/python3}"; [ -x "$PY3" ] || PY3="$HOME/venv_ib/bin/python3"; [ -x "$PY3" ] || PY3=python3
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

# BENCH_QNR: bench a different TPC-DS query (reads $Q_DIR/q<N>.sql, e.g.
# BENCH_QNR=64). Default stays the built-in q1. Labels below follow suit.
BENCH_QNR="${BENCH_QNR:-1}"
if [ "$BENCH_QNR" != "1" ]; then
  QF="$Q_DIR/q${BENCH_QNR}.sql"
  [ -f "$QF" ] || { echo "FATAL: BENCH_QNR=$BENCH_QNR but no $QF"; exit 1; }
  Q1="$(cat "$QF")"
fi
QLABEL="q${BENCH_QNR}"

# force_author is GONE from the product (a client flag that faked a cold state
# instead of creating one). A cold author is produced the honest way — evict the
# MV, which evict_mv already does between iterations — so the request carries the
# query and nothing else.
run_sql(){ curl -s -m 900 "$QAPI/admin/query/run" -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "$(python3 -c 'import json,sys;print(json.dumps({"original_sql":sys.argv[1],"source":"customer"}))' "$1")"; }

# evict an MV so the NEXT identical query re-authors from scratch (clears the
# sigcache row + warehouse bytes — copied from run.sh's \evict). The author-timing
# loop calls this after each build so every iteration is a genuine COLD author.
evict_mv(){ # <namespace> <table>
  [ -n "${2:-}" ] || return 0
  curl -s -m 30 -X DELETE "$QAPI/admin/mv/delete?namespace=${1:-tpcds_mv}&table=$2" \
    -H "Authorization: Bearer $TOKEN" >/dev/null 2>&1; }

stats(){ python3 -c "
import sys,statistics as st
v=[float(x) for x in sys.argv[1:] if x]
v=[x for x in v if x>0]
if not v: print('n=0'); raise SystemExit
print('n=%d min=%.0f median=%.0f avg=%.0f max=%.0f (ms)'%(len(v),min(v),st.median(v),st.mean(v),max(v)))" "$@"; }

registered_sigs(){ curl -s -m 20 "$QAPI/admin/mv/registered_queries" -H "Authorization: Bearer $TOKEN" \
  | python3 -c "import json,sys;print(' '.join(q['sig'] for q in json.load(sys.stdin).get('queries',[])))" 2>/dev/null; }

echo "== MV lifecycle benchmark v2: cluster $CLUSTER_ID (authors=$AUTHOR_ITERS, appends=$ITERS, upserts=$ITERS, rows/cycle=$CDC_ROWS) =="

# ---- A) COLD author of q1, AUTHOR_ITERS times, EVICTING after each ----------------
# Same query (q1) every iteration, but the MV is DELETED right after it's built, so
# the next iteration re-authors from scratch — a repeatable COLD author+materialize
# measurement (not a warm fastpath reuse, which reports no author_ms). force_author
# forces the deterministic build even before the evict has propagated.
A_AUTH=(); A_MAT=(); A_WALL=(); A_DONE=0
for i in $(seq 1 "$AUTHOR_ITERS"); do
  T0=$(now); R=$(run_sql "$Q1" 1); T1=$(now)            # force_author=1
  ST=$(echo "$R" | J status); AU=$(echo "$R" | J author_ms); MA=$(echo "$R" | J materialize_ms)
  MVNS=$(echo "$R" | J mv_namespace); MVTB=$(echo "$R" | J mv_table); W=$(el "$T0" "$T1")
  if [ -n "$AU" ] && [ "${AU%.*}" -gt 0 ] 2>/dev/null; then
    A_AUTH+=("$AU"); A_MAT+=("${MA:-0}"); A_WALL+=("$W"); A_DONE=$((A_DONE+1))
    echo "  author[$i] $QLABEL COLD status=$ST author_ms=$AU materialize_ms=${MA:-?} wall_ms=$W mv=${MVTB:-?}"
  else
    echo "  author[$i] $QLABEL status=$ST (no author_ms — served warm; evicting to force cold next) wall_ms=$W"
  fi
  evict_mv "${MVNS:-tpcds_mv}" "$MVTB"                  # drop it → next iteration is cold
  sleep 1
done

cycle(){ # cycle <label> <seed-extra-args...> — commit, webhook, inline re-serve
  local label="$1"; shift
  # BENCH_FACT: which fact the append hits (default store_returns; use
  # catalog_sales for q64-class join-CTE MVs — the seeder appends referential
  # catalog_returns rows alongside so the join actually gains delta rows).
  local ft="${BENCH_FACT:-store_returns}"
  # Source-bucket creds must win over any stale ~/.aws/credentials profile.
  AWS_ACCESS_KEY_ID="${S3_KEY:-${AWS_ACCESS_KEY_ID:-}}" AWS_SECRET_ACCESS_KEY="${S3_SECRET:-${AWS_SECRET_ACCESS_KEY:-}}" \
  "$PY3" "$HERE/seed_tpcds.py" --catalog "$ICEBERG_URL" --warehouse "$WAREHOUSE" \
    --namespace "$NAMESPACE" --table "$ft" --rows "$CDC_ROWS" \
    --s3-region "$REGION" "$@" >/dev/null 2>&1
  curl -s -m 60 "$QAPI/admin/source/snapshot_changed" -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"namespace\":\"$NAMESPACE\",\"table\":\"$ft\",\"trigger\":\"bench-$label\"}" >/dev/null
  if [ "$ft" = catalog_sales ]; then
    curl -s -m 60 "$QAPI/admin/source/snapshot_changed" -H "Authorization: Bearer $TOKEN" \
      -H "Content-Type: application/json" \
      -d "{\"namespace\":\"$NAMESPACE\",\"table\":\"catalog_returns\",\"trigger\":\"bench-$label\"}" >/dev/null
  fi
  local T0 R T1
  T0=$(now); R=$(run_sql "$Q1"); T1=$(now)
  CY_MAT=$(echo "$R" | J materialize_ms); CY_Q=$(echo "$R" | J query_ms)
  CY_MERGE=$(echo "$R" | J merge_ms)   # delta-merge time (O(delta) incremental refresh)
  CY_ENGINE=$(echo "$R" | J engine); CY_WALL=$(el "$T0" "$T1")
}

# ---- B) append → incremental refresh ---------------------------------------------
B_MAT=(); B_MERGE=(); B_WALL=()
for i in $(seq 1 "$ITERS"); do
  cycle "append-$i"
  B_MAT+=("${CY_MAT:-0}"); B_MERGE+=("${CY_MERGE:-0}"); B_WALL+=("$CY_WALL")
  echo "  append[$i] refresh delta_merge_ms=${CY_MERGE:-?} materialize_ms=${CY_MAT:-?} query_ms=${CY_Q:-?} engine=$CY_ENGINE wall_ms=$CY_WALL"
done

echo
echo "== SUMMARY ($QLABEL-scale MV over ${CDC_ROWS}-row commits) =="
echo "  one-time author_ms (fresh queries):   $(stats "${A_AUTH[@]:-}")"
echo "  one-time materialize_ms:              $(stats "${A_MAT[@]:-}")"
echo "  one-time wall_ms:                     $(stats "${A_WALL[@]:-}")"
echo "  append  delta_merge_ms:               $(stats "${B_MERGE[@]:-}")"
echo "  append  refresh materialize_ms:       $(stats "${B_MAT[@]:-}")"
echo "  append  commit→answer wall_ms:        $(stats "${B_WALL[@]:-}")"
