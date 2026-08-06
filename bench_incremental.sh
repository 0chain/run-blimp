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
Q1='SELECT *
FROM
  (SELECT count(*) h8_30_to_9
   FROM store_sales,
        household_demographics,
        time_dim,
        store
   WHERE ss_sold_time_sk = time_dim.t_time_sk
     AND ss_hdemo_sk = household_demographics.hd_demo_sk
     AND ss_store_sk = s_store_sk
     AND time_dim.t_hour = 8
     AND time_dim.t_minute >= 30
     AND ((household_demographics.hd_dep_count = 4
           AND household_demographics.hd_vehicle_count<=4+2)
          OR (household_demographics.hd_dep_count = 2
              AND household_demographics.hd_vehicle_count<=2+2)
          OR (household_demographics.hd_dep_count = 0
              AND household_demographics.hd_vehicle_count<=0+2))
     AND store.s_store_name = '\''ese'\'') s1,
  (SELECT count(*) h9_to_9_30
   FROM store_sales,
        household_demographics,
        time_dim,
        store
   WHERE ss_sold_time_sk = time_dim.t_time_sk
     AND ss_hdemo_sk = household_demographics.hd_demo_sk
     AND ss_store_sk = s_store_sk
     AND time_dim.t_hour = 9
     AND time_dim.t_minute < 30
     AND ((household_demographics.hd_dep_count = 4
           AND household_demographics.hd_vehicle_count<=4+2)
          OR (household_demographics.hd_dep_count = 2
              AND household_demographics.hd_vehicle_count<=2+2)
          OR (household_demographics.hd_dep_count = 0
              AND household_demographics.hd_vehicle_count<=0+2))
     AND store.s_store_name = '\''ese'\'') s2,
  (SELECT count(*) h9_30_to_10
   FROM store_sales,
        household_demographics,
        time_dim,
        store
   WHERE ss_sold_time_sk = time_dim.t_time_sk
     AND ss_hdemo_sk = household_demographics.hd_demo_sk
     AND ss_store_sk = s_store_sk
     AND time_dim.t_hour = 9
     AND time_dim.t_minute >= 30
     AND ((household_demographics.hd_dep_count = 4
           AND household_demographics.hd_vehicle_count<=4+2)
          OR (household_demographics.hd_dep_count = 2
              AND household_demographics.hd_vehicle_count<=2+2)
          OR (household_demographics.hd_dep_count = 0
              AND household_demographics.hd_vehicle_count<=0+2))
     AND store.s_store_name = '\''ese'\'') s3,
  (SELECT count(*) h10_to_10_30
   FROM store_sales,
        household_demographics,
        time_dim,
        store
   WHERE ss_sold_time_sk = time_dim.t_time_sk
     AND ss_hdemo_sk = household_demographics.hd_demo_sk
     AND ss_store_sk = s_store_sk
     AND time_dim.t_hour = 10
     AND time_dim.t_minute < 30
     AND ((household_demographics.hd_dep_count = 4
           AND household_demographics.hd_vehicle_count<=4+2)
          OR (household_demographics.hd_dep_count = 2
              AND household_demographics.hd_vehicle_count<=2+2)
          OR (household_demographics.hd_dep_count = 0
              AND household_demographics.hd_vehicle_count<=0+2))
     AND store.s_store_name = '\''ese'\'') s4,
  (SELECT count(*) h10_30_to_11
   FROM store_sales,
        household_demographics,
        time_dim,
        store
   WHERE ss_sold_time_sk = time_dim.t_time_sk
     AND ss_hdemo_sk = household_demographics.hd_demo_sk
     AND ss_store_sk = s_store_sk
     AND time_dim.t_hour = 10
     AND time_dim.t_minute >= 30
     AND ((household_demographics.hd_dep_count = 4
           AND household_demographics.hd_vehicle_count<=4+2)
          OR (household_demographics.hd_dep_count = 2
              AND household_demographics.hd_vehicle_count<=2+2)
          OR (household_demographics.hd_dep_count = 0
              AND household_demographics.hd_vehicle_count<=0+2))
     AND store.s_store_name = '\''ese'\'') s5,
  (SELECT count(*) h11_to_11_30
   FROM store_sales,
        household_demographics,
        time_dim,
        store
   WHERE ss_sold_time_sk = time_dim.t_time_sk
     AND ss_hdemo_sk = household_demographics.hd_demo_sk
     AND ss_store_sk = s_store_sk
     AND time_dim.t_hour = 11
     AND time_dim.t_minute < 30
     AND ((household_demographics.hd_dep_count = 4
           AND household_demographics.hd_vehicle_count<=4+2)
          OR (household_demographics.hd_dep_count = 2
              AND household_demographics.hd_vehicle_count<=2+2)
          OR (household_demographics.hd_dep_count = 0
              AND household_demographics.hd_vehicle_count<=0+2))
     AND store.s_store_name = '\''ese'\'') s6,
  (SELECT count(*) h11_30_to_12
   FROM store_sales,
        household_demographics,
        time_dim,
        store
   WHERE ss_sold_time_sk = time_dim.t_time_sk
     AND ss_hdemo_sk = household_demographics.hd_demo_sk
     AND ss_store_sk = s_store_sk
     AND time_dim.t_hour = 11
     AND time_dim.t_minute >= 30
     AND ((household_demographics.hd_dep_count = 4
           AND household_demographics.hd_vehicle_count<=4+2)
          OR (household_demographics.hd_dep_count = 2
              AND household_demographics.hd_vehicle_count<=2+2)
          OR (household_demographics.hd_dep_count = 0
              AND household_demographics.hd_vehicle_count<=0+2))
     AND store.s_store_name = '\''ese'\'') s7,
  (SELECT count(*) h12_to_12_30
   FROM store_sales,
        household_demographics,
        time_dim,
        store
   WHERE ss_sold_time_sk = time_dim.t_time_sk
     AND ss_hdemo_sk = household_demographics.hd_demo_sk
     AND ss_store_sk = s_store_sk
     AND time_dim.t_hour = 12
     AND time_dim.t_minute < 30
     AND ((household_demographics.hd_dep_count = 4
           AND household_demographics.hd_vehicle_count<=4+2)
          OR (household_demographics.hd_dep_count = 2
              AND household_demographics.hd_vehicle_count<=2+2)
          OR (household_demographics.hd_dep_count = 0
              AND household_demographics.hd_vehicle_count<=0+2))
     AND store.s_store_name = '\''ese'\'') s8 '

# BENCH_QNR: bench a different TPC-DS query (reads $Q_DIR/q<N>.sql, e.g.
# BENCH_QNR=64). Labels below follow suit.
#
# DEFAULT q88, and it is embedded inline above rather than read from $Q_DIR so
# `blimp bench` works on a box that has never generated the query files.
#
# Why q88: it is the only query in the default CDC set that currently authors,
# VERIFIES and delta-merges end to end. Measured on a fresh SF1 cluster
# (2026-08-05, image c4e6c88e6): exact-mv match on a 158,369-row banked MV,
# `verify claim=query matched=true`, served in 130-135 ms. At SF1000 the same
# shape merged in 146-1,525 ms and served in 176-593 ms.
#
# The previous default q1 was a poor benchmark subject: it is the
# customer_total_return correlated-average shape, and on the same cluster its
# archetype authors an MV whose rewrite does not survive the query-level verify.
# A bench whose subject falls back to a base scan measures the base scan.
#
# Do NOT default this to q9 or q13: q9's MV is 101 rows at EVERY scale factor
# (ss_quantity is a bounded domain) and q13's is 23, so both are refused by the
# ZUS_MV_AUTHORING_MIN_MV_ROWS=1000 floor that provisioning sets — no dataset
# size rescues them. q59 ties on its ORDER BY key (store has 12 rows but only 6
# distinct s_store_id, so the self-join fans out) and its LIMIT 100 is therefore
# unstable, which reads as a verify mismatch that is not a real one.
# Which dataset the gateway answers from — internal (the built-in TPC-DS set,
# the UI's "Run on TPC SF1") or customer (an external source wired via
# `blimp --setup`). Default internal: hardcoding "customer" made every run on a
# fresh cluster return an error the harness rendered as a 0ms non-answer.
SOURCE="${SOURCE:-internal}"
BENCH_QNR="${BENCH_QNR:-88}"
if [ "$BENCH_QNR" != "88" ]; then
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
  -d "$(python3 -c 'import json,sys;print(json.dumps({"original_sql":sys.argv[1],"source":sys.argv[2]}))' "$1" "$SOURCE")"; }

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

# query_fact — the fact table the benched query actually reads, so the append
# lands where the MV's grain comes from. First fact mentioned in the SQL wins;
# falls back to store_sales (the fact behind most of the suite) rather than to a
# table the query may not read at all.
query_fact(){
  local sql="" f
  [ -n "${QF:-}" ] && [ -f "${QF:-}" ] && sql=$(tr 'A-Z' 'a-z' < "$QF")
  for f in store_sales catalog_sales web_sales store_returns catalog_returns web_returns inventory; do
    case "$sql" in *"$f"*) printf '%s' "$f"; return;; esac
  done
  printf 'store_sales'
}

echo "== MV lifecycle benchmark v2: cluster $CLUSTER_ID (authors=$AUTHOR_ITERS, appends=$ITERS, upserts=$ITERS, rows/cycle=$CDC_ROWS) =="
# Say which fact the appends hit. When it does not match the query's own fact
# there is no delta for its MV, every "refresh" is a warm re-serve, and
# delta_merge_ms comes back empty — which reads as "the delta path is broken"
# rather than "this run never changed the table the query reads".
echo "   appends hit fact: ${BENCH_FACT:-$(query_fact)} (query $QLABEL)"

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
  # BENCH_FACT: which fact the append hits. DERIVED FROM THE QUERY by default,
  # not hardcoded — a fixed default silently measures nothing for any query
  # whose fact it does not touch. That is what happened with q9/q88/q14: all
  # three are store_sales MVs, the append went to store_returns, so their source
  # never moved, there was no delta to merge, and the run reported
  # "delta_merge_ms: n=0" and a warm re-serve as though the delta path had been
  # exercised. Explicit BENCH_FACT still wins (use catalog_sales for q64-class
  # join-CTE MVs — the seeder appends referential catalog_returns rows alongside
  # so the join actually gains delta rows).
  local ft="${BENCH_FACT:-$(query_fact)}"
  # Source-bucket creds must win over any stale ~/.aws/credentials profile.
  AWS_ACCESS_KEY_ID="${S3_KEY:-${AWS_ACCESS_KEY_ID:-}}" AWS_SECRET_ACCESS_KEY="${S3_SECRET:-${AWS_SECRET_ACCESS_KEY:-}}" \
  "$PY3" "$HERE/seed_tpcds.py" --catalog "${ICEBERG_URL_LOCAL:-$ICEBERG_URL}" --warehouse "$WAREHOUSE" \
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
