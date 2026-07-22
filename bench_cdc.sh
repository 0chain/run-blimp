#!/usr/bin/env bash
# bench_cdc.sh — CDC-friendly suite: author ALL MVs, then one append, then
# re-run ALL to measure the incremental delta-merge. Prints a summary table
# (author / materialize / cold-serve / incremental merge_ms / incremental query_ms
# / wave mode) in addition to what shows on the cluster panel.
#
# Only CDC-refreshable shapes are included: single-fact SUM/COUNT group-by over
# the appended table (store_returns), no CTE/AVG/DISTINCT, grain wide enough to
# materialize (not result-shaped). See CDC_INCREMENTAL.md. Multi-fact (q25/q29)
# and AVG/CTE queries are NOT here — they fall back to full re-author by design.
#
# Every call is labeled (<name>:author / <name>:incr) and uses skip_verify so the
# panel is readable and the row-hash verify doesn't blow the request budget on big
# MVs. It also EVICTS these MVs first so `snapshot_changed` only wakes them, not a
# herd of stale multi-fact MVs from earlier runs.
#
# Env: GW CLUSTER_ID ICEBERG_URL WAREHOUSE [NAMESPACE=tpcds] [REGION=ap-south-1]
#      [CDC_ROWS=5000] [MERGE_THREADS=<n>]   (MERGE_THREADS is advisory — the merge
#      runs on the gateway; set the gateway's duckdb threads there to change it.)
set -u
: "${GW:?}" "${CLUSTER_ID:?}" "${ICEBERG_URL:?}" "${WAREHOUSE:?}"
NAMESPACE="${NAMESPACE:-tpcds}"; REGION="${REGION:-ap-south-1}"; CDC_ROWS="${CDC_ROWS:-5000}"
QAPI="http://$GW:9000"; TOKEN="zus-$CLUSTER_ID"; HERE="$(cd "$(dirname "$0")" && pwd)"
PY3="$HOME/venv_ib/bin/python3"; [ -x "$PY3" ] || PY3=python3
J(){ python3 -c "import json,sys
try: print(json.load(sys.stdin).get('$1',''))
except: print('')"; }

# ---- CDC-friendly suite over the appended fact: single-fact SUM/COUNT group-by
# queries whose ONE fact is the table we append to, so a delta on it merges.
# Default = the store_sales queries that both delta-merge AND are independently
# confirmed correct by spark_verify (see CDC_INCREMENTAL.md "Verified set"):
#   q3 q19 q43 q52 q55.
# Deliberately EXCLUDED (flaky or not incrementally mergeable, verified as such):
#   q34/q59/q68 (derived-grain result-shapes), q73/q79 (needle queries, no reusable
#   MV), q42/q46 (verifier can't confirm — self-aliased agg / derived-grain EXISTS).
# Set QNRS explicitly to test the full 99 or other facts (catalog_sales q15,q99;
# web_sales q45,q62). Multi-fact/window/AVG/CTE queries full-rebuild by design.
CDC_TABLE="${CDC_TABLE:-store_sales}"
Q_DIR="${Q_DIR:-$HOME/tpcds_queries}"
QNRS="${QNRS:-3 19 43 52 55}"
declare -A SQL; NAMES=()
for nr in $QNRS; do
  f="$Q_DIR/q$nr.sql"; [ -f "$f" ] || { echo "  skip q$nr: no $f"; continue; }
  NAMES+=("q$nr"); SQL[q$nr]="$(cat "$f")"
done
[ ${#NAMES[@]} -gt 0 ] || { echo "FATAL: no query files in $Q_DIR (generate via duckdb tpcds extension)"; exit 1; }

# per-run captured columns
declare -A A_MS M_MS S_MS I_QMS MERGE MODE MVTBL MV_ROWS

run(){ # run <sql> <label> [force]  -> echoes the JSON
  curl -s -m 400 "$QAPI/admin/query/run" -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "$(python3 -c 'import json,sys;print(json.dumps({"original_sql":sys.argv[1],"source":"customer","label":sys.argv[2],"skip_verify":True,"force_author":sys.argv[3]=="1"}))' "$1" "$2" "${3:-0}")"
}

echo "== CDC bench: cluster=$CLUSTER_ID gw=$GW rows/append=$CDC_ROWS =="

# ---- PHASE 1: author ALL (force a cold build) ----------------------------------
echo ">> phase 1: author all (force_author)"
for n in "${NAMES[@]}"; do
  R=$(run "${SQL[$n]}" "$n:author" 1)
  A_MS[$n]=$(echo "$R" | J author_ms); M_MS[$n]=$(echo "$R" | J materialize_ms)
  S_MS[$n]=$(echo "$R" | J query_ms);  MVTBL[$n]=$(echo "$R" | J mv_table)
  MV_ROWS[$n]=$(echo "$R" | J mv_rows)
  echo "   $n: author=${A_MS[$n]:-?} materialize=${M_MS[$n]:-?} cold_serve=${S_MS[$n]:-?}ms mv_rows=${MV_ROWS[$n]:-?} mv=${MVTBL[$n]:-none}"
done

# ---- PHASE 2: one append + notify ----------------------------------------------
echo ">> phase 2: append +$CDC_ROWS to $CDC_TABLE + snapshot_changed"
"$PY3" "$HERE/seed_tpcds.py" --catalog "$ICEBERG_URL" --warehouse "$WAREHOUSE" \
  --namespace "$NAMESPACE" --table "$CDC_TABLE" --rows "$CDC_ROWS" --s3-region "$REGION" 2>&1 | tail -1
curl -s -m 60 "$QAPI/admin/source/snapshot_changed" -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"namespace\":\"$NAMESPACE\",\"table\":\"$CDC_TABLE\",\"trigger\":\"cdc-bench\"}" >/dev/null

# ---- PHASE 3: re-run ALL (incremental) -----------------------------------------
echo ">> phase 3: re-run all (incremental)"
for n in "${NAMES[@]}"; do
  R=$(run "${SQL[$n]}" "$n:incr"); I_QMS[$n]=$(echo "$R" | J query_ms)
  echo "   $n: incr_query=${I_QMS[$n]:-?}ms"
done
sleep 4

# ---- pull per-MV merge_ms + mode from the wave log -----------------------------
WAVE=$(curl -s -m 30 "$QAPI/admin/mv/wave/report?limit=40" -H "Authorization: Bearer $TOKEN")
for n in "${NAMES[@]}"; do
  t="${MVTBL[$n]##*.}"
  read m md < <(echo "$WAVE" | python3 -c "
import json,sys
w=json.load(sys.stdin).get('waves',[])
for x in w:
  if (x.get('mv_table','').split('.')[-1])=='$t':
    print(x.get('merge_ms', x.get('materialize_ms','')) or '-', x.get('mode','-')); break
else: print('-','-')")
  MERGE[$n]="$m"; MODE[$n]="$md"
done

# ---- SUMMARY TABLE (== the CDC "contributions" table) --------------------------
# query | source fact | MV rows | cold author (full 2.88B-fact scan) | delta-merge
# | incr-serve | mode. Author time is dominated by the fact SCAN (independent of
# MV size), which is exactly why delta-merge (reads only |MV|+|delta|) wins big.
echo ""
echo "============================== CDC CONTRIBUTIONS =============================="
printf '%-10s %-14s %10s %11s %10s %8s %9s\n' query fact mv_rows author_ms merge_ms mode incr_ms
printf '%-10s %-14s %10s %11s %10s %8s %9s\n' ---------- -------------- ---------- ----------- ---------- -------- ---------
for n in "${NAMES[@]}"; do
  # author time = shape+materialize; when phase-1 reused a warm MV, author_ms is
  # blank — fall back to materialize_ms so the cold-build cost is still shown.
  au="${A_MS[$n]}"; [ -z "$au" -o "$au" = "0" ] && au="${M_MS[$n]:-?}"
  printf '%-10s %-14s %10s %11s %10s %8s %9s\n' \
    "$n" "$CDC_TABLE" "${MV_ROWS[$n]:-?}" "$au" "${MERGE[$n]:-?}" "${MODE[$n]:-?}" "${I_QMS[$n]:-?}"
done
echo "=============================================================================="
echo "mode=incremental → delta-merged (merge_ms, reads |MV|+|delta|); fallback/no-delta"
echo "→ full re-author (author_ms, full ${CDC_TABLE} scan). Merge is the O(MV) fast path."
echo "DONE"
