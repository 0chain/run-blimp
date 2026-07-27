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
PY3="${BLIMP_PY:-$HOME/.blimp_venv/bin/python3}"; [ -x "$PY3" ] || PY3="$HOME/venv_ib/bin/python3"; [ -x "$PY3" ] || PY3=python3
J(){ python3 -c "import json,sys
try: print(json.load(sys.stdin).get('$1',''))
except: print('')"; }

# ---- CDC-friendly suite over the appended fact: single-fact SUM/COUNT group-by
# queries whose ONE fact is the table we append to, so a delta on it merges.
# Default = the store_sales queries that both delta-merge AND are independently
# confirmed correct (see CDC_INCREMENTAL.md "Verified set"):
#   q3 q19 q43 q52 q55.
# Deliberately EXCLUDED (flaky or not incrementally mergeable, verified as such):
#   q34/q59/q68 (derived-grain result-shapes), q73/q79 (needle queries, no reusable
#   MV), q42/q46 (verifier can't confirm — self-aliased agg / derived-grain EXISTS).
# Set QNRS explicitly to test the full 99 or other facts (catalog_sales q15,q99;
# web_sales q45,q62). Multi-fact/window/AVG/CTE queries full-rebuild by design.
# MULTI-FACT suites: "fact:qnrs;fact:qnrs;…". Each fact gets its own
# author → append → incremental cycle, so CDC is proven per table, not just
# store_sales. Facts with no graftable/verified queries yet are omitted.
# Override with SUITES or the legacy CDC_TABLE/QNRS pair.
Q_DIR="${Q_DIR:-$HOME/tpcds_queries}"
if [ -n "${QNRS:-}" ]; then
  SUITES="${CDC_TABLE:-store_sales}:$QNRS"
else
  SUITES="${SUITES:-store_returns:1}"
fi
declare -A SQL FACT; NAMES=()
IFS=';' read -ra SUITE_ARR <<< "$SUITES"
for su in "${SUITE_ARR[@]}"; do
  fact="${su%%:*}"
  for nr in ${su#*:}; do
    f="$Q_DIR/q$nr.sql"; [ -f "$f" ] || { echo "  skip q$nr: no $f"; continue; }
    NAMES+=("q$nr"); SQL[q$nr]="$(cat "$f")"; FACT[q$nr]="$fact"
  done
done
[ ${#NAMES[@]} -gt 0 ] || { echo "FATAL: no query files in $Q_DIR (generate via duckdb tpcds extension)"; exit 1; }
facts_of(){ printf '%s\n' "${SUITE_ARR[@]}" | cut -d: -f1 | sort -u; }
names_for_fact(){ local ft="$1" n; for n in "${NAMES[@]}"; do [ "${FACT[$n]}" = "$ft" ] && printf '%s ' "$n"; done; }

# per-run captured columns
declare -A A_MS M_MS S_MS I_QMS I_MERGE MERGE MODE MVTBL MV_ROWS

run(){ # run <sql> <label> [force]  -> echoes the JSON
  # AUTHOR_TIMEOUT: graft-primary cold authors probe + materialize the WIDEST
  # feasible candidate first with cap-backoff — several full-fact CTAS attempts
  # can exceed 400s at SF1000; a shorter curl -m SIGKILLs the in-flight CTAS
  # (request ctx cancel) and every queued candidate with it (2026-07-22).
  # 1500 killed the q64 cross_sales fine-grain build (a 2.88B-group aggregate,
  # 19+ min of CTAS compute) at exactly 25 min (2026-07-27) — a cold branch
  # author + verify is two full source scans; give it two hours.
  curl -s -m "${AUTHOR_TIMEOUT:-7200}" "$QAPI/admin/query/run" -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "$(python3 -c 'import json,sys;print(json.dumps({"original_sql":sys.argv[1],"source":"customer","label":sys.argv[2],"skip_verify":True,"force_author":sys.argv[3]=="1"}))' "$1" "$2" "${3:-0}")"
}

echo "== CDC bench: cluster=$CLUSTER_ID gw=$GW rows/append=$CDC_ROWS suites=[$SUITES] =="

for ft in $(facts_of); do
  FNAMES=$(names_for_fact "$ft"); [ -n "$FNAMES" ] || continue
  echo "==== fact: $ft (${FNAMES% }) ===="
  echo ">> phase 1: author all (force_author)"
  for n in $FNAMES; do
    R=$(run "${SQL[$n]}" "$n:author" 1)
    A_MS[$n]=$(echo "$R" | J author_ms); M_MS[$n]=$(echo "$R" | J materialize_ms)
    S_MS[$n]=$(echo "$R" | J query_ms);  MVTBL[$n]=$(echo "$R" | J mv_table)
    MV_ROWS[$n]=$(echo "$R" | J mv_rows)
    echo "   $n: author=${A_MS[$n]:-?} materialize=${M_MS[$n]:-?} cold_serve=${S_MS[$n]:-?}ms mv_rows=${MV_ROWS[$n]:-?} mv=${MVTBL[$n]:-none}"
  done
  echo ">> phase 2: append +$CDC_ROWS to $ft + snapshot_changed"
  # Source-bucket creds must win over any stale ~/.aws/credentials profile.
  AWS_ACCESS_KEY_ID="${S3_KEY:-${AWS_ACCESS_KEY_ID:-}}" AWS_SECRET_ACCESS_KEY="${S3_SECRET:-${AWS_SECRET_ACCESS_KEY:-}}" \
  "$PY3" "$HERE/seed_tpcds.py" --catalog "$ICEBERG_URL" --warehouse "$WAREHOUSE" \
    --namespace "$NAMESPACE" --table "$ft" --rows "$CDC_ROWS" --s3-region "$REGION" 2>&1 | tail -1
  curl -s -m 60 "$QAPI/admin/source/snapshot_changed" -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"namespace\":\"$NAMESPACE\",\"table\":\"$ft\",\"trigger\":\"cdc-bench\"}" >/dev/null
  # catalog_sales appends are referential (seed also appends matching
  # catalog_returns rows — q64's cs_ui joins the two) → notify both tables.
  if [ "$ft" = catalog_sales ]; then
    curl -s -m 60 "$QAPI/admin/source/snapshot_changed" -H "Authorization: Bearer $TOKEN" \
      -H "Content-Type: application/json" \
      -d "{\"namespace\":\"$NAMESPACE\",\"table\":\"catalog_returns\",\"trigger\":\"cdc-bench\"}" >/dev/null
  fi
  echo ">> phase 3: re-run all (incremental)"
  for n in $FNAMES; do
    R=$(run "${SQL[$n]}" "$n:incr"); I_QMS[$n]=$(echo "$R" | J query_ms)
    # Lazy CDC model: snapshot_changed only MARKS the MV stale; the delta-merge
    # happens ON this query and is reported inline as merge_ms.
    I_MERGE[$n]=$(echo "$R" | J merge_ms)
    echo "   $n: incr_query=${I_QMS[$n]:-?}ms merge=${I_MERGE[$n]:-–}ms"
  done
done
sleep 4

# ---- pull per-MV merge_ms + mode from the wave log -----------------------------
WAVE=$(curl -s -m 30 "$QAPI/admin/mv/wave/report?limit=40" -H "Authorization: Bearer $TOKEN")
for n in "${NAMES[@]}"; do
  t="${MVTBL[$n]##*.}"
  # Rows are newest-first. The webhook refreshes ASYNC, so by the time we
  # re-query, the merge row is often buried under later 'no-delta' true-no-op
  # advances (drift-poll rechecks). Take the newest REAL refresh row
  # (incremental/full); fall back to the newest row of any mode only if no
  # real refresh exists.
  read m md < <(echo "$WAVE" | python3 -c "
import json,sys
w=json.load(sys.stdin).get('waves',[])
rows=[x for x in w if (x.get('mv_table','').split('.')[-1])=='$t']
real=[x for x in rows if x.get('mode') in ('incremental','full')]
x=(real or rows or [{}])[0]
print(x.get('merge_ms', x.get('materialize_ms','')) or '-', x.get('mode','-'))")
  MERGE[$n]="$m"; MODE[$n]="$md"
  # Lazy CDC: the query-time inline merge_ms is authoritative — the wave log
  # only sees proactive merges, which the lazy default no longer performs.
  if [ -n "${I_MERGE[$n]:-}" ] && [ "${I_MERGE[$n]}" != "null" ]; then
    MERGE[$n]="${I_MERGE[$n]}"; MODE[$n]="incremental"
  fi
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
    "$n" "${FACT[$n]}" "${MV_ROWS[$n]:-?}" "$au" "${MERGE[$n]:-?}" "${MODE[$n]:-?}" "${I_QMS[$n]:-?}"
done
echo "=============================================================================="
echo "mode=incremental → delta-merged (merge_ms, reads |MV|+|delta|); fallback/no-delta"
echo "→ full re-author (author_ms, full fact scan). Merge is the O(MV) fast path."

# ---- PASS/FAIL gate: every query must have built an MV AND delta-merged --------
FAILS=0
for n in "${NAMES[@]}"; do
  [ -n "${MVTBL[$n]:-}" ] && [ "${MVTBL[$n]}" != "none" ] || { echo "FAIL $n: no MV built"; FAILS=$((FAILS+1)); continue; }
  [ "${MODE[$n]:-}" = "incremental" ] || { echo "FAIL $n: refresh mode='${MODE[$n]:--}' (expected incremental)"; FAILS=$((FAILS+1)); }
done
if [ "$FAILS" -eq 0 ]; then echo "RESULT: PASS (${#NAMES[@]}/${#NAMES[@]} authored + delta-merged)"
else echo "RESULT: FAIL ($FAILS of ${#NAMES[@]} queries failed the gate)"; fi
echo "DONE"
exit $([ "$FAILS" -eq 0 ] && echo 0 || echo 1)
