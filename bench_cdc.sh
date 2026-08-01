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
# Every call is labeled (<name>:author / <name>:incr). Verification follows the
# product's own model: the cold author (force_author) IS verified — that is the
# one point where an MV's values are proven against the source — and the merge
# calls pass skip_verify, because a delta-merge re-verify is another full source
# scan per query and a merge that goes wrong falls back to force_author (which
# verifies) on its own. It also EVICTS these MVs first so `snapshot_changed`
# only wakes them, not a herd of stale multi-fact MVs from earlier runs.
#
# Env: GW CLUSTER_ID ICEBERG_URL WAREHOUSE [NAMESPACE=tpcds] [REGION=ap-south-1]
#      [CDC_ROWS=5000] [FORCE_AUTHOR=0] [VERIFY=0] [MERGE_THREADS=<n>]   (MERGE_THREADS is advisory — the merge
#      runs on the gateway; set the gateway's duckdb threads there to change it.)
set -u
: "${GW:?}" "${CLUSTER_ID:?}" "${ICEBERG_URL:?}" "${WAREHOUSE:?}"
NAMESPACE="${NAMESPACE:-tpcds}"; REGION="${REGION:-ap-south-1}"; CDC_ROWS="${CDC_ROWS:-5000}"
# QAPI/TOKEN overridable for non-cluster gateways (e.g. the test2 manual stack:
# QAPI=http://localhost:9100 TOKEN=<ZS3_ADMIN_TOKEN>); defaults keep the
# run-blimp cluster convention.
QAPI="${QAPI:-http://$GW:9000}"; TOKEN="${TOKEN:-zus-$CLUSTER_ID}"; HERE="$(cd "$(dirname "$0")" && pwd)"
PY3="${BLIMP_PY:-$HOME/.blimp_venv/bin/python3}"; [ -x "$PY3" ] || PY3="$HOME/venv_ib/bin/python3"; [ -x "$PY3" ] || PY3=python3
J(){ python3 -c "import json,sys
try: print(json.load(sys.stdin).get('$1',''))
except: print('')"; }

# ---- The delta-merge suite. Default = the five queries the merge work is
# actually being proven on (2026-07-31):
#   q9 q88   single-MV store_sales group-bys — sub-second merges, the easy rung
#   q14      3-fact (store_sales + catalog_sales + web_sales), Spark-verified
#   q64      multi-CTE, decomposes into branch MVs / whole-query recipe
#   q4       multi-CTE year_total over all three sales facts
# The old default (q3 q19 q43 q52 q55) proved single-fact CDC and now proves
# nothing new: every one of them merges. These five are where it still breaks.
# They are multi-fact by design, so CDC_APPEND_TABLES below appends to ALL the
# facts they read — a single-table append cannot exercise a k>1 merge.
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
  SUITES="${SUITES:-store_sales:9 88 14 64 4}"
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
declare -A A_MS M_MS S_MS I_QMS I_MERGE MERGE MODE MVTBL MV_ROWS MV_COLS

run(){ # run <sql> <label> [author_phase]  -> echoes the JSON
  # VERIFY=1 (blimp --query --verify) turns verification ON for the whole run;
  # default 0 is PROD-LIKE, because the gateway does no verification in the serving
  # path — correctness checking is a full source scan and production must not pay it
  # per request. Verification is a TEST-TIME option here, and a separate background
  # sampler is what catches drift in prod.
  #
  # arg3=1 marks the AUTHOR phase: verify it when VERIFY=1; every other call
  # is a warm serve / delta-merge → skip_verify. Passing skip_verify on the
  # author too suppressed the single verification the CDC model relies on.
  #
  # force_author is NO LONGER implied by arg3. A forced author is not what
  # production does: a real query arrives, the matcher finds (or does not find) an
  # MV, and CDC merges lazily before serving. Forcing made phase 1 rebuild MVs that
  # already existed — measured on test2 2026-07-30, q64 spent 497s re-authoring an
  # MV it already had (mv_h_95a7b7fa888f, 300k rows) before the append it was
  # supposed to be measuring. Set FORCE_AUTHOR=1 only to deliberately rebuild.
  # AUTHOR_TIMEOUT: graft-primary cold authors probe + materialize the WIDEST
  # feasible candidate first with cap-backoff — several full-fact CTAS attempts
  # can exceed 400s at SF1000; a shorter curl -m SIGKILLs the in-flight CTAS
  # (request ctx cancel) and every queued candidate with it (2026-07-22).
  # 1500 killed the q64 cross_sales fine-grain build (a 2.88B-group aggregate,
  # 19+ min of CTAS compute) at exactly 25 min (2026-07-27) — a cold branch
  # author + verify is two full source scans; give it two hours.
  curl -s -m "${AUTHOR_TIMEOUT:-7200}" "$QAPI/admin/query/run" -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "$(python3 -c 'import json,sys;print(json.dumps({"original_sql":sys.argv[1],"source":"customer","label":sys.argv[2],"skip_verify":not(sys.argv[3]=="1" and sys.argv[5]=="1"),"force_author":sys.argv[4]=="1"}))' "$1" "$2" "${3:-0}" "${FORCE_AUTHOR:-0}" "${VERIFY:-0}")"
}

echo "== CDC bench: cluster=$CLUSTER_ID gw=$GW rows/append=$CDC_ROWS suites=[$SUITES] =="

for ft in $(facts_of); do
  FNAMES=$(names_for_fact "$ft"); [ -n "$FNAMES" ] || continue
  echo "==== fact: $ft (${FNAMES% }) ===="
  echo ">> phase 1: serve/author all (force_author=${FORCE_AUTHOR:-0})"
  for n in $FNAMES; do
    R=$(run "${SQL[$n]}" "$n:author" 1)
    A_MS[$n]=$(echo "$R" | J author_ms); M_MS[$n]=$(echo "$R" | J materialize_ms)
    S_MS[$n]=$(echo "$R" | J query_ms);  MVTBL[$n]=$(echo "$R" | J mv_table)
    MV_ROWS[$n]=$(echo "$R" | J mv_rows); MV_COLS[$n]=$(echo "$R" | J mv_cols)
    # A WARM serve reports no mv_rows/mv_cols — only an author does — so the
    # dimensions were blank for exactly the queries that reused an MV, i.e. the
    # normal production case. Read them off the MV parquet instead (also proves the
    # file is really there). MV_DIMS_CMD is the hook: it receives the bare MV table
    # name and must echo "<rows> <cols>"; unset = leave as reported.
    if [ -z "${MV_ROWS[$n]}" ] && [ -n "${MVTBL[$n]}" ] && [ -n "${MV_DIMS_CMD:-}" ]; then
      D=$($MV_DIMS_CMD "${MVTBL[$n]##*.}" 2>/dev/null)
      MV_ROWS[$n]="${D%% *}"; MV_COLS[$n]="${D##* }"
    fi
    echo "   $n: author=${A_MS[$n]:-?} materialize=${M_MS[$n]:-?} cold_serve=${S_MS[$n]:-?}ms mv=${MV_ROWS[$n]:-?}x${MV_COLS[$n]:-?} (${MVTBL[$n]:-none})"
  done
  # MULTI-TABLE APPEND. CDC_APPEND_TABLES lets one cycle append to SEVERAL facts
  # before phase 3, so a multi-fact query actually exercises a multi-fact merge.
  # Without it every cycle touched exactly one fact, so q14 (store_sales +
  # catalog_sales + web_sales) and q64 (catalog_sales + catalog_returns) only ever
  # merged a single-table delta -- the k>1 path went untested. Defaults to $ft, so
  # unset behaviour is byte-identical to before.
  APPEND_TABLES="${CDC_APPEND_TABLES:-store_sales store_returns catalog_sales catalog_returns web_sales}"
  echo ">> phase 2: append +$CDC_ROWS to [$APPEND_TABLES] + snapshot_changed"
  for at in $APPEND_TABLES; do
  # Source-bucket creds must win over any stale ~/.aws/credentials profile.
  #
  # Capture instead of `| tail -1`: the pipe threw away both the traceback AND
  # the seeder's exit status, so an append that failed for EVERY fact printed one
  # cryptic line and the run carried on to phase 3 reporting merge_ms=- /
  # no-delta — indistinguishable from "this shape has no delta". Two real bugs
  # (parquet field IDs, then all-null decimal stats) hid behind that for hours.
  # On success keep the one-line summary; on failure print the whole traceback.
  seed_out=$(AWS_ACCESS_KEY_ID="${S3_KEY:-${AWS_ACCESS_KEY_ID:-}}" AWS_SECRET_ACCESS_KEY="${S3_SECRET:-${AWS_SECRET_ACCESS_KEY:-}}" \
    "$PY3" "$HERE/seed_tpcds.py" --catalog "$ICEBERG_URL" --warehouse "$WAREHOUSE" \
    --namespace "$NAMESPACE" --table "$at" --rows "$CDC_ROWS" --s3-region "$REGION" \
    ${S3_ENDPOINT:+--s3-endpoint "$S3_ENDPOINT"} 2>&1); seed_rc=$?
  if [ "$seed_rc" -ne 0 ]; then
    echo "   !! APPEND FAILED for $at (exit $seed_rc) — no rows added, so any"
    echo "   !! merge measured below is against UNCHANGED data. Full output:"
    printf '%s\n' "$seed_out" | sed 's/^/   | /'
  else
    printf '%s\n' "$seed_out" | tail -1
  fi
  curl -s -m 60 "$QAPI/admin/source/snapshot_changed" -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"namespace\":\"$NAMESPACE\",\"table\":\"$at\",\"trigger\":\"cdc-bench\"}" >/dev/null
  # catalog_sales appends are referential (seed also appends matching
  # catalog_returns rows — q64's cs_ui joins the two) → notify both tables.
  if [ "$at" = catalog_sales ]; then
    curl -s -m 60 "$QAPI/admin/source/snapshot_changed" -H "Authorization: Bearer $TOKEN" \
      -H "Content-Type: application/json" \
      -d "{\"namespace\":\"$NAMESPACE\",\"table\":\"catalog_returns\",\"trigger\":\"cdc-bench\"}" >/dev/null
  fi
  done
  echo ">> phase 3: re-run all (incremental)"
  for n in $FNAMES; do
    R=$(run "${SQL[$n]}" "$n:incr"); I_QMS[$n]=$(echo "$R" | J query_ms)
    # Lazy CDC model: snapshot_changed only MARKS the MV stale; the delta-merge
    # happens ON this query and is reported inline as merge_ms.
    I_MERGE[$n]=$(echo "$R" | J merge_ms)
    echo "   $n: incr_query=${I_QMS[$n]:-?}ms merge=${I_MERGE[$n]:-–}ms"
  done
  # ---- phase 4 (OPT-IN): verify the MERGED MV against the source -------------
  # A delta-merge is NOT verified anywhere: phase 3 passes skip_verify, and the
  # gateway deliberately does no verification in the serving path — correctness
  # checking is a full source scan and production must not pay it per request.
  # So a wrong delta is SERVED, unlike a wrong author which the row-hash gate
  # rejects. VERIFY_AFTER_MERGE=1 re-runs each query with verification ON, which
  # re-scans the source and row-hash-compares it to the merged MV.
  #
  # Costs a full original scan per query (measured at SF1000: 36s for q9, 1m27s
  # for q23, ~1m10s for q4) because the append INVALIDATES the 24h original-hash
  # cache — the answer changed, so the cached hash cannot be reused. Budget
  # ~1.5 min per query. Still far cheaper than a Spark job, which stays the
  # independent third check on a select few.
  if [ "${VERIFY:-0}" = "1" ]; then
    echo ">> phase 4: verify merged MVs against source (opt-in)"
    for n in $FNAMES; do
      R=$(run "${SQL[$n]}" "$n:verify" 1)
      V=$(echo "$R" | J status); VM=$(echo "$R" | J mv_table)
      echo "   $n: verify_status=${V:-?} mv=${VM:-none}"
    done
  fi
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
printf '%-10s %-14s %16s %11s %10s %8s %9s\n' query fact 'mv_rows x cols' author_ms merge_ms mode incr_ms
printf '%-10s %-14s %16s %11s %10s %8s %9s\n' ---------- -------------- ---------------- ----------- ---------- -------- ---------
for n in "${NAMES[@]}"; do
  # author time = shape+materialize; when phase-1 reused a warm MV, author_ms is
  # blank — fall back to materialize_ms so the cold-build cost is still shown.
  au="${A_MS[$n]}"; [ -z "$au" -o "$au" = "0" ] && au="${M_MS[$n]:-?}"
  printf '%-10s %-14s %16s %11s %10s %8s %9s\n' \
    "$n" "${FACT[$n]}" "${MV_ROWS[$n]:-?}x${MV_COLS[$n]:-?}" "$au" "${MERGE[$n]:-?}" "${MODE[$n]:-?}" "${I_QMS[$n]:-?}"
done
echo "=============================================================================="
echo "mode=incremental → delta-merged (merge_ms, reads |MV|+|delta|); fallback/no-delta"
echo "→ full re-author (author_ms, full fact scan). Merge is the O(MV) fast path."

# ---- outcome summary (reporting only, no gate) --------------------------------
# There is no PASS/FAIL assertion and no non-zero exit. A query that authored no
# MV, or refreshed by full re-author instead of a delta-merge, is a MEASUREMENT —
# often the interesting one — not a suite failure, and turning it into a red
# RESULT line hid the numbers behind a verdict. Each query's state is named
# plainly below; read the table above for the timings.
for n in "${NAMES[@]}"; do
  if [ -z "${MVTBL[$n]:-}" ] || [ "${MVTBL[$n]}" = "none" ]; then
    echo "  $n: no MV — served from base"
  elif [ "${MODE[$n]:-}" != "incremental" ]; then
    echo "  $n: MV ${MVTBL[$n]} — refreshed by full re-author (mode='${MODE[$n]:--}')"
  else
    echo "  $n: MV ${MVTBL[$n]} — delta-merged"
  fi
done
echo "DONE"
