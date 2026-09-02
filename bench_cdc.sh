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
# SOURCE selects which dataset the gateway answers from. The accepted ids are
# normalizeSourceID's: internal|demo|sf1|minio -> the BUILT-IN TPC-DS set (the
# UI's "Run on TPC SF1"), customer|prod|production -> an external Iceberg/S3
# source the operator wired via `blimp --setup`.
#
# Default INTERNAL. This used to be hardcoded "customer", which meant the whole
# suite could never run on a cluster that had no external source wired — i.e. on
# every fresh cluster. The gateway refuses loudly and correctly:
#
#   "no external Prod source is wired to this node — connect your Iceberg/S3
#    source (blimp --setup), or use Run on TPC SF1 for the internal dataset"
#
# and bench_cdc reported it as `author=? materialize=? cold_serve=0ms mv=?x?
# (none)` for EVERY query — 19 identical non-answers that look like 19 failures
# and are actually one wiring error. Observed on cluster 1785947456705,
# 2026-08-05. Set SOURCE=customer to drive a wired production source.
# Derive the gateway source from the dataset THIS env points at, never assume
# the on-cluster SF1 demo: NAMESPACE=tpcds_sf1000 → the sf1000 source (test2's
# SF1000 legacy source), a tpcds/tpcds_sf1 namespace → internal (the UI's
# "Run on TPC SF1" tab), anything else → the wired customer source. 2026-09-02:
# the default "internal" ran the whole suite on SF1 while the seeder appended
# to the SF1000 catalog — every result was for the wrong dataset.
if [ -z "${SOURCE:-}" ]; then
  case "${NAMESPACE:-tpcds}" in
    tpcds_sf1000|*sf1000*) SOURCE=sf1000 ;;
    tpcds|tpcds_sf1)       SOURCE=internal ;;
    *)                     SOURCE=customer ;;
  esac
fi
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
  # DEFAULT SUITE = the four queries proven end-to-end at SF1000 on the AWS
  # single-box platform (r6i.4xlarge, NAS-mode gateway, local MinIO + Iceberg).
  # Ordered by BASE wall time so the suite ramps cheap → expensive:
  #
  #        base wall   MV warm    APPEND merge_ms   post-merge serve
  #   q13     2.2 s     149 ms         240 ms            156 ms
  #   q59     3.2 s     293 ms         133 ms            319 ms
  #   q88    19.5 s     182 ms         146 ms            176 ms
  #   q9     23.2 s     450 ms         471 ms            143 ms
  #
  # All four take mode=APPEND ("delta-only aggregate, cost O(|delta|)") after a
  # proportional store_sales append — that is the whole point of this suite, so
  # every slot must actually merge. The previous default (9 88 14 64 4) carried
  # q14/q64/q4: q14 and q64 had no verified recipe at all, and q4's MV is
  # fact-sized (~172M rows at SF1000), so three of five slots proved nothing.
  # A later default (9 44 65 59 36 88;web_sales:2) was measured at SF1, not
  # SF1000, so its merge_ms said nothing about behaviour at scale.
  #
  # The condition every query here satisfies: a banked recipe whose delta has
  # exactly ONE fact per UNION branch. With k facts in a branch the binder
  # expands 2^k-1 inclusion-exclusion terms and each unchanged fact binds at
  # FULL size — q64 (2 facts/branch, 6 terms) took 702 s at SF1000 before being
  # killed, against 133-471 ms for the single-fact recipes above.
  # PLUS the 15 linear-core recipes now embedded in the gateway binary
  # (mv_linear_recipes.json, imported into rag_authors at boot). They are here
  # so a run EXERCISES them — embedding a recipe nothing runs proves nothing.
  # Grouped by the fact whose append should move each query's MV:
  #
  #   store_sales  q47 q70 q24 q13 q59 q88 q9   single-fact, store-driven
  #                q4 q11 q14 q23 q31 q51 q17   multi-channel, store is the
  #                q5 q80 q49                    largest contributor
  #   catalog_sales q72                          cs x inventory
  #   web_sales     q95                          web-only
  #
  # STATUS, so a red slot is read correctly rather than chased:
  #   PROVEN at SF1000 (mode=APPEND measured): q13 q59 q88 q9 q14 q24 q4 q11 q23
  #   PROVEN at SF1 by their authoring agents, NEVER verified by a gateway:
  #     q5 q17 q31 q47 q49 q51 q70 q72 q80 q95 — these import with an EMPTY
  #     row_hash on purpose, so THIS cluster's verifier decides. A first-run
  #     failure on one of them is information, not a regression.
  #   EXPECTED NOT to merge: q72 ships cover-only (no sound delta exists);
  #     q17 and q49 have >1 fact per branch, so their merges are 2^k-1 terms.
  #
  # Set QNRS/SUITES to trim this; it is deliberately broad because the point of
  # a fresh-cluster run is to find out which recipes survive contact with real
  # data, not to confirm the four we already know about.
  # THE FIVE. Every one MEASURED delta-merging end to end at SF1000 on
  # 2026-08-05 (i-036afe917af144a84, after a +50,000-row store_sales append that
  # marked 6 MVs stale). Ordered by merge cost:
  #
  #        mode      merge_ms   query_ms   wall
  #   q47  APPEND         125        236   0.93 s
  #   q59  APPEND         130        415   0.90 s
  #   q88  APPEND         146        195   1.14 s
  #   q13  APPEND         233        144   1.08 s
  #   q9   APPEND         238        115   0.83 s
  #
  # mode=APPEND is the whole point: "delta-only aggregate, cost O(|delta|), MV
  # parquet not rewritten". All five are single-fact-per-branch, which is the
  # condition for that — with k facts in a branch the binder expands 2^k-1
  # inclusion-exclusion terms and each unchanged fact binds at FULL size.
  #
  # Kept deliberately SMALL. The 19-query set this replaced ran every recipe in
  # the shipped corpus, which is the right thing for a coverage sweep and the
  # wrong thing for a default: it authored 19 queries through the LLM ladder on
  # a fresh cluster, and most of those recipes have never been verified by a
  # gateway. Use SUITES= for that sweep; the default stays the proven five.
  SUITES="${SUITES:-store_sales:47 59 88 13 9}"
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
declare -A A_MS M_MS S_MS I_QMS I_MERGE MERGE MODE MVTBL MV_ROWS MV_COLS MV_HASH_OLD MV_HASH_NEW DELTA_ROWS DELTA_VERDICT

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
    -d "$(python3 -c 'import json,sys;print(json.dumps({"original_sql":sys.argv[1],"source":sys.argv[6],"label":sys.argv[2],"skip_verify":not(sys.argv[3]=="1" and sys.argv[5]=="1"),"force_author":sys.argv[4]=="1"}))' "$1" "$2" "${3:-0}" "${FORCE_AUTHOR:-0}" "${VERIFY:-0}" "$SOURCE")"
}

# ---- MV content signature + THE DELTA GATE -----------------------------------
# The query response only reports mv_rows/mv_cols on a COLD author, so once the
# MVs exist every run printed "?x?" and there was no way to tell a merge that
# updated the MV from one that did nothing. Row count alone cannot tell you
# either: these are additive aggregates at a fixed grain, so appended rows land
# in EXISTING groups and the count does not move even when every measure changed.
#
# The gate that actually settles it is the DELTA PART ROW COUNT. The gateway
# counts delta FILES and never delta ROWS, so a merge over an empty delta is
# indistinguishable from a real one in every log line and every response field
# it emits. Measured on test2 SF1000 2026-08-04:
#     mv_h_4a822e01c1c3/delta-*.parquet ->      0 rows   (q24, 15,233 ms)
#     mv_h_8c010ddc8133/delta-*.parquet ->      0 rows   (q88,  2,346 ms)
#     mv_f5dcf9d821f33c4a/delta-*.parquet -> 49,992 rows (a real merge)
# Two of those three "merges" measured nothing. mv_delta_rows.py snapshots each
# MV's parts.json + data.parquet ETag before phase 3 and counts the rows in the
# parts that appear after, so a run whose timings measured empty deltas is
# reported as INVALID instead of as a result.
#
# MV_BUCKET is the MV NAMESPACE with '_' -> '-' (ZS3_MV_NAMESPACE=tpcds_mv ->
# tpcds-mv). It is NOT ZS3_MV_WAREHOUSE_BUCKET: on test2 that is `mv-warehouse`,
# which holds only qresults/ and the iceberg warehouse — no delta parts at all,
# so pointing the gate there silently reports "no parts" for everything.
MV_NAMESPACE="${MV_NAMESPACE:-tpcds_mv}"
MV_BUCKET="${MV_BUCKET:-${MV_NAMESPACE//_/-}}"
# The MV bucket lives with the GATEWAY, not with the source. Defaulting this to
# $S3_ENDPOINT is correct only where the source and MV object stores happen to be
# the same MinIO (test2); on the AWS cluster S3_ENDPOINT is the source MinIO
# 10.10.250.114:9000 while the MVs are on the gateway at 10.10.250.209:9000, so
# the gate would have found no parts for anything and reported UNCHANGED across
# the board — a false negative that looks exactly like the bug.
MV_S3_ENDPOINT="${MV_S3_ENDPOINT:-http://$GW:9000}"
MV_S3_KEY="${MV_S3_KEY:-${GW_AK:-${S3_KEY:-${AWS_ACCESS_KEY_ID:-}}}}"
MV_S3_SECRET="${MV_S3_SECRET:-${GW_SK:-${S3_SECRET:-${AWS_SECRET_ACCESS_KEY:-}}}}"
DELTA_TOOL="$HERE/mv_delta_rows.py"
DELTA_PRE="$(mktemp -t mvpre.XXXXXX)"; DELTA_POST="$(mktemp -t mvpost.XXXXXX)"
trap 'rm -f "$DELTA_PRE" "$DELTA_POST"' EXIT

delta_tool(){ # delta_tool <snapshot|verdict> <extra args...> -- <tables...>
  [ -x "$PY3" ] || return 1
  [ -f "$DELTA_TOOL" ] || return 1
  MV_BUCKET="$MV_BUCKET" MV_S3_ENDPOINT="$MV_S3_ENDPOINT" \
  MV_S3_KEY="$MV_S3_KEY" MV_S3_SECRET="$MV_S3_SECRET" \
    "$PY3" "$DELTA_TOOL" "$@" 2>&1; }

mv_etag(){ # mv_etag <table> -> content signature of the materialized parquet
  [ -n "$MV_S3_KEY" ] || { printf ''; return; }
  AWS_ACCESS_KEY_ID="$MV_S3_KEY" AWS_SECRET_ACCESS_KEY="$MV_S3_SECRET" \
    aws s3api head-object --bucket "$MV_BUCKET" --key "$1/data.parquet" \
    --endpoint-url "$MV_S3_ENDPOINT" --region "${REGION:-us-east-1}" 2>/dev/null \
    | "$PY3" -c "import json,sys;print(json.load(sys.stdin).get('ETag','').strip('\"'))" 2>/dev/null; }

mv_dims(){ # mv_dims <table> -> "<rows> <cols>"; MV_DIMS_CMD is the node-side hook
  [ -n "${MV_DIMS_CMD:-}" ] && { $MV_DIMS_CMD "$1" 2>/dev/null; return; }
  # /admin/mv/list is a PROXY to the MV catalog and returns
  # "dial tcp: lookup host.docker.internal ... no such host" on the test2 stack,
  # so it cannot be the only source — hence the MV_DIMS_CMD hook above.
  curl -s -m 30 "$QAPI/admin/mv/list" -H "Authorization: Bearer $TOKEN" 2>/dev/null | "$PY3" -c "
import json,sys
t=sys.argv[1]
try: d=json.load(sys.stdin)
except Exception: print(''); raise SystemExit
if isinstance(d,dict) and d.get('error'): print(''); raise SystemExit
for m in (d if isinstance(d,list) else d.get('mvs',[])):
    if m.get('table')==t:
        print('%s %s'%(m.get('row_count','?'), len(m.get('schema') or []) or '?')); raise SystemExit
print('')" "$1" 2>/dev/null; }

# ---- phase 0 (opt-in, EVICT=1 / blimp --query --evict): GENUINE cold state ---
# force_author was REMOVED from the gateway (2026-07-31): a plain query can no
# longer ask to rebuild an MV it already has, so FORCE_AUTHOR above is a no-op on
# current images. The only real cold state is an evicted MV (POST /admin/mv/evict,
# recipe kept). Two-tier answers regenerate from their chart MV the moment the
# answer is evicted, so evict-and-rematch until the matcher returns nothing.
evict_query(){ # evict_query <sql> <name>
  local rounds=0 busy=0 m t ns e ok
  while :; do
    m=$(curl -s -m 600 "$QAPI/admin/query/run" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
      -d "$(python3 -c 'import json,sys;print(json.dumps({"original_sql":sys.argv[1],"source":sys.argv[3],"label":sys.argv[2]+":match","match_only":True,"skip_verify":True,"skip_passthrough":True}))' "$1" "$2" "$SOURCE")")
    t=$(echo "$m" | J mv_table); t="${t##*.}"; ns=$(echo "$m" | J mv_namespace)
    [ -n "$t" ] || break
    e=$(curl -s -m 120 "$QAPI/admin/mv/evict" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
      -d "{\"namespace\":\"${ns:-$MV_NAMESPACE}\",\"table\":\"$t\",\"keep_recipe\":true}")
    ok=$(echo "$e" | J evicted)
    echo "   $2: evict ${ns:-$MV_NAMESPACE}.$t evicted=$ok $(echo "$e" | J error)"
    if [ "$ok" != "True" ]; then
      # mid-merge / mid-serve (the match probe itself wakes a stale MV's refresh):
      # wait for the flight to land, then retry — up to ~10 min, like the
      # internal harness. Retrying instantly just re-hits the same lock.
      busy=$((busy+1)); [ "$busy" -ge 30 ] && { echo "   WARN $2: $t stayed busy for $busy rounds — NOT evicted, phase 1 serves warm"; break; }
      sleep 20; continue
    fi
    rounds=$((rounds+1))
    [ "$rounds" -ge 8 ] && { echo "   WARN $2 still matches after $rounds evictions (two-tier answer regenerating from a companion the API cannot reach)"; break; }
  done
}

echo "== CDC bench: cluster=$CLUSTER_ID gw=$GW rows/append=$CDC_ROWS suites=[$SUITES] =="
echo "== delta gate: bucket=$MV_BUCKET endpoint=$MV_S3_ENDPOINT tool=$DELTA_TOOL =="

for ft in $(facts_of); do
  FNAMES=$(names_for_fact "$ft"); [ -n "$FNAMES" ] || continue
  echo "==== fact: $ft (${FNAMES% }) ===="
  if [ "${EVICT:-0}" = "1" ]; then
    echo ">> phase 0: evict (cold state, recipe kept)"
    for n in $FNAMES; do evict_query "${SQL[$n]}" "$n"; done
  fi
  echo ">> phase 1: serve/author all (evict=${EVICT:-0} verify=${VERIFY:-0})"
  for n in $FNAMES; do
    R=$(run "${SQL[$n]}" "$n:author" 1)
    A_MS[$n]=$(echo "$R" | J author_ms); M_MS[$n]=$(echo "$R" | J materialize_ms)
    S_MS[$n]=$(echo "$R" | J query_ms);  MVTBL[$n]=$(echo "$R" | J mv_table)
    MV_ROWS[$n]=$(echo "$R" | J mv_rows); MV_COLS[$n]=$(echo "$R" | J mv_cols)
    t="${MVTBL[$n]##*.}"; [ -n "$t" ] && MV_HASH_OLD[$n]=$(mv_etag "$t")
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
  # ---- THE DELTA GATE, part 1: snapshot every MV's parts BEFORE the append ---
  # Must be before phase 2, not between phase 2 and phase 3: snapshot_changed can
  # trigger the webhook's ACTIVE refresh (webhook_router.go
  # "[webhook][active-refresh] merged mv=... merge_ms=..."), which writes the
  # delta part immediately. Snapshotting after the notify would treat that part
  # as pre-existing and report the query's merge as UNCHANGED — the gate would
  # then be lying in the same direction as the bug it exists to catch.
  GATE_TABLES=""
  for n in $FNAMES; do t="${MVTBL[$n]##*.}"; [ -n "$t" ] && GATE_TABLES="$GATE_TABLES $t"; done
  if [ -n "$GATE_TABLES" ]; then
    delta_tool snapshot --out "$DELTA_PRE" --tables $GATE_TABLES >/dev/null || \
      echo "   WARN: delta gate unavailable — merge_ms below is UNVERIFIED"
  fi
  # ---- phase 2: ONE PROPORTIONAL CDC TICK ACROSS ALL SIX FACTS --------------
  # This used to loop CDC_APPEND_TABLES and append a FLAT $CDC_ROWS to each of
  # five facts (and the AWS driver appended to only three, omitting returns
  # entirely). That is not a workload anyone runs: it makes web_sales as busy as
  # store_sales and returns as busy as sales. Real TPC-DS facts stand at roughly
  # 4:2:1 store:catalog:web with returns at ~10% of their parent, which is what
  # `seed_tpcds.py --tick` emits (--ratios / --returns-ratio to change it).
  #
  # --catalog prefers ICEBERG_URL_LOCAL: seed_tpcds.py runs HERE, while
  # ICEBERG_URL is the address the CLUSTER uses — in external mode that is this
  # node's PUBLIC ip, and dialling our own public ip is blocked by any
  # restrictive security group. That timed out on cluster 1786037195000
  # (2026-08-06), added 0 rows, and made every query below report "full
  # re-author / NO-BASELINE" — which reads like an optimizer regression but is
  # a connectivity failure.
  #
  # It is also ONE process, which is what lets returns be REFERENTIAL: the
  # returns rows are keyed on (item_sk, ticket/order number) of the sales rows
  # written microseconds earlier. Independently generated returns never join
  # their parent, so q24/q64-class merges scan both facts and produce nothing.
  #
  # CDC_APPEND_TABLES still forces the old flat per-table behaviour if set.
  APPEND_TABLES="${CDC_APPEND_TABLES:-}"
  NOTIFY_TABLES="store_sales store_returns catalog_sales catalog_returns web_sales web_returns"
  SEED_CREDS_AK="${S3_KEY:-${AWS_ACCESS_KEY_ID:-}}"; SEED_CREDS_SK="${S3_SECRET:-${AWS_SECRET_ACCESS_KEY:-}}"
  # Capture instead of `| tail -1`: the pipe threw away both the traceback AND
  # the seeder's exit status, so an append that failed for EVERY fact printed one
  # cryptic line and the run carried on to phase 3 reporting merge_ms=- /
  # no-delta — indistinguishable from "this shape has no delta". Two real bugs
  # (parquet field IDs, then all-null decimal stats) hid behind that for hours.
  if [ -z "$APPEND_TABLES" ]; then
    echo ">> phase 2: CDC tick (base=$CDC_ROWS rows, ratios=${CDC_RATIOS:-4:2:1}, returns=${CDC_RETURNS_RATIO:-0.1}) + snapshot_changed"
    seed_out=$(AWS_ACCESS_KEY_ID="$SEED_CREDS_AK" AWS_SECRET_ACCESS_KEY="$SEED_CREDS_SK" \
      "$PY3" "$HERE/seed_tpcds.py" --catalog "${ICEBERG_URL_LOCAL:-$ICEBERG_URL}" --warehouse "$WAREHOUSE" \
      --namespace "$NAMESPACE" --tick --rows "$CDC_ROWS" --s3-region "$REGION" \
      ${CDC_RATIOS:+--ratios "$CDC_RATIOS"} \
      --returns-ratio "${CDC_RETURNS_RATIO:-0.1}" \
      ${CDC_YEARS:+--years "$CDC_YEARS"} \
      ${S3_ENDPOINT:+--s3-endpoint "$S3_ENDPOINT"} 2>&1); seed_rc=$?
    if [ "$seed_rc" -ne 0 ]; then
      echo "   !! CDC TICK FAILED (exit $seed_rc) — no rows added, so EVERY merge"
      echo "   !! measured below is against UNCHANGED data. Full output:"
      printf '%s\n' "$seed_out" | sed 's/^/   | /'
    else
      printf '%s\n' "$seed_out" | sed 's/^/   /'
    fi
  else
    echo ">> phase 2: append +$CDC_ROWS to [$APPEND_TABLES] (flat, legacy) + snapshot_changed"
    NOTIFY_TABLES="$APPEND_TABLES"
    for at in $APPEND_TABLES; do
      seed_out=$(AWS_ACCESS_KEY_ID="$SEED_CREDS_AK" AWS_SECRET_ACCESS_KEY="$SEED_CREDS_SK" \
        "$PY3" "$HERE/seed_tpcds.py" --catalog "${ICEBERG_URL_LOCAL:-$ICEBERG_URL}" --warehouse "$WAREHOUSE" \
        --namespace "$NAMESPACE" --table "$at" --rows "$CDC_ROWS" --s3-region "$REGION" \
        --returns-ratio "${CDC_RETURNS_RATIO:-0.1}" \
        ${CDC_YEARS:+--years "$CDC_YEARS"} \
        ${S3_ENDPOINT:+--s3-endpoint "$S3_ENDPOINT"} 2>&1); seed_rc=$?
      if [ "$seed_rc" -ne 0 ]; then
        echo "   !! APPEND FAILED for $at (exit $seed_rc) — no rows added, so any"
        echo "   !! merge measured below is against UNCHANGED data. Full output:"
        printf '%s\n' "$seed_out" | sed 's/^/   | /'
      else
        printf '%s\n' "$seed_out" | sed 's/^/   /'
      fi
      case "$at" in
        *_sales) NOTIFY_TABLES="$NOTIFY_TABLES ${at%_sales}_returns";;
      esac
    done
  fi
  # Notify EVERY fact the tick touched. A sales append is referential — it also
  # writes rows into the matching returns fact — so notifying only the sales
  # table leaves the returns MVs believing their source never moved.
  for at in $(printf '%s\n' $NOTIFY_TABLES | sort -u); do
    curl -s -m 60 "$QAPI/admin/source/snapshot_changed" -H "Authorization: Bearer $TOKEN" \
      -H "Content-Type: application/json" \
      -d "{\"namespace\":\"$NAMESPACE\",\"table\":\"$at\",\"trigger\":\"cdc-bench\"}" >/dev/null
  done

  echo ">> phase 3: re-run all (incremental)"
  for n in $FNAMES; do
    R=$(run "${SQL[$n]}" "$n:incr"); I_QMS[$n]=$(echo "$R" | J query_ms)
    # Lazy CDC model: snapshot_changed only MARKS the MV stale; the delta-merge
    # happens ON this query and is reported inline as merge_ms.
    I_MERGE[$n]=$(echo "$R" | J merge_ms)
    # Take the MV's dimensions AND content hash from the MERGE response. Phase 1
    # only reports them on a COLD author, so once the MVs exist every later run
    # printed "?x?" — and with no hash there was no way to tell a merge that
    # updated the MV from one that did nothing. Row count alone cannot tell you:
    # these are additive aggregates at a fixed grain, so appended rows land in
    # EXISTING groups and the row count does not move even when the content does.
    t="${MVTBL[$n]##*.}"
    if [ -n "$t" ]; then
      read mr mc <<<"$(mv_dims "$t")"
      [ -n "${mr:-}" ] && MV_ROWS[$n]="$mr"; [ -n "${mc:-}" ] && MV_COLS[$n]="$mc"
      MV_HASH_NEW[$n]=$(mv_etag "$t")
    fi
    mh="${MV_HASH_NEW[$n]:-}"
    hint=""
    if [ -n "$mh" ] && [ -n "${MV_HASH_OLD[$n]:-}" ]; then
      # base-parquet ETag only; an append merge leaves it untouched by design,
      # so this is informational — the delta gate below is what decides.
      if [ "$mh" = "${MV_HASH_OLD[$n]}" ]; then hint=" base=untouched"; else hint=" base=rewritten"; fi
    fi
    echo "   $n: incr_query=${I_QMS[$n]:-?}ms merge=${I_MERGE[$n]:-–}ms mv=${MV_ROWS[$n]:-?}x${MV_COLS[$n]:-?}${hint}"
  done

  # ---- THE DELTA GATE, part 2: how many rows did each merge actually fold? ---
  # This is the acceptance gate for every merge_ms above it. A merge over an
  # empty delta reports a perfectly normal time and there is NOTHING in the
  # gateway's response or logs that distinguishes it — delta_files is a file
  # count, and mv_rows does not move because appended rows land in existing
  # groups. Only the delta part's own parquet footer settles it.
  if [ -n "$GATE_TABLES" ] && [ -s "$DELTA_PRE" ]; then
    echo ">> delta gate: rows folded in by each merge"
    delta_tool verdict --pre "$DELTA_PRE" --tables $GATE_TABLES > "$DELTA_POST" 2>&1 || true
    cat "$DELTA_POST" | sed 's/^/   /'
    for n in $FNAMES; do
      t="${MVTBL[$n]##*.}"; [ -n "$t" ] || continue
      read v r <<<"$("$PY3" -c "
import json,sys
try: d=json.load(open(sys.argv[1]))
except Exception: print(''); raise SystemExit
e=d.get(sys.argv[2]) or {}
print('%s %s'%(e.get('verdict','?'), e.get('delta_rows','?')))" "$DELTA_POST" "$t" 2>/dev/null)"
      DELTA_VERDICT[$n]="${v:-?}"; DELTA_ROWS[$n]="${r:-?}"
    done
  fi
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
# content: did the merge actually change what the MV serves?
# The base data.parquet ETag is NOT the whole story. In APPEND-MERGE mode the
# gateway deliberately does not rewrite data.parquet — it adds a delta part and
# reads base+parts with union_by_name (mv_append_merge.go). So an append merge
# that folded 50,000 rows still leaves the base ETag byte-identical, and reading
# the ETag alone reports "UNCHANGED" for exactly the merges that worked. Defer to
# the delta-row verdict whenever there is one; the ETag only settles the
# rebaseline/full-re-aggregation lane, which does rewrite the base.
cc(){ local n="$1"
  case "${DELTA_VERDICT[$n]:-}" in
    merged)      printf 'part+%s' "${DELTA_ROWS[$n]:-?}"; return;;
    EMPTY)       printf 'EMPTY-part'; return;;
  esac
  [ -z "${MV_HASH_NEW[$n]:-}" ] && { printf '?'; return; }
  [ -z "${MV_HASH_OLD[$n]:-}" ] && { printf 'n/a'; return; }
  [ "${MV_HASH_NEW[$n]}" = "${MV_HASH_OLD[$n]}" ] && printf 'UNCHANGED' || printf 'changed'; }
printf '%-10s %-14s %16s %11s %10s %8s %9s %-11s %10s %-12s\n' query fact 'mv_rows x cols' author_ms merge_ms mode incr_ms content delta_rows delta_verdict
printf '%-10s %-14s %16s %11s %10s %8s %9s %-9s %10s %-12s\n' ---------- -------------- ---------------- ----------- ---------- -------- --------- ----------- ---------- ------------
for n in "${NAMES[@]}"; do
  # author time = shape+materialize; when phase-1 reused a warm MV, author_ms is
  # blank — fall back to materialize_ms so the cold-build cost is still shown.
  au="${A_MS[$n]}"; [ -z "$au" -o "$au" = "0" ] && au="${M_MS[$n]:-?}"
  printf '%-10s %-14s %16s %11s %10s %8s %9s %-9s %10s %-12s\n' \
    "$n" "${FACT[$n]}" "${MV_ROWS[$n]:-?}x${MV_COLS[$n]:-?}" "$au" "${MERGE[$n]:-?}" "${MODE[$n]:-?}" "${I_QMS[$n]:-?}" "$(cc "$n")" \
    "${DELTA_ROWS[$n]:-?}" "${DELTA_VERDICT[$n]:-?}"
done
echo "=============================================================================="
echo "mode=incremental → delta-merged (merge_ms, reads |MV|+|delta|); fallback/no-delta"
echo "→ full re-author (author_ms, full fact scan). Merge is the O(MV) fast path."
echo ""
echo "delta_verdict is THE GATE on merge_ms — it is read off the delta part's own"
echo "parquet footer, the only place the truth exists (the gateway counts delta"
echo "FILES, never rows):"
echo "  merged       delta part had rows>0            -> merge_ms is a REAL number"
echo "  rebaselined  no part, MV content changed      -> full re-aggregation ran"
echo "  EMPTY        delta part had 0 rows            -> merge_ms MEASURED NOTHING"
echo "  UNCHANGED    no part, MV byte-identical       -> merge_ms MEASURED NOTHING"
echo "  NO-BASELINE  MV absent from the pre-snapshot     -> unproven, not a result"
echo "Do NOT report a merge_ms whose verdict is EMPTY or UNCHANGED as a result."

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
