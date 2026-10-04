#!/usr/bin/env bash
# bench_cdc.sh — CDC-friendly suite: author ALL MVs, then one append, then
# re-run ALL to measure the incremental delta-merge. Prints a summary table
# (author / materialize / cold-serve / incremental merge_ms / incremental query_ms
# / wave mode) in addition to what shows on the cluster panel.
#
# Every call is labeled (<name>:author / <name>:incr). The cold author is
# verified by the node (its row-hash); the merge calls pass skip_verify.
# --verify (phase 4) checks the tick's served answer against the original query.
#
# Env: GW CLUSTER_ID ICEBERG_URL WAREHOUSE [NAMESPACE=tpcds] [REGION=ap-south-1]
#      [CDC_ROWS=5000] [EVICT=0] [EVICT_FAMILY=0] [VERIFY=0] [CDC_DIM_GROWTH] [CDC_DIM_RATE]
set -u
: "${GW:?}" "${CLUSTER_ID:?}" "${ICEBERG_URL:?}" "${WAREHOUSE:?}"
NAMESPACE="${NAMESPACE:-tpcds}"; REGION="${REGION:-ap-south-1}"; CDC_ROWS="${CDC_ROWS:-5000}"
# One source per node: the external Iceberg/S3 dataset wired by `blimp --setup`
# (the gateway calls it "customer").
SOURCE="${SOURCE:-customer}"
# QAPI/TOKEN are overridable for a non-default gateway address; the bearer is
# the wiring's CLUSTER_TOKEN (the account fleet token).
[ -n "${TOKEN:-${CLUSTER_TOKEN:-}}" ] || { echo "FATAL: fleet token required (CLUSTER_TOKEN)"; exit 1; }
QAPI="${QAPI:-http://$GW:9000}"; TOKEN="${TOKEN:-$CLUSTER_TOKEN}"; HERE="$(cd "$(dirname "$0")" && pwd)"
# Scratch files for this run (no fixed /tmp names: concurrent runs and other
# users must not collide).
QT_ERR="$(mktemp -t qterr.XXXXXX)"; POOLS_JSON="$(mktemp -t cdcpools.XXXXXX)"
POOLS_LOG="$(mktemp -t cdcpools_log.XXXXXX)"; DELTA_ERR="$(mktemp -t mverr.XXXXXX)"
PY3="${BLIMP_PY:-$HOME/.blimp_venv/bin/python3}"; [ -x "$PY3" ] || PY3="$HOME/venv_ib/bin/python3"; [ -x "$PY3" ] || PY3=python3
# pool_python: the first interpreter whose duckdb imports (the bench venv, then
# the system python). When neither imports, the venv's duckdb is reinstalled once.
pool_python(){
  local p
  for p in "$PY3" python3; do "$p" -c "import duckdb" 2>/dev/null && { echo "$p"; return 0; }; done
  if [ -x "$(dirname "$PY3")/pip" ]; then
    "$(dirname "$PY3")/pip" install -q --force-reinstall --no-deps duckdb >/dev/null 2>&1 || true
    "$PY3" -c "import duckdb" 2>/dev/null && { echo "$PY3"; return 0; }
  fi
  return 1
}
J(){ python3 -c "import json,sys
try: print(json.load(sys.stdin).get('$1',''))
except: print('')"; }

# ---- The suite: QNRS (TPC-DS query numbers, from `blimp --query`) or SUITES
# ("fact:qnrs;fact:qnrs;…" — each fact gets its own author → append → tick
# cycle). Default: five single-fact store_sales queries that delta-merge.
# Unless the fact is given (CDC_TABLE, or an explicit SUITES), each query's fact
# is derived from its SQL + the catalog below (query_tables.py).
Q_DIR="${Q_DIR:-$HOME/tpcds_queries}"
DERIVE_FACT=0
if [ -n "${QNRS:-}" ]; then
  SUITES="${CDC_TABLE:-store_sales}:$QNRS"; [ -z "${CDC_TABLE:-}" ] && DERIVE_FACT=1
else
  [ -z "${SUITES:-}" ] && DERIVE_FACT=1
  SUITES="${SUITES:-store_sales:47 59 88 13 9}"
fi
declare -A SQL FACT QFILE QTABLES; NAMES=()
# ANY SQL, ANY DATASET (blimp --query --sql <file|dir>): SQL_FILES lists .sql
# files (a directory expands to its *.sql). The query's tables and its FACT are
# DERIVED from the SQL text + the catalog (query_tables.py: names from FROM/JOIN,
# existence from the catalog listing, fact = the referenced table with the most
# rows, the same rule the gateway uses) — never from a TPC-DS number.
if [ -n "${SQL_FILES:-}" ]; then
  SUITE_ARR=()
  for p in $SQL_FILES; do
    if [ -d "$p" ]; then for f in "$p"/*.sql; do [ -f "$f" ] && SQL_LIST="${SQL_LIST:-} $f"; done
    elif [ -f "$p" ]; then SQL_LIST="${SQL_LIST:-} $p"
    else echo "  skip $p: not a file or directory"; fi
  done
  qt_args=""; for f in $SQL_LIST; do qt_args="$qt_args --sql-file $f"; done
  QT_JSON=$("$PY3" "$HERE/query_tables.py" $qt_args --catalog "${ICEBERG_URL_LOCAL:-$ICEBERG_URL}" \
              ${ICEBERG_PREFIX:+--prefix "$ICEBERG_PREFIX"} --warehouse "$WAREHOUSE" --namespace "$NAMESPACE" 2>"$QT_ERR") \
    || { echo "FATAL: could not derive the queries' tables from the catalog: $(tail -1 "$QT_ERR")"; exit 1; }
  for f in $SQL_LIST; do
    n=$(basename "$f" .sql)
    line=$(printf '%s\n' "$QT_JSON" | python3 -c 'import json,sys
want=sys.argv[1]
for l in sys.stdin:
    d=json.loads(l)
    if d["file"]==want: print(d["fact"]+" "+" ".join(d["tables"])); break' "$f")
    fact="${line%% *}"; tabs="${line#* }"
    [ -n "$fact" ] || { echo "  skip $n: none of its tables exist in $NAMESPACE (refs: $("$PY3" "$HERE/query_tables.py" --sql-file "$f" --list-refs | python3 -c 'import json,sys;print(" ".join(json.load(sys.stdin)["refs"]))'))"; continue; }
    NAMES+=("$n"); SQL[$n]="$(cat "$f")"; FACT[$n]="$fact"; QFILE[$n]="$f"; QTABLES[$n]="$tabs"
    printf '%s\n' "${SUITE_ARR[@]}" | grep -qx "$fact:" || SUITE_ARR+=("$fact:")
    echo "  $n: tables [$tabs] fact=$fact"
  done
  [ ${#NAMES[@]} -gt 0 ] || { echo "FATAL: no usable SQL files in $SQL_FILES"; exit 1; }
else
IFS=';' read -ra SUITE_ARR <<< "$SUITES"
for su in "${SUITE_ARR[@]}"; do
  fact="${su%%:*}"
  for nr in ${su#*:}; do
    f="$Q_DIR/q$nr.sql"; [ -f "$f" ] || { echo "  skip q$nr: no $f"; continue; }
    NAMES+=("q$nr"); SQL[q$nr]="$(cat "$f")"; FACT[q$nr]="$fact"; QFILE[q$nr]="$f"
  done
done
[ ${#NAMES[@]} -gt 0 ] || { echo "FATAL: no query files in $Q_DIR (generate via duckdb tpcds extension)"; exit 1; }
# Derive each query's FACT (the referenced table with the most rows in the
# catalog) instead of labelling every query with the suite's default table.
if [ "$DERIVE_FACT" = 1 ]; then
  qt_args=""; for n in "${NAMES[@]}"; do qt_args="$qt_args --sql-file ${QFILE[$n]}"; done
  if QT_JSON=$("$PY3" "$HERE/query_tables.py" $qt_args --catalog "${ICEBERG_URL_LOCAL:-$ICEBERG_URL}" \
                ${ICEBERG_PREFIX:+--prefix "$ICEBERG_PREFIX"} --warehouse "$WAREHOUSE" --namespace "$NAMESPACE" 2>"$QT_ERR"); then
    SUITE_ARR=()
    for n in "${NAMES[@]}"; do
      fact=$(printf '%s\n' "$QT_JSON" | python3 -c 'import json,sys
want=sys.argv[1]
for l in sys.stdin:
    d=json.loads(l)
    if d["file"]==want: print(d["fact"]); break' "${QFILE[$n]}" 2>/dev/null)
      [ -n "$fact" ] && FACT[$n]="$fact"
      printf '%s\n' "${SUITE_ARR[@]}" | grep -qx "${FACT[$n]}:" || SUITE_ARR+=("${FACT[$n]}:")
    done
  else
    echo "  (could not derive each query's fact from the catalog — labelled ${CDC_TABLE:-store_sales}: $(tail -1 "$QT_ERR"))"
  fi
fi
fi
facts_of(){ printf '%s\n' "${SUITE_ARR[@]}" | cut -d: -f1 | sort -u; }
names_for_fact(){ local ft="$1" n; for n in "${NAMES[@]}"; do [ "${FACT[$n]}" = "$ft" ] && printf '%s ' "$n"; done; }

# per-run captured columns
declare -A A_MS M_MS V_MS S_MS I_QMS I_MERGE I_STATUS I_ROWS I_MD5 I_MD5R I_MVURL I_RESURL V_RESULT V_RESURL MERGE MODE MVTBL MV_ROWS MV_COLS MV_HASH_OLD MV_HASH_NEW DELTA_ROWS DELTA_VERDICT

run(){ # run <sql> <label> [author_phase]  -> echoes the JSON
  # EVERY call passes skip_verify — the production path. The author's own
  # row-hash still runs regardless, so an MV cannot bank unverified.
  # AUTHOR_TIMEOUT (default 2 h): a cold author at large scale can take a long
  # time, and cutting the request short cancels the build.
  curl -s -m "${AUTHOR_TIMEOUT:-7200}" "$QAPI/admin/query/run" -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "$(python3 -c 'import json,sys;print(json.dumps({"original_sql":sys.argv[1],"source":sys.argv[6],"label":sys.argv[2],"skip_verify":True}))' "$1" "$2" "${3:-0}" "" "${VERIFY:-0}" "$SOURCE" "${EVICT:-0}")"
}

# ---- MV content signature + THE DELTA GATE -----------------------------------
# A merge over an empty delta reports a normal merge_ms, and row counts do not
# move when appended rows land in existing groups. mv_delta_rows.py snapshots
# each MV's delta parts + data.parquet ETag before the append and counts the
# rows in the parts that appear after, so a timing that measured nothing is
# reported as such. MV_BUCKET is the MV namespace with '_' -> '-'.
MV_NAMESPACE="${MV_NAMESPACE:-tpcds_mv}"
MV_BUCKET="${MV_BUCKET:-${MV_NAMESPACE//_/-}}"
# The MV bucket lives with the GATEWAY, not with the source.
MV_S3_ENDPOINT="${MV_S3_ENDPOINT:-http://$GW:9000}"
MV_S3_KEY="${MV_S3_KEY:-${GW_AK:-${S3_KEY:-${AWS_ACCESS_KEY_ID:-}}}}"
MV_S3_SECRET="${MV_S3_SECRET:-${GW_SK:-${S3_SECRET:-${AWS_SECRET_ACCESS_KEY:-}}}}"
DELTA_TOOL="$HERE/mv_delta_rows.py"
DELTA_PRE="$(mktemp -t mvpre.XXXXXX)"; DELTA_POST="$(mktemp -t mvpost.XXXXXX)"
trap 'rm -f "$DELTA_PRE" "$DELTA_POST" "$QT_ERR" "$POOLS_JSON" "$POOLS_LOG" "$DELTA_ERR"' EXIT

delta_tool(){ # delta_tool <snapshot|verdict> <extra args...> -- <tables...>
  [ -x "$PY3" ] || return 1
  [ -f "$DELTA_TOOL" ] || return 1
  MV_BUCKET="$MV_BUCKET" MV_S3_ENDPOINT="$MV_S3_ENDPOINT" \
  MV_S3_KEY="$MV_S3_KEY" MV_S3_SECRET="$MV_S3_SECRET" \
    "$PY3" "$DELTA_TOOL" "$@" 2>>"$DELTA_ERR"; }   # stderr kept out of the JSON

mv_etag(){ # mv_etag <table> -> content signature of the materialized parquet
  [ -n "$MV_S3_KEY" ] || { printf ''; return; }
  AWS_ACCESS_KEY_ID="$MV_S3_KEY" AWS_SECRET_ACCESS_KEY="$MV_S3_SECRET" \
    aws s3api head-object --bucket "$MV_BUCKET" --key "$1/data.parquet" \
    --endpoint-url "$MV_S3_ENDPOINT" --region "${REGION:-us-east-1}" 2>/dev/null \
    | "$PY3" -c "import json,sys;print(json.load(sys.stdin).get('ETag','').strip('\"'))" 2>/dev/null; }

mv_dims(){ # mv_dims <table> -> "<rows> <cols>"; MV_DIMS_CMD is the node-side hook
  [ -n "${MV_DIMS_CMD:-}" ] && { $MV_DIMS_CMD "$1" 2>/dev/null; return; }
  # /admin/mv/list can fail on some deployments — hence the MV_DIMS_CMD hook.
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
# The only real cold state is an evicted MV (POST /admin/mv/evict, recipe kept).
# Two-tier answers regenerate from their chart MV the moment the answer is
# evicted, so evict-and-rematch until the matcher returns nothing.
# --evict-family: the gateway resolves the query's signature from its text and
# evicts every MV banked under it (chart, branches, answers), recipes kept.
evict_family(){ # evict_family <sql> <name>
  local ef
  ef=$(curl -s -m 300 "$QAPI/admin/mv/evict" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d "$(python3 -c 'import json,sys;print(json.dumps({"original_sql":sys.argv[1],"source":sys.argv[2],"cascade":True,"confirm":True,"force":True,"keep_recipe":True}))' "$1" "$SOURCE")")
  echo "   $2: evict family evicted=$(echo "$ef" | J evicted) kept_shared=$(echo "$ef" | J kept_shared) $(echo "$ef" | J error)"
}
evict_query(){ # evict_query <sql> <name>
  local rounds=0 busy=0 m t ns e ok rg e2 last=
  while :; do
    m=$(curl -s -m 600 "$QAPI/admin/query/run" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
      -d "$(python3 -c 'import json,sys;print(json.dumps({"original_sql":sys.argv[1],"source":sys.argv[3],"label":sys.argv[2]+":match","match_only":True,"skip_verify":True,"skip_passthrough":True}))' "$1" "$2" "$SOURCE")")
    t=$(echo "$m" | J mv_table); t="${t##*.}"; ns=$(echo "$m" | J mv_namespace)
    # BRANCH MVs: a query served by reassembling branch MVs names none of them
    # in mv_table. The probe's trace names them (phase names matched below);
    # evict every one.
    for b in $(echo "$m" | "$PY3" -c '
import json,re,sys
try: d=json.loads(sys.stdin.read() or "{}")
except Exception: d={}
seen=set()
for s in d.get("author_trace") or []:
    if s.get("phase") in ("reassemble","reassemble_partial","reassemble_branch_mv","reassemble_branch_sig","branch_match","reassemble_branch_own_bank"):
        for x in re.findall(r"\b(mv_[a-z0-9_]{8,})\b", str(s.get("detail") or "")):
            if x not in seen: seen.add(x); print(x)
' 2>/dev/null); do
      [ "$b" = "$t" ] && continue
      eb=$(curl -s -m 120 "$QAPI/admin/mv/evict" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
        -d "{\"namespace\":\"$MV_NAMESPACE\",\"table\":\"$b\",\"keep_recipe\":true,\"force\":true}")
      echo "   $2: evict branch $MV_NAMESPACE.$b evicted=$(echo "$eb" | J evicted) $(echo "$eb" | J error)"
    done
    [ -n "$t" ] || break
    # The SAME table matching right after it was evicted is its kept recipe
    # (data dropped, name kept), not data: the query is cold — stop here.
    [ "$t" = "$last" ] && { echo "   $2: $t matches only as its kept recipe (data evicted) — cold"; break; }
    e=$(curl -s -m 120 "$QAPI/admin/mv/evict" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
      -d "{\"namespace\":\"${ns:-$MV_NAMESPACE}\",\"table\":\"$t\",\"keep_recipe\":true,\"force\":true}")
    ok=$(echo "$e" | J evicted)
    echo "   $2: evict ${ns:-$MV_NAMESPACE}.$t evicted=$ok $(echo "$e" | J error)"
    # An ANSWER row regenerates from its chart on the next touch; the gateway
    # names the chart in regenerates_from — evict it too.
    rg=$(echo "$e" | J regenerates_from)
    if [ "$ok" = "True" ] && [ -n "$rg" ]; then
      e2=$(curl -s -m 120 "$QAPI/admin/mv/evict" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
        -d "{\"namespace\":\"${rg%%.*}\",\"table\":\"${rg#*.}\",\"keep_recipe\":true,\"force\":true}")
      echo "   $2: evict $rg (chart the answer regenerates from) evicted=$(echo "$e2" | J evicted) $(echo "$e2" | J error)"
    fi
    if [ "$ok" != "True" ]; then
      # mid-merge / mid-serve (the match probe itself wakes a stale MV's refresh):
      # wait for the flight to land, then retry — up to ~10 min, like the
      # internal harness. Retrying instantly just re-hits the same lock.
      busy=$((busy+1)); [ "$busy" -ge 30 ] && { echo "   WARN $2: $t stayed busy for $busy rounds — NOT evicted, phase 1 serves warm"; break; }
      sleep 20; continue
    fi
    last=$t
    rounds=$((rounds+1))
    [ "$rounds" -ge 8 ] && { echo "   WARN $2 still matches after $rounds evictions (two-tier answer regenerating from a companion the API cannot reach)"; break; }
  done
}

echo "== CDC bench: cluster=$CLUSTER_ID gw=$GW rows/append=$CDC_ROWS suites=[$SUITES] =="
echo "== delta gate: bucket=$MV_BUCKET endpoint=$MV_S3_ENDPOINT tool=$DELTA_TOOL =="

for ft in $(facts_of); do
  FNAMES=$(names_for_fact "$ft"); [ -n "$FNAMES" ] || continue
  echo "==== fact: $ft (${FNAMES% }) ===="
  if [ "${EVICT_FAMILY:-0}" = "1" ]; then
    echo ">> phase 0: evict family (every MV banked under the query, recipe kept)"
    for n in $FNAMES; do evict_family "${SQL[$n]}" "$n"; done
  elif [ "${EVICT:-0}" = "1" ]; then
    echo ">> phase 0: evict (cold state, recipe kept)"
    for n in $FNAMES; do evict_query "${SQL[$n]}" "$n"; done
  fi
  # drain_authors waits until the gateway reports no in-flight requests AND no
  # detached authors, so a tick never appends into a build still in progress.
  # AUTHOR_DRAIN_SEC=0 disables the wait entirely.
  drain_authors() {
    local maxs="${AUTHOR_DRAIN_SEC:-5400}" what="${1:-authors}"
    [ "$maxs" -gt 0 ] 2>/dev/null || return 0
    local t0 quiet act now el
    t0=$(date +%s); quiet=0
    while :; do
      act=$(curl -s -m 15 -H "Authorization: Bearer $TOKEN" "$QAPI/admin/query/active" 2>/dev/null \
            | "$PY3" -c 'import json,sys
try:
    d=json.load(sys.stdin)
    print(len(d.get("active") or []) + int(d.get("authors_in_flight") or 0))
except Exception: print(-1)' 2>/dev/null)
      [ -z "$act" ] && act=-1
      now=$(date +%s); el=$((now-t0))
      if [ "$act" = "0" ]; then
        quiet=$((quiet+1)); [ "$quiet" -ge 2 ] && { echo "   $what: drained in ${el}s (author work off the request)"; return 0; }
      else
        quiet=0
      fi
      if [ "$el" -ge "$maxs" ]; then
        echo "   WARN: $what still busy ($act) after ${el}s — continuing anyway"
        return 0
      fi
      # DRAIN_POLL_SEC (default 2).
      sleep "${DRAIN_POLL_SEC:-2}"
    done
  }

  echo ">> phase 1: serve/author all (evict=${EVICT:-0}; correctness is the author's row-hash, which always runs)"
  for n in $FNAMES; do
    R=$(run "${SQL[$n]}" "$n:author" 1)
    A_MS[$n]=$(echo "$R" | J author_ms); M_MS[$n]=$(echo "$R" | J materialize_ms)
    V_MS[$n]=$(echo "$R" | J verify_ms)
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
    # author INCLUDES materialize and verify (plan = author - materialize - verify);
    # cold_serve is the MV read alone now that the serve gate reuses the request's
    # own proof instead of re-running the original on base.
    echo "   $n: author=${A_MS[$n]:-?} materialize=${M_MS[$n]:-?} verify=${V_MS[$n]:-0} cold_serve=${S_MS[$n]:-?}ms mv=${MV_ROWS[$n]:-?}x${MV_COLS[$n]:-?} (${MVTBL[$n]:-none})"
    # The served result's identity; phase 3 (--tick) replaces it with the tick's.
    I_STATUS[$n]=$(echo "$R" | J status); I_ROWS[$n]=$(echo "$R" | J rows)
    I_MD5[$n]=$(echo "$R" | J md5); I_MD5R[$n]=$(echo "$R" | J md5_rounded)
    I_MVURL[$n]=$(echo "$R" | J mv_url); I_RESURL[$n]=$(echo "$R" | J result_url)
    # Let this query's author finish before touching the next one: the gateway
    # builds one MV at a time, so racing ahead only queues them on the same slot.
    drain_authors "$n's author"
  done
  # ---- TICK=1 (blimp --tick): append rows, then re-run → the delta merge ----
  # Without it the run stops after phase 1 (author / serve); --verify then
  # checks the phase-1 served result.
  if [ "${TICK:-0}" = 1 ]; then
  # ---- DRAIN THE DETACHED AUTHORS BEFORE ANYTHING TOUCHES THE SOURCE --------
  # Backstop for a refresh or build started late. AUTHOR_DRAIN_SEC=0 skips it.
  echo ">> draining detached authors before the tick"
  drain_authors "authors"
  # ---- THE DELTA GATE, part 1: snapshot every MV's parts BEFORE the append ---
  # Must be before phase 2: snapshot_changed can refresh an MV immediately, and
  # a part written then must count as new.
  GATE_TABLES=""
  for n in $FNAMES; do t="${MVTBL[$n]##*.}"; [ -n "$t" ] && GATE_TABLES="$GATE_TABLES $t"; done
  if [ -n "$GATE_TABLES" ]; then
    delta_tool snapshot --out "$DELTA_PRE" --tables $GATE_TABLES >/dev/null || \
      echo "   WARN: delta gate unavailable — merge_ms below is UNVERIFIED"
  fi
  # ---- phase 2: ONE PROPORTIONAL CDC TICK ACROSS ALL SIX FACTS --------------
  # `seed_tpcds.py --tick`: facts at roughly 4:2:1 store:catalog:web with returns
  # at ~10% of their parent (--ratios / --returns-ratio), in one process so
  # returns reference the sales rows just written. --catalog prefers
  # ICEBERG_URL_LOCAL (the seeder runs HERE; ICEBERG_URL is the cluster's address).
  # CDC_APPEND_TABLES forces a flat per-table append instead.
  APPEND_TABLES="${CDC_APPEND_TABLES:-}"
  # CDC_EXTRA_TABLES: the non-sales tables the tick also appends (default: all
  # 18, so every dimension's delta path is exercised; "" = facts only). New
  # dimension keys are max(existing)+1 and dimensions append BEFORE the facts.
  EXTRA_TABLES="${CDC_EXTRA_TABLES-inventory customer customer_address customer_demographics date_dim household_demographics item income_band promotion reason ship_mode store time_dim warehouse web_page web_site call_center catalog_page}"
  # QUERY-SCOPED EXTRAS (default; CDC_EXTRA_SCOPE=all appends every table): a
  # table no query of this run reads cannot reach any MV the run measures, yet
  # each costs an Iceberg commit.
  if [ -z "${CDC_EXTRA_TABLES+x}" ] && [ "${CDC_EXTRA_SCOPE:-query}" != "all" ]; then
    scoped=""
    for t in $EXTRA_TABLES; do
      for n in "${NAMES[@]}"; do
        if printf '%s' "${SQL[$n]}" | grep -qiw "$t"; then scoped="$scoped $t"; break; fi
      done
    done
    EXTRA_TABLES="${scoped# }"
  fi
  NOTIFY_TABLES="store_sales store_returns catalog_sales catalog_returns web_sales web_returns $EXTRA_TABLES"
  # With --sql the tables to notify are the ones the queries actually read (derived
  # above from the SQL + catalog), not a fixed list: snapshot_changed for each of
  # them after the append is what arms the watermark diff → merge → serve.
  if [ -n "${SQL_FILES:-}" ]; then
    NOTIFY_TABLES=""; for n in "${NAMES[@]}"; do NOTIFY_TABLES="$NOTIFY_TABLES ${QTABLES[$n]}"; done
  fi
  SEED_CREDS_AK="${S3_KEY:-${AWS_ACCESS_KEY_ID:-}}"; SEED_CREDS_SK="${S3_SECRET:-${AWS_SECRET_ACCESS_KEY:-}}"
  # Capture the seeder's full output and exit status: a failed append must be
  # loud, not indistinguishable from "this shape has no delta".
  if [ -z "$APPEND_TABLES" ]; then
    echo ">> phase 2: CDC tick (base=$CDC_ROWS rows, ratios=${CDC_RATIOS:-4:2:1}, returns=${CDC_RETURNS_RATIO:-0.1}, extra=[${EXTRA_TABLES:-none}]) + snapshot_changed"
    # QUERY-AWARE TICK (default on; CDC_TARGET_POOLS=0 restores uniform draws):
    # derive key/date pools from this run's queries' dimension predicates
    # (query_pools.py) so the appended rows land inside the filters.
    POOLS_ARG=""
    if [ "${CDC_TARGET_POOLS:-1}" != "0" ]; then
      pf=""; for n in "${NAMES[@]}"; do [ -f "${QFILE[$n]}" ] && pf="$pf --sql-file ${QFILE[$n]}"; done   # every loaded query, --sql or TPC-DS
      if [ -z "$pf" ]; then
        echo "   pools: no query SQL loaded — UNIFORM draws; any MV that bakes a date filter merges 0 rows"
      elif ! POOLPY=$(pool_python) || [ -z "$POOLPY" ]; then
        echo "   pools: no python with a working duckdb module ($PY3, python3) — UNIFORM draws; any MV that bakes a date filter merges 0 rows"
      elif "$POOLPY" "$HERE/query_pools.py" $pf --catalog "${ICEBERG_URL_LOCAL:-$ICEBERG_URL}" --warehouse "$WAREHOUSE" --namespace "$NAMESPACE" \
             ${S3_ENDPOINT:+--s3-endpoint "$S3_ENDPOINT"} --out "$POOLS_JSON" 2>"$POOLS_LOG"; then
        POOLS_ARG="--key-pools $POOLS_JSON"
        grep -E '^pools:|-> ' "$POOLS_LOG" | sed "s/^/   pools: /"
      else
        echo "   pools: derivation failed (UNIFORM draws) — $(tail -1 "$POOLS_LOG")"
      fi
    fi
    seed_out=$(AWS_ACCESS_KEY_ID="$SEED_CREDS_AK" AWS_SECRET_ACCESS_KEY="$SEED_CREDS_SK" \
      "$PY3" "$HERE/seed_tpcds.py" --catalog "${ICEBERG_URL_LOCAL:-$ICEBERG_URL}" --warehouse "$WAREHOUSE" \
      --namespace "$NAMESPACE" --tick --rows "$CDC_ROWS" --s3-region "$REGION" $POOLS_ARG \
      ${EXTRA_TABLES:+--extra-tables "$EXTRA_TABLES"} \
      ${CDC_RATIOS:+--ratios "$CDC_RATIOS"} ${CDC_DIM_GROWTH:+--dim-growth "$CDC_DIM_GROWTH"} \
      ${CDC_DIM_RATE:+--dim-rate "$CDC_DIM_RATE"} \
      --returns-ratio "${CDC_RETURNS_RATIO:-0.1}" \
      ${CDC_YEARS:+--years "$CDC_YEARS"} ${CDC_STREAM_DAYS:+--stream-days "$CDC_STREAM_DAYS"} \
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
        ${CDC_YEARS:+--years "$CDC_YEARS"} ${CDC_STREAM_DAYS:+--stream-days "$CDC_STREAM_DAYS"} \
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
    _p3=$(date +%s.%N)
    R=$(run "${SQL[$n]}" "$n:incr"); I_QMS[$n]=$(echo "$R" | J query_ms)
    _p3r=$(date +%s.%N)
    # Lazy CDC model: snapshot_changed only MARKS the MV stale; the delta-merge
    # happens ON this query and is reported inline as merge_ms.
    I_MERGE[$n]=$(echo "$R" | J merge_ms)
    I_STATUS[$n]=$(echo "$R" | J status); I_ROWS[$n]=$(echo "$R" | J rows)
    I_MD5[$n]=$(echo "$R" | J md5); I_MD5R[$n]=$(echo "$R" | J md5_rounded)
    I_MVURL[$n]=$(echo "$R" | J mv_url); I_RESURL[$n]=$(echo "$R" | J result_url)
    # Take the MV's dimensions AND content hash after the merge (phase 1 only
    # reports dimensions on a cold author).
    t="${MVTBL[$n]##*.}"
    if [ -n "$t" ]; then
      # The merge response's own mv_rows/mv_cols first; /admin/mv/list's
      # row_count can be 0 for a merged MV.
      mr=$(echo "$R" | J mv_rows); mc=$(echo "$R" | J mv_cols)
      case "$mr" in ''|0|None|null) read mr mc2 <<<"$(mv_dims "$t")"; mc="${mc:-$mc2}";; esac
      case "$mr" in ''|0|None|null) ;; *) MV_ROWS[$n]="$mr";; esac
      case "$mc" in ''|0|None|null) ;; *) MV_COLS[$n]="$mc";; esac
      MV_HASH_NEW[$n]=$(mv_etag "$t")
    fi
    echo "   $n: phase-3 wall: request $(awk "BEGIN{printf \"%.1f\", $_p3r-$_p3}")s, dims+etag $(awk "BEGIN{printf \"%.1f\", $(date +%s.%N)-$_p3r}")s"
    mh="${MV_HASH_NEW[$n]:-}"
    hint=""
    if [ -n "$mh" ] && [ -n "${MV_HASH_OLD[$n]:-}" ]; then
      # base-parquet ETag only; an append merge leaves it untouched by design,
      # so this is informational — the delta gate below is what decides.
      if [ "$mh" = "${MV_HASH_OLD[$n]}" ]; then hint=" base=untouched"; else hint=" base=rewritten"; fi
    fi
    # THE TICK'S RESULT IDENTITY: status, rows and the result md5 the gateway
    # computed, so the served tick can be compared with the original query.
    echo "   $n: incr_query=${I_QMS[$n]:-?}ms merge=${I_MERGE[$n]:-–}ms mv=${MV_ROWS[$n]:-?}x${MV_COLS[$n]:-?}${hint} status=$(echo "$R" | J status) rows=$(echo "$R" | J rows) md5=$(echo "$R" | J md5) md5r=$(echo "$R" | J md5_rounded)"
    # THE TICK, one format for every query: the post-append request's own phase
    # totals (the parts sum to its wall time) and the MV's current size.
    echo "$R" | "$PY3" -c '
import json,sys
n=sys.argv[1]
try: d=json.loads(sys.stdin.read() or "{}")
except Exception: d={}
p=d.get("phases") or {}
m=int(p.get("merge_ms") or 0); s=int(p.get("serve_ms") or 0)
b=int(p.get("build_ms") or 0); a=int(p.get("author_ms") or 0); t=int(p.get("total_ms") or 0)
# THE TICK IS THE REQUEST WALL TIME (a rebuild inside the request counts too).
tick=max(t, m+s)
rows=d.get("mv_rows") or sys.argv[2] or "?"; cols=d.get("mv_cols") or sys.argv[3] or "?"
tbl=(d.get("mv_table") or "").split(".")[-1] or "none"
if not p: print("   %s: tick: ? (no phase totals in the response — gateway predates them)" % n)
else: print("   %s: tick: %.2f seconds  merge: %d ms, serve: %d ms, build: %d ms, author: %d ms, other: %d ms  MV: %s rows x %s cols (%s)" % (n,tick/1000.0,m,s,b,a,max(0,tick-m-s-b-a),rows,cols,tbl))
' "$n" "${MV_ROWS[$n]:-}" "${MV_COLS[$n]:-}"
  done

  # ---- THE DELTA GATE, part 2: how many rows did each merge actually fold? ---
  # The acceptance gate for every merge_ms above it: read off the delta part's
  # own parquet footer.
  if [ -n "$GATE_TABLES" ] && [ -s "$DELTA_PRE" ]; then
    echo ">> delta gate: rows folded in by each merge"
    : > "$DELTA_ERR"
    delta_tool verdict --pre "$DELTA_PRE" --tables $GATE_TABLES > "$DELTA_POST" || true
    sed 's/^/   /' "$DELTA_POST"
    [ -s "$DELTA_ERR" ] && sed 's/^/   ! /' "$DELTA_ERR"
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
  fi  # TICK
  # ---- phase 4 (--verify): the tick's served result vs the ORIGINAL query ---
  # One plain comparison: run each query's original SQL over base (no_mv, same
  # data — nothing is appended between the tick above and this run) and compare
  # the gateway's result md5 with the tick's. Equal md5 = same rows. md5r
  # (float-rounded) equal = same rows up to float summation order. Anything
  # else is a wrong answer served by the tick. This replaces the old phase 4,
  # which re-verified the merged MV (not the served answer) against source.
  # Cost: one original-query run per query; capped by VERIFY_CAP_S.
  if [ "${VERIFY:-0}" = 1 ]; then
    echo ">> phase 4: verify — served result vs original query over base"
    for n in $FNAMES; do
      B=$(curl -s -m "${VERIFY_CAP_S:-3600}" "$QAPI/admin/query/run" -H "Authorization: Bearer $TOKEN" \
        -H "Content-Type: application/json" \
        -d "$(python3 -c 'import json,sys;print(json.dumps({"original_sql":sys.argv[1],"source":sys.argv[2],"label":sys.argv[3],"no_mv":True,"persist_result":True}))' "${SQL[$n]}" "$SOURCE" "$n:verify")")
      bst=$(echo "$B" | J status); brows=$(echo "$B" | J rows); bmd5=$(echo "$B" | J md5); bmd5r=$(echo "$B" | J md5_rounded)
      tmd5="${I_MD5[$n]:-}"; [ "$tmd5" = null ] && tmd5=""; [ "$bmd5" = null ] && bmd5=""
      if [ -z "$tmd5" ] || [ -z "$bmd5" ]; then v="UNCHECKED"
      elif [ "$tmd5" = "$bmd5" ]; then v="MATCH"
      elif [ -n "${I_MD5R[$n]:-}" ] && [ "${I_MD5R[$n]}" != null ] && [ "${I_MD5R[$n]}" = "$bmd5r" ]; then v="MATCH(float)"
      else v="MISMATCH"; fi
      V_RESULT[$n]="$v"; V_RESURL[$n]=$(echo "$B" | J result_url)
      echo "   $n: verify: $v tick(status=${I_STATUS[$n]:-?} rows=${I_ROWS[$n]:-?} md5=${tmd5:-none}) original(status=${bst:-?} rows=${brows:-?} md5=${bmd5:-none})"
    done
  fi
done
sleep 4

# ---- without --tick: the served result only (no append, no merge) ------------
if [ "${TICK:-0}" != 1 ]; then
  echo ""
  echo "=================================== RESULTS ==================================="
  printf '%-10s %-14s %16s %11s %10s %10s %-12s\n' query fact 'mv_rows x cols' author_ms verify_ms serve_ms verify
  for n in "${NAMES[@]}"; do
    printf '%-10s %-14s %16s %11s %10s %10s %-12s\n' "$n" "${FACT[$n]}" "${MV_ROWS[$n]:-?}x${MV_COLS[$n]:-?}" \
      "${A_MS[$n]:-?}" "${V_MS[$n]:-0}" "${S_MS[$n]:-?}" "${V_RESULT[$n]:-(no --verify)}"
  done
  for n in "${NAMES[@]}"; do
    u() { [ -n "$1" ] && [ "$1" != null ] && printf '%s' "$1" || printf '%s' "$2"; }
    echo "  $n: result: status=${I_STATUS[$n]:-?} rows=${I_ROWS[$n]:-?} md5=${I_MD5[$n]:-?}  verify=${V_RESULT[$n]:-(no --verify)}"
    echo "      mv:     $(u "${I_MVURL[$n]:-}" "none (served from base)")"
    [ -n "${V_RESULT[$n]:-}" ] && echo "      base:   $(u "${V_RESURL[$n]:-}" "not persisted (gateway without persist_result)")"
  done
  echo "DONE"; exit 0
fi
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
echo ""
echo "============================== CDC CONTRIBUTIONS =============================="
# content: did the merge actually change what the MV serves? An append merge
# adds a delta part and leaves data.parquet untouched, so defer to the delta-row
# verdict whenever there is one; the ETag only settles a full re-aggregation.
cc(){ local n="$1"
  case "${DELTA_VERDICT[$n]:-}" in
    merged)      printf 'part+%s' "${DELTA_ROWS[$n]:-?}"; return;;
    EMPTY)       printf 'EMPTY-part'; return;;
  esac
  [ -z "${MV_HASH_NEW[$n]:-}" ] && { printf '?'; return; }
  [ -z "${MV_HASH_OLD[$n]:-}" ] && { printf 'n/a'; return; }
  [ "${MV_HASH_NEW[$n]}" = "${MV_HASH_OLD[$n]}" ] && printf 'UNCHANGED' || printf 'changed'; }
printf '%-10s %-14s %16s %11s %10s %8s %9s %-11s %10s %-12s %-12s\n' query fact 'mv_rows x cols' author_ms merge_ms mode incr_ms content delta_rows delta_verdict verify
printf '%-10s %-14s %16s %11s %10s %8s %9s %-9s %10s %-12s %-12s\n' ---------- -------------- ---------------- ----------- ---------- -------- --------- ----------- ---------- ------------ ------------
for n in "${NAMES[@]}"; do
  # author time = shape+materialize; when phase-1 reused a warm MV, author_ms is
  # blank — fall back to materialize_ms so the cold-build cost is still shown.
  au="${A_MS[$n]}"; [ -z "$au" -o "$au" = "0" ] && au="${M_MS[$n]:-?}"
  printf '%-10s %-14s %16s %11s %10s %8s %9s %-9s %10s %-12s %-12s\n' \
    "$n" "${FACT[$n]}" "${MV_ROWS[$n]:-?}x${MV_COLS[$n]:-?}" "$au" "${MERGE[$n]:-?}" "${MODE[$n]:-?}" "${I_QMS[$n]:-?}" "$(cc "$n")" \
    "${DELTA_ROWS[$n]:-?}" "${DELTA_VERDICT[$n]:-?}" "${V_RESULT[$n]:-(no --verify)}"
done
# The tick's MV and result as the node hosts them (the same viewer pages the
# node panel links: mv_url / result_url of the tick's /admin/query/run).
for n in "${NAMES[@]}"; do
  u() { [ -n "$1" ] && [ "$1" != null ] && printf '%s' "$1" || printf '%s' "$2"; }
  echo "  $n: tick result: status=${I_STATUS[$n]:-?} rows=${I_ROWS[$n]:-?} md5=${I_MD5[$n]:-?}  verify=${V_RESULT[$n]:-(no --verify)}"
  echo "      mv:     $(u "${I_MVURL[$n]:-}" "none (served from base)")"
  echo "      result: $(u "${I_RESURL[$n]:-}" "not persisted for this run")"
  [ -n "${V_RESULT[$n]:-}" ] && echo "      base:   $(u "${V_RESURL[$n]:-}" "not persisted (gateway without persist_result)")"
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
