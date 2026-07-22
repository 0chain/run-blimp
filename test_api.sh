#!/usr/bin/env bash
# test_api.sh — exercise every Blimp Prod-Query gateway API and write a results doc.
#
# Walks the full endpoint catalog from the Prod-Query API Reference (gateway :9000
# /admin/* + /optimizer/*, and the zus-cdc helper :9401 /cdc/*), calls each one,
# and writes API_RESULTS.md listing every API with its HTTP status + a short
# response snippet.
#
# SAFE BY DEFAULT: only read-only GETs, plan-only POSTs (query/explain,
# optimizer/advise), and "auth probes" (a deliberately-empty POST that proves the
# route is alive + the token is enforced, WITHOUT mutating anything) are run.
# Query execution and mutating/destructive calls are opt-in:
#   --run-query    also run one real POST /admin/query/run (executes a trivial query)
#   --destructive  also run mutating calls (mv/delete, mv/reap, source/*, cdc/*)
#
# Usage:
#   GATEWAY=<gateway> CLUSTER_ID=<cluster-id> ./test_api.sh
#   ./test_api.sh --gateway <gateway> --cluster-id <cluster-id> --run-query
#
# Env / flags:
#   GATEWAY      gateway host or ip (no scheme, no port). required.
#   CLUSTER_ID   cluster id; the admin token is zus-<CLUSTER_ID>. required.
#   PORT         gateway port (default 9000)
#   CDC_PORT     zus-cdc helper port (default 9401)
#   SCHEME       http|https (default http)
#   OUT          output doc path (default ./API_RESULTS.md)
#   TIMEOUT      per-call curl timeout seconds (default 20)
set -uo pipefail

PORT="${PORT:-9000}"; CDC_PORT="${CDC_PORT:-9401}"; SCHEME="${SCHEME:-http}"
OUT="${OUT:-./API_RESULTS.md}"; TIMEOUT="${TIMEOUT:-20}"
RUN_QUERY=0; DESTRUCTIVE=0

while [ $# -gt 0 ]; do
  case "$1" in
    --gateway) GATEWAY="$2"; shift 2;;
    --cluster-id) CLUSTER_ID="$2"; shift 2;;
    --port) PORT="$2"; shift 2;;
    --cdc-port) CDC_PORT="$2"; shift 2;;
    --run-query) RUN_QUERY=1; shift;;
    --destructive) DESTRUCTIVE=1; shift;;
    --out) OUT="$2"; shift 2;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done

: "${GATEWAY:?set GATEWAY (host/ip, no scheme/port)}"
: "${CLUSTER_ID:?set CLUSTER_ID (token is zus-<CLUSTER_ID>)}"
TOKEN="zus-${CLUSTER_ID}"
GW="${SCHEME}://${GATEWAY}:${PORT}"
CDC="${SCHEME}://${GATEWAY}:${CDC_PORT}"

command -v curl >/dev/null || { echo "curl required" >&2; exit 1; }
have_jq=0; command -v jq >/dev/null && have_jq=1

# rows collected as: GROUP\tMETHOD\tPATH\tMODE\tSTATUS\tSNIPPET
ROWS_FILE="$(mktemp)"
# per-run probe files — a fixed /tmp/.probe_body races when two runs overlap
# (observed 2026-07-17: 14 bogus ERRs from concurrent instances)
PROBE_BODY="$(mktemp)"; PROBE_ERR="$(mktemp)"
trap 'rm -f "$ROWS_FILE" "$PROBE_BODY" "$PROBE_ERR"' EXIT

# call GROUP METHOD PATH-OR-URL MODE [json-body]
#   MODE: read | plan | authprobe | exec | mutate  (label only; call is the same)
# Records status + a compact one-line snippet of the body.
call() {
  local group="$1" method="$2" path="$3" mode="$4" body="${5:-}"
  local url; case "$path" in http*) url="$path";; /cdc/*|/iceberg/*|/view*|/bench*|/snapshot*) url="${CDC}${path}";; *) url="${GW}${path}";; esac
  # exec-class calls run real queries / authoring — a cold gateway (fresh
  # recreate) can take >TIMEOUT on the first materialize; give them longer.
  local tmo="$TIMEOUT"; case "$mode" in exec|plan) tmo="${EXEC_TIMEOUT:-180}";; esac
  local args=(-sS -m "$tmo" -o "$PROBE_BODY" -w '%{http_code}' -X "$method"
              -H "Authorization: Bearer ${TOKEN}")
  if [ -n "$body" ]; then args+=(-H "Content-Type: application/json" -d "$body"); fi
  : > "$PROBE_BODY"; : > "$PROBE_ERR"
  local code; code="$(curl "${args[@]}" "$url" 2>"$PROBE_ERR")" || code="ERR"
  # a stream (SSE) that delivered data then hit -m is a PASS, not an error
  if [ "$code" = "ERR" ] && [ -s "$PROBE_BODY" ] && grep -q "timed out" "$PROBE_ERR"; then code="200-stream"; fi
  local snip; snip="$(tr -d '\r' < "$PROBE_BODY" | tr '\n' ' ' | sed 's/  */ /g' | cut -c1-160)"
  [ -z "$snip" ] && snip="$(cut -c1-120 "$PROBE_ERR" 2>/dev/null)"
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$group" "$method" "$path" "$mode" "$code" "${snip:-<empty>}" >> "$ROWS_FILE"
  printf '  %-4s %-34s -> %s\n' "$method" "$path" "$code" >&2
}

echo "Probing ${GW} (cdc ${CDC}) as ${TOKEN}  [run-query=${RUN_QUERY} destructive=${DESTRUCTIVE}]" >&2

# --- auth enforcement proof: same admin endpoint WITHOUT the token must 401 ---
noauth_code="$(curl -sS -m "$TIMEOUT" -o /dev/null -w '%{http_code}' "${GW}/admin/mv/list" 2>/dev/null || echo ERR)"
printf '%s\t%s\t%s\t%s\t%s\t%s\n' "Auth" "GET" "/admin/mv/list (no token)" "authprobe" "$noauth_code" "expect 401 — proves the admin token is enforced" >> "$ROWS_FILE"

# ---- context discovery: a real MV + a registered query make every read/plan
# ---- endpoint answer 200 instead of a validated 400 -------------------------------
MV_NS=""; MV_TABLE=""; REG_SQL=""
mvctx="$(curl -sS -m "$TIMEOUT" -H "Authorization: Bearer ${TOKEN}" "${GW}/admin/mv/list" 2>/dev/null)"
if [ -n "$mvctx" ]; then
  read -r MV_NS MV_TABLE <<EOF2
$(printf '%s' "$mvctx" | python3 -c 'import json,sys
try:
  a=json.load(sys.stdin)
  a=[m for m in a if str(m.get("table","")).startswith("mv_")] or a
  print(a[0].get("namespace","tpcds_mv"), a[0].get("table",""))
except Exception: print("","")' 2>/dev/null)
EOF2
fi
REG_SQL="$(curl -sS -m "$TIMEOUT" -H "Authorization: Bearer ${TOKEN}" "${GW}/admin/mv/registered_queries" 2>/dev/null | python3 -c 'import json,sys
try: print(json.load(sys.stdin)["queries"][0]["original_sql"].replace("\n"," "))
except Exception: print("")' 2>/dev/null)"
[ -z "$REG_SQL" ] && REG_SQL="SELECT 1 AS one"
jbody(){ python3 -c 'import json,sys; d=json.loads(sys.argv[1]); d[sys.argv[2]]=sys.argv[3]; print(json.dumps(d))' "$1" "$2" "$3"; }

# ---- 01 Query & optimizer (:9000) ----
# explain runs EXPLAIN on a real local engine (duckdb-stdin ships in the gateway
# image) — plan-only, no data touched.
call "Query & optimizer" POST "/admin/query/explain" plan \
  "$(python3 -c 'import json;print(json.dumps({"engine":"local","sql":"SELECT 1","engines_yaml":"engines:\n  - name: local\n    transport: duckdb-stdin\n    dialect: duckdb\n"}))')"
# /optimizer/advise + /optimizer/job removed 2026-07-17 (legacy heuristic
# advisor; the AI path was retired May 2026 and the real LLM surface is
# query/run + mv/llm_author). Not probed.
call "Query & optimizer" GET  "/optimizer/advice_log" read
if [ "$RUN_QUERY" = 1 ]; then
  call "Query & optimizer" POST "/admin/query/run" exec \
    "$(jbody '{"source":"'"${SOURCE:-customer}"'"}' original_sql "$REG_SQL")"
else
  call "Query & optimizer" POST "/admin/query/run" authprobe '{}'
fi
call "Query & optimizer" GET "/admin/query/suite" read   # GET: returns the seed suite list

# ---- 02 Materialized views (:9000) ----
call "Materialized views" GET "/admin/mv/list" read
call "Materialized views" GET "/admin/mv/registered_queries" read
call "Materialized views" GET "/admin/mv/source_tables" read
if [ -n "$MV_TABLE" ]; then
  call "Materialized views" GET "/admin/mv/data?namespace=${MV_NS}&table=${MV_TABLE}&limit=1" read
else
  call "Materialized views" GET "/admin/mv/data?namespace=tpcds_mv&limit=1" read  # 400: no MV to name yet
fi
call "Materialized views" GET "/admin/mv/stream" read   # SSE — just confirm it opens
call "Materialized views" POST "/admin/mv/resolve" plan \
  "$(jbody '{"mode":"match"}' original_sql "$REG_SQL")"   # read-only resolve → 200 (path hit|miss)
# a registered query's SQL hits the library fastpath (scheme_match, no LLM call,
# already materialized) — proves the authoring route end-to-end without mutating
call "Materialized views" POST "/admin/mv/llm_author" plan \
  "$(jbody '{"source":"'"${SOURCE:-customer}"'"}' original_sql "$REG_SQL")"
if [ "$DESTRUCTIVE" = 1 ]; then
  call "Materialized views" POST "/admin/mv/llm_authoring" mutate '{"enabled":false}'
  call "Materialized views" POST "/admin/mv/reap" mutate '{"policy":"age","dry_run":true}'
  # delete is only run destructively AND requires an explicit target; left as authprobe otherwise
  call "Materialized views" DELETE "/admin/mv/delete?namespace=__none__&table=__none__" mutate ''
else
  call "Materialized views" POST "/admin/mv/llm_authoring" authprobe '{}'
  call "Materialized views" POST "/admin/mv/reap" authprobe '{}'
  call "Materialized views" DELETE "/admin/mv/delete?namespace=__none__&table=__none__" authprobe ''
fi

# ---- 03 Source & CDC (:9000) ----
# The auto-iceberg registrar endpoints (/admin/iceberg/state|snapshot_at|scan)
# were removed 2026-07-17 — a BYO-catalog cluster IS the source of truth, so the
# self-cataloging registrar was dead customer surface. Not probed.
if [ "$DESTRUCTIVE" = 1 ]; then
  call "Source & CDC" POST "/admin/source/snapshot_changed" mutate '{"namespace":"tpcds","table":"store_returns","trigger":"probe"}'
  call "Source & CDC" POST "/admin/source/append" mutate \
    '{"namespace":"cdc_demo","table":"api_probe","rows":[{"k":"probe","v":1}]}'
else
  call "Source & CDC" POST "/admin/source/snapshot_changed" authprobe '{}'
  call "Source & CDC" POST "/admin/source/append" authprobe '{}'
fi
# /admin/source/ingest was removed from the gateway 2026-07-17 (implemented but
# caller-less; append + metadata registration cover the real paths) — not probed.

# ---- 04 CDC helper (:9401) ----
# /view is a browser page: auth is ?token= (query), not the Bearer header, and it
# needs ns+table to render.
if [ -n "$MV_TABLE" ]; then
  call "CDC helper" GET "/view?token=${TOKEN}&ns=${MV_NS}&table=${MV_TABLE}" read
else
  call "CDC helper" GET "/view?token=${TOKEN}" read
fi
call "CDC helper" GET "/view" authprobe ''   # no token → 403 proves the viewer is gated
# The bench API the panel's benchmark table polls is /bench/list (query-token
# auth like /view). The bare /bench + /snapshot HTML pages exist only on
# demo-source clusters — not probed here.
call "CDC helper" GET  "/bench/list?token=${TOKEN}" read
call "CDC helper" POST "/iceberg/added_files" read '{"table":"store_returns"}'  # read: no files listed → reports current snapshot only
if [ "$DESTRUCTIVE" = 1 ]; then
  call "CDC helper" POST "/cdc/wave" mutate '{"dry_run":true}'
  # NOTE: /cdc/append mutates even with an empty body — it appends demo rows to
  # cdc_demo.sales. Kept out of safe mode; only exercised with --destructive.
  call "CDC helper" POST "/cdc/append" mutate '{}'
fi

# ---- 05 Ops & billing (:9000) ----
call "Ops & billing" GET "/metrics" read
call "Ops & billing" GET "/admin/slow_log" read
call "Ops & billing" GET "/admin/sidecar/config" read
call "Ops & billing" GET "/admin/alloc/usage" read

# ---------- render API_RESULTS.md ----------
ok=0; auth=0; err=0; tot=0; unexpected=0
# expected non-2xx: 4xx probes/auth proofs, and the llm_author 500 when LLM is off
# (no ANTHROPIC_API_KEY). Anything else in 5xx/ERR is an UNEXPECTED failure.
classify() { case "$1" in
  2*) ok=$((ok+1));;
  400|401|403|404|405|409|422) auth=$((auth+1));;
  *) err=$((err+1));
     case "$2" in *auto_author*|*ANTHROPIC*) : ;; *) unexpected=$((unexpected+1));; esac;;
esac; }
while IFS=$'\t' read -r _ _ _ _ code snip; do tot=$((tot+1)); classify "$code" "$snip"; done < "$ROWS_FILE"

{
  echo "# Prod-Query API — probe results"
  echo
  echo "- **Gateway:** \`${GW}\`  ·  **CDC helper:** \`${CDC}\`"
  echo "- **Cluster:** \`${CLUSTER_ID}\`  ·  **Token:** \`Bearer ${TOKEN}\`"
  echo "- **Modes:** run-query=\`${RUN_QUERY}\` · destructive=\`${DESTRUCTIVE}\`"
  echo "- **Totals:** ${tot} endpoints — ${ok} × 2xx, ${auth} × 4xx (route alive / auth-enforced / bad-probe-body), ${err} × error/5xx/unreachable"
  echo
  echo "\`MODE\` legend: **read** = live read-only GET · **plan** = plan-only (no execute/mutation) · **exec** = query executed · **authprobe** = empty/bad body to confirm the route exists and the token is enforced (a 400/422 here = route alive) · **mutate** = state-changing (only with \`--destructive\`)."
  echo
  cur=""
  # stable group order
  for g in "Auth" "Query & optimizer" "Materialized views" "Source & CDC" "CDC helper" "Ops & billing"; do
    grep -F "$(printf '%s\t' "$g")" "$ROWS_FILE" >/dev/null 2>&1 || continue
    echo "## ${g}"
    echo
    echo "| Method | Path | Mode | Status | Response (truncated) |"
    echo "|---|---|---|---|---|"
    while IFS=$'\t' read -r grp method path mode code snip; do
      [ "$grp" = "$g" ] || continue
      # escape pipes in snippet
      snip="$(printf '%s' "$snip" | sed 's/|/\\|/g')"
      echo "| \`${method}\` | \`${path}\` | ${mode} | ${code} | ${snip} |"
    done < "$ROWS_FILE"
    echo
  done
  echo "---"
  echo "_Generated by \`test_api.sh\`. Re-run with \`--run-query\` to execute a real query, or \`--destructive\` to also exercise the mutating endpoints (mv reap/delete, source refresh, cdc wave)._"
} > "$OUT"

echo "Wrote $OUT  (${tot} endpoints: ${ok} ok / ${auth} 4xx / ${err} err; ${unexpected} UNEXPECTED)" >&2
exit $([ "$unexpected" -eq 0 ] && echo 0 || echo 1)
