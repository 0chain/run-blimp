#!/usr/bin/env bash
# run_e2e.sh — ONE COMMAND, zero-intervention, timed end-to-end validation of the
# whole kit against a fresh cluster: launch client box → wire creds → register
# sf1000 → hook up the cluster → cache suite → flow-panel suite → query suite
# (incl. real CDC incremental) → API suite. Prints a per-phase wall-clock table
# and exits non-zero if any phase fails.
#
# Run from your Mac/laptop (awscli + the KEY_NAME .pem in ~/.ssh):
#   CLUSTER_ID=<id> REGION=ap-south-1 \
#   ORIGIN_BUCKET=blimp-tpcds1000-aps1 WAREHOUSE=s3://blimp-tpcds1000-aps1/wh_e2e \
#   KEY_NAME=zus-prodtest-iceberg-aps1-key ./run_e2e.sh
#
# Split runs: PART=storage runs only the storage/cache suite; PART=query runs only
# the query-optimizer + MV + CDC suites (needs register+hookup); PART=both (default)
# runs everything. e.g.  PART=query RUN_CDC=1 ./run_e2e.sh   or   PART=storage ./run_e2e.sh
#
# CDC CONTRIBUTIONS TABLE (zero-touch): a single command on a FRESH cluster stands
# up the box + WAL catalog, registers sf1000, hooks up the gateway, then authors
# the 12 store_sales MVs cold and delta-merges them against a live append —
# bench_cdc prints the query|fact|mv_rows|author|merge table at the end:
#   PART=query RUN_CDC=1 CLUSTER_ID=<id> REGION=<r> \
#     ORIGIN_BUCKET=<b> WAREHOUSE=s3://<b>/wh_e2e KEY_NAME=<key> ./run_e2e.sh
# (RUN_CDC is opt-in because phase-1 cold authoring is ~3-6 min/MV at sf1000.)
#
# Optional: INSTANCE_TYPE (default c6in.4xlarge for headline cache throughput; the
#           correctness suite has NO client-side Spark now, so INSTANCE_TYPE=c6in.xlarge
#           works for a lighter functional run — cache MB/s just becomes client-bound),
#           NAMESPACE (tpcds), SKIP_MLPERF=1.
# Spark row-hash verification is a SEPARATE, standalone step (spark_verify.sh) — it is
# deliberately not wired into this e2e runner; ship + run it by hand on a big box.
set -uo pipefail
: "${CLUSTER_ID:?}" "${REGION:?}" "${ORIGIN_BUCKET:?}" "${WAREHOUSE:?}" "${KEY_NAME:?}"
export INSTANCE_TYPE="${INSTANCE_TYPE:-c6in.4xlarge}" NAMESPACE="${NAMESPACE:-tpcds}"
PART="${PART:-both}"   # storage | query | both — run the storage suite, the query suite, or both
want(){ [ "$PART" = both ] || [ "$PART" = "$1" ]; }
HERE="$(cd "$(dirname "$0")" && pwd)"
PEM="$HOME/.ssh/${KEY_NAME}.pem"
LOG="${LOG:-/tmp/e2e_${CLUSTER_ID}.log}"
declare -a PH_NAME PH_SECS PH_RC
say(){ printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
phase(){ # phase <name> <cmd...>
  local name="$1"; shift
  say "$name"; local t0=$(date +%s)
  "$@" >>"$LOG" 2>&1; local rc=$?
  local dt=$(( $(date +%s) - t0 ))
  PH_NAME+=("$name"); PH_SECS+=("$dt"); PH_RC+=("$rc")
  printf '   %s: %ss rc=%s\n' "$name" "$dt" "$rc"
  return $rc
}
bx(){ ssh -o StrictHostKeyChecking=no -o ConnectTimeout=15 -i "$PEM" ubuntu@"$BOX" "$@"; }

T_START=$(date +%s)
echo "e2e log: $LOG"

# ---- 0. API suite in PARALLEL with setup — it needs only the gateway's public
# ---- IP + cluster id (no client box, no catalog), so it runs from here while
# ---- the box launches. Joined + reported after the box suites finish.
GWPUB=$(aws ec2 describe-instances --region "$REGION" --filters "Name=tag:Name,Values=cluster-${CLUSTER_ID}-zs3server" "Name=instance-state-name,Values=running" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
API_RC_FILE=$(mktemp); API_T0=$(date +%s)
if want query && [ -n "$GWPUB" ] && [ "$GWPUB" != "None" ]; then
  say "test_api (parallel with setup, gw $GWPUB)"
  ( env GATEWAY="$GWPUB" CLUSTER_ID="$CLUSTER_ID" SOURCE=customer OUT=/tmp/API_RESULTS_${CLUSTER_ID}.md \
      bash "$HERE/test_api.sh" --run-query >>"$LOG" 2>&1; echo $? > "$API_RC_FILE" ) &
  API_PID=$!
else
  echo "   WARN: no public gateway IP — api suite will be skipped"; API_PID=""
fi

# ---- 1. client box (setup_tests: box + tools + catalog + VPCE + IAM) --------------
phase "setup (box+catalog+IAM+VPCE)" env CLUSTER_ID="$CLUSTER_ID" REGION="$REGION" \
  WAREHOUSE="$WAREHOUSE" KEY_NAME="$KEY_NAME" NAMESPACE="$NAMESPACE" \
  INSTANCE_TYPE="$INSTANCE_TYPE" "$HERE/setup_tests.sh" || exit 1
BOX=$(grep -Eo 'ssh -i [^ ]+ ubuntu@[0-9.]+' "$LOG" | tail -1 | awk -F@ '{print $2}')
BOXPRIV=$(grep -Eo 'private [0-9.]+' "$LOG" | tail -1 | awk '{print $2}')
GWPRIV=$(grep -Eo 'gateway_priv=[0-9.]+' "$LOG" | tail -1 | cut -d= -f2)
GWIID=$(aws ec2 describe-instances --region "$REGION" --filters "Name=tag:Name,Values=cluster-${CLUSTER_ID}-zs3server" "Name=instance-state-name,Values=running" --query 'Reservations[0].Instances[0].InstanceId' --output text)
echo "   box=$BOX (priv $BOXPRIV) gw=$GWPRIV ($GWIID)"
[ -n "$BOX" ] && [ -n "$GWPRIV" ] || { echo "FATAL: could not parse box/gateway"; exit 1; }

# ---- 2. gateway S3 creds via SSM (no manual step) ---------------------------------
fetch_creds(){
  local out; out=$(aws ssm send-command --region "$REGION" --instance-ids "$GWIID" \
    --document-name AWS-RunShellScript \
    --parameters 'commands=["docker inspect minioserver --format \"{{range .Config.Env}}{{println .}}{{end}}\" | grep -E \"^MINIO_ROOT_(USER|PASSWORD)=\""]' \
    --query 'Command.CommandId' --output text) || return 1
  sleep 8
  CREDS=$(aws ssm get-command-invocation --region "$REGION" --command-id "$out" --instance-id "$GWIID" --query 'StandardOutputContent' --output text)
  GW_AK=$(echo "$CREDS" | grep MINIO_ROOT_USER | cut -d= -f2)
  GW_SK=$(echo "$CREDS" | grep MINIO_ROOT_PASSWORD | cut -d= -f2)
  [ -n "$GW_AK" ] && [ -n "$GW_SK" ]
}
phase "gateway creds (SSM)" fetch_creds || exit 1

# ---- 3. ship the kit to the box ---------------------------------------------------
ship(){ scp -o StrictHostKeyChecking=no -i "$PEM" \
  "$HERE"/register_tpcds_tables.py "$HERE"/seed_tpcds.py "$HERE"/upsert_tpcds.py "$HERE"/gen_incr_delta.py \
  "$HERE"/test_cache.sh "$HERE"/test_query.sh "$HERE"/test_api.sh "$HERE"/test_flow_panel.sh \
  "$HERE"/verify_panel.sh "$HERE"/verify_result_freshness.sh "$HERE"/test_fingerprint.sh \
  "$HERE"/bench_incremental.sh "$HERE"/bench_upsert.sh "$HERE"/bench_cdc.sh "$HERE"/bench_tpcds99.sh \
  "$HERE"/run_cluster.sh "$HERE"/run_router.sh ubuntu@"$BOX":~/ ; }
# NOTE: spark_verify.sh is intentionally NOT part of e2e — independent cross-engine
# correctness verification is a SEPARATE step (ship + run it by hand when desired).
phase "ship kit" ship || exit 1

# ---- 4. register the 24 sf1000 tables (metadata-only) -----------------------------
reg(){ bx "python3 -m venv ~/venv_ib 2>/dev/null; ~/venv_ib/bin/pip install -q 'pyiceberg[s3fs]==0.9.1' 'pyarrow>=18' s3fs boto3 && \
  ~/venv_ib/bin/python3 ~/register_tpcds_tables.py --catalog http://$BOXPRIV:8181 \
  --warehouse $WAREHOUSE --source-bucket $ORIGIN_BUCKET --region $REGION --namespace $NAMESPACE"; }
if want query; then phase "register 24 tables" reg || exit 1; fi

# ---- 5. hook the cluster to the source (compose replace + router repoint, via SSM) -
hook(){
  local b64; b64=$(base64 < "$HERE/hookup_cluster_source.sh" | tr -d '\n')
  printf '{"commands":["echo %s | base64 -d > /tmp/hookup.sh","ICEBERG_URL=http://%s:8181 WAREHOUSE=%s NAMESPACE=%s ORIGIN_BUCKET=%s S3_REGION=%s bash /tmp/hookup.sh"]}' \
    "$b64" "$BOXPRIV" "$WAREHOUSE" "$NAMESPACE" "$ORIGIN_BUCKET" "$REGION" > /tmp/.e2e_hook.json
  local cid; cid=$(aws ssm send-command --region "$REGION" --instance-ids "$GWIID" --document-name AWS-RunShellScript \
    --parameters file:///tmp/.e2e_hook.json --query 'Command.CommandId' --output text) || return 1
  for i in $(seq 1 30); do sleep 10
    local st; st=$(aws ssm get-command-invocation --region "$REGION" --command-id "$cid" --instance-id "$GWIID" --query 'Status' --output text 2>/dev/null)
    [ "$st" = "Success" ] && break; [ "$st" = "Failed" ] && { aws ssm get-command-invocation --region "$REGION" --command-id "$cid" --instance-id "$GWIID" --query 'StandardOutputContent' --output text; return 1; }
  done
  aws ssm get-command-invocation --region "$REGION" --command-id "$cid" --instance-id "$GWIID" --query 'StandardOutputContent' --output text | grep -q "DONE"
}
if want query; then phase "hookup (gateway+router)" hook || exit 1; fi

# ---- 6-9. the four suites, sequentially on the box --------------------------------
ENVV="GW=$GWPRIV GW_AK=$GW_AK GW_SK=$GW_SK ORIGIN_BUCKET=$ORIGIN_BUCKET REGION=$REGION \
CLUSTER_ID=$CLUSTER_ID SOURCE=customer ICEBERG_URL=http://$BOXPRIV:8181 NAMESPACE=$NAMESPACE WAREHOUSE=$WAREHOUSE"
[ "${SKIP_MLPERF:-0}" = "1" ] && ENVV="$ENVV MLPERF_SKIP=1"
want storage && phase "test_cache" bx "env $ENVV bash ~/test_cache.sh"
if want query; then
phase "test_flow_panel" bx "env $ENVV TABLE=customer_address bash ~/test_flow_panel.sh"
phase "test_query"      bx "env $ENVV bash ~/test_query.sh"
# the standalone correctness/robustness suites — run every time so nothing is
# missed (all quick; each gates on its own exit code).
phase "verify_panel"           bx "env GATEWAY=$GWPRIV CLUSTER_ID=$CLUSTER_ID bash ~/verify_panel.sh"
phase "verify_result_freshness" bx "env $ENVV bash ~/verify_result_freshness.sh"
phase "test_fingerprint"       bx "env GATEWAY=$GWPRIV CLUSTER_ID=$CLUSTER_ID N=30 bash ~/test_fingerprint.sh"
fi
# benchmarks are long (bench_incremental ~30m, tpcds99 hours) — opt in with
# RUN_BENCH=1 / RUN_TPCDS99=1 so the default E2E stays ~20-25 min.
want query && [ "${RUN_BENCH:-0}" = "1" ] && phase "bench_incremental" bx "env $ENVV ITERS=5 AUTHOR_ITERS=0 bash ~/bench_incremental.sh"
want query && [ "${RUN_CDC:-0}" = "1" ] && phase "bench_cdc" bx "env $ENVV bash ~/bench_cdc.sh"
want query && [ "${RUN_TPCDS99:-0}" = "1" ] && phase "bench_tpcds99" bx "env $ENVV TIMEOUT_S=200 bash ~/bench_tpcds99.sh"
# join the parallel API suite (started before setup)
if [ -n "${API_PID:-}" ]; then
  wait "$API_PID" 2>/dev/null
  API_RC=$(cat "$API_RC_FILE" 2>/dev/null); API_RC="${API_RC:-1}"
  PH_NAME+=("test_api (parallel)"); PH_SECS+=("$(( $(date +%s) - API_T0 ))(wall)"); PH_RC+=("$API_RC")
  printf '   %s: rc=%s (ran alongside setup; results /tmp/API_RESULTS_%s.md)\n' "test_api" "$API_RC" "$CLUSTER_ID"
fi
rm -f "$API_RC_FILE"

# ---- perf grading: ±20% band vs the 2026-07-17 reference (throughput below /
# ---- timing above = regression). Advisory in the summary; gate with STRICT_PERF=1.
if [ -f "$HERE/check_perf.sh" ]; then
  say "perf bands (±20% vs reference)"
  perf_rc=0; bash "$HERE/check_perf.sh" "$LOG" || perf_rc=$?
  if [ "${STRICT_PERF:-0}" = "1" ]; then
    PH_NAME+=("perf bands"); PH_SECS+=("0"); PH_RC+=("$perf_rc")
  fi
fi

# ---- summary -----------------------------------------------------------------------
TOTAL=$(( $(date +%s) - T_START ))
say "E2E SUMMARY (total ${TOTAL}s = $((TOTAL/60))m$((TOTAL%60))s)"
FAILS=0
for i in "${!PH_NAME[@]}"; do
  s="PASS"; [ "${PH_RC[$i]}" != "0" ] && { s="FAIL"; FAILS=$((FAILS+1)); }
  printf '   %-28s %6ss   %s\n' "${PH_NAME[$i]}" "${PH_SECS[$i]}" "$s"
done
echo "   full log: $LOG"
exit $([ "$FAILS" -eq 0 ] && echo 0 || echo 1)
