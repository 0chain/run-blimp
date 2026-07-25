#!/usr/bin/env bash
# ==============================================================================
# test_router_freshness.sh — router-flag correctness tests, fully automated.
# ==============================================================================
# Proves two things the read-through cache MUST guarantee, by toggling
# ZS3_SOURCE_VIA_ROUTER / ZS3_SOURCE_DATA_VIA_ROUTER on the gateway and observing
# behaviour against the LIVE cluster + the customer Iceberg source:
#
#   TEST A  CDC refresh from customer Iceberg (one query): author -> append to
#           the CUSTOMER catalog -> snapshot_changed -> re-run -> result changed.
#   TEST B  router ON -> DuckDB is NOT stale: with the read-through cache on and
#           warm, an append still changes the served result (fresh data, immutable
#           keys always miss the cache).
#   TEST C  router OFF -> the source S3 tables are NOT cached on the blobbers:
#           a source-read query writes ZERO bytes to the router/eblobber cache.
#
# Orchestrated from the operator box (Mac): SSM toggles the gateway flag +
# recreates minioserver; the append/query run on the in-VPC client box; router
# cache accounting comes from the gateway router's /iostats.
#
# Env (or override): REGION CLUSTER_ID GW_IID GW_PRIV CLIENT_IP PEM
#   ICEBERG_URL WAREHOUSE NAMESPACE(tpcds) Q(q1) ROWS(20000)
# ==============================================================================
set -uo pipefail
REGION="${REGION:?}"; CLUSTER_ID="${CLUSTER_ID:?}"; GW_IID="${GW_IID:?}"
GW_PRIV="${GW_PRIV:?}"; CLIENT_IP="${CLIENT_IP:?}"; PEM="${PEM:?}"
ICEBERG_URL="${ICEBERG_URL:?}"; WAREHOUSE="${WAREHOUSE:?}"
NAMESPACE="${NAMESPACE:-tpcds}"; ROWS="${ROWS:-50000}"
TOKEN="zus-$CLUSTER_ID"; FACT="${FACT:-store_returns}"
# Freshness signal = the optimizer's result md5. Use a single-fact aggregate that
# an append MUST move — a customer-join query (q1) filters synthetic sr_customer_sk
# rows out and would falsely read "stale". Override PROBE_SQL for another fact.
PROBE_SQL="${PROBE_SQL:-SELECT count(*) AS n, round(sum(sr_return_amt),2) AS tot FROM $FACT}"
export AWS_PAGER=""
PASS=0; FAIL=0
ok(){ printf '  \033[32mPASS\033[0m %s\n' "$*"; PASS=$((PASS+1)); }
no(){ printf '  \033[31mFAIL\033[0m %s\n' "$*"; FAIL=$((FAIL+1)); }
hdr(){ printf '\n\033[1m%s\033[0m\n' "$*"; }
box(){ ssh -o StrictHostKeyChecking=no -o ConnectTimeout=15 -i "$PEM" ubuntu@"$CLIENT_IP" "$@"; }

# --- toggle the two source-router flags on the gateway + recreate minioserver ---
gw_set_flags(){ # <via_router> <data_via_router>
  local via="$1" data="$2"
  hdr ">> gateway: ZS3_SOURCE_VIA_ROUTER=$via ZS3_SOURCE_DATA_VIA_ROUTER=$data (recreate minioserver)"
  local b64; b64=$(cat <<PYEOF | base64 | tr -d '\n'
import os,re,subprocess,glob,time
cf=subprocess.run(["docker","inspect","minioserver","--format",'{{index .Config.Labels "com.docker.compose.project.config_files"}}'],capture_output=True,text=True).stdout.strip()
if not cf or not os.path.isabs(cf):
    wd=subprocess.run(["docker","inspect","minioserver","--format",'{{index .Config.Labels "com.docker.compose.project.working_dir"}}'],capture_output=True,text=True).stdout.strip()
    cf=os.path.join(wd,cf) if wd and cf else cf
if not cf or not os.path.isfile(cf):
    g=glob.glob("/opt/0chain/zs3server/environment/docker-compose*.y*ml")+glob.glob("/opt/0chain/zs3server/docker-compose*.y*ml")
    cf=g[0] if g else ""
open(cf+".bak-%d"%int(time.time()),"w").write(open(cf).read())
src=open(cf).read()
vals={"ZS3_SOURCE_VIA_ROUTER":"$via","ZS3_SOURCE_DATA_VIA_ROUTER":"$data"}
for k,v in vals.items():
    pat=re.compile(r'^(\s+%s:\s*).*$'%re.escape(k),re.M)
    if pat.search(src): src=pat.sub(lambda m:m.group(1)+'"'+v+'"',src)
    else:
        a=re.search(r'^(\s+)ZS3_SOURCE_[A-Z_]+:.*$',src,re.M); ind=a.group(1)
        src=src[:a.end()]+'\n%s%s: "%s"'%(ind,k,v)+src[a.end():]
open(cf,"w").write(src)
wd=os.path.dirname(cf)
subprocess.run("cd %s && (docker-compose -f %s rm -sf minioserver || docker rm -f minioserver); docker-compose -f %s up -d minioserver"%(wd,cf,cf),shell=True)
for _ in range(40):
    h=subprocess.run(["bash","-lc","curl -s -o /dev/null -w '%{http_code}' http://localhost:9000/minio/health/live"],capture_output=True,text=True).stdout
    if h=="200": break
    time.sleep(3)
print("minioserver health:",h,"via_router=$via data_via_router=$data")
PYEOF
)
  local cmd; cmd=$(aws ssm send-command --region "$REGION" --instance-ids "$GW_IID" --document-name AWS-RunShellScript \
    --parameters "commands=[\"echo $b64 | base64 -d | python3\"]" --timeout-seconds 200 --query 'Command.CommandId' --output text 2>/dev/null)
  local st
  for i in $(seq 1 30); do sleep 6; st=$(aws ssm get-command-invocation --region "$REGION" --command-id "$cmd" --instance-id "$GW_IID" --query Status --output text 2>/dev/null)
    { [ "$st" = Success ] || [ "$st" = Failed ]; } && break; done
  aws ssm get-command-invocation --region "$REGION" --command-id "$cmd" --instance-id "$GW_IID" --query StandardOutputContent --output text 2>/dev/null | sed 's/^/    /'
  [ "$st" = Success ] || { no "gateway flag toggle failed"; return 1; }
}

# --- run the probe through the optimizer (source=customer) -> result md5 -------
run_q_hash(){ box "curl -s -m 900 http://$GW_PRIV:9000/admin/query/run -H 'Authorization: Bearer $TOKEN' -H 'Content-Type: application/json' \
  -d \"\$(python3 -c 'import json;print(json.dumps({\"original_sql\":\"$PROBE_SQL\",\"source\":\"customer\"}))')\" \
  | python3 -c 'import json,sys;d=json.load(sys.stdin);print(d.get(\"md5\",\"\"), d.get(\"merge_ms\") or \"\")'"; }

# --- append to the CUSTOMER catalog fact + fire the refresh webhook ------------
append_and_refresh(){
  box "PY=\$HOME/venv_ib/bin/python3; [ -x \$PY ] || PY=python3; SEED=\$HOME/seed_tpcds.py; [ -f \$SEED ] || SEED=\$HOME/kit/seed_tpcds.py; \$PY \$SEED --catalog $ICEBERG_URL --warehouse $WAREHOUSE --namespace $NAMESPACE --table $FACT --rows $ROWS --s3-region $REGION 2>&1 | tail -1"
  box "curl -s -m 60 http://$GW_PRIV:9000/admin/source/snapshot_changed -H 'Authorization: Bearer $TOKEN' -H 'Content-Type: application/json' -d '{\"namespace\":\"$NAMESPACE\",\"table\":\"$FACT\",\"trigger\":\"router-test\"}' >/dev/null; echo refreshed"
}

router_cache_written(){ box "curl -s -m5 http://$GW_PRIV:8088/iostats | python3 -c 'import json,sys;print(json.load(sys.stdin).get(\"bytes_written_to_cache\",-1))'" 2>/dev/null; }

# ================================ TESTS =======================================
hdr "TEST A + B — router ON: CDC refresh from customer Iceberg is FRESH (not stale)"
gw_set_flags 1 1 || exit 1
H0=$(run_q_hash | awk '{print $1}'); echo "    baseline probe md5=$H0"
run_q_hash >/dev/null  # warm the router cache with the source data files
CW0=$(router_cache_written); echo "    router bytes_written_to_cache after warm = $CW0"
append_and_refresh
H1=$(run_q_hash | awk '{print $1}'); echo "    post-append probe md5=$H1"
[ -n "$H0" ] && [ "$H0" != "$H1" ] && ok "router ON: result changed after append ($H0 -> $H1) — DuckDB read the fresh appended data, not the stale cache" \
  || no "router ON: result did NOT change ($H0 -> $H1) — possible STALE cache read"

hdr "TEST C — router OFF: source S3 tables are NOT cached on the blobbers"
gw_set_flags 0 0 || exit 1
CW_before=$(router_cache_written); echo "    router bytes_written_to_cache (pre-query) = $CW_before"
run_q_hash >/dev/null   # a source-read query; with router OFF it must bypass the cache
sleep 3
CW_after=$(router_cache_written); echo "    router bytes_written_to_cache (post-query) = $CW_after"
if [ "$CW_before" = "$CW_after" ]; then ok "router OFF: cache bytes unchanged ($CW_before) — source reads bypassed the router, nothing cached on the blobbers"
else no "router OFF: cache grew ($CW_before -> $CW_after) — source is being cached with the router off"; fi

hdr "restore gateway to router ON (default)"; gw_set_flags 1 1 >/dev/null || true
hdr "RESULT: $PASS passed, $FAIL failed"; [ "$FAIL" -eq 0 ]
