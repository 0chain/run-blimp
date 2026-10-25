#!/bin/bash
# canary_node.sh <env> <irc_url|-> <s3_endpoint|-> "<list>" <label>: wait for TAG on minioserver (update once),
# wire the gateway to <env>'s source (overrides for container addresses), verify, evict, roll back, run.
ENVF=$1; IRC=$2; S3E=$3; LIST=$4; OUT=$5; SHA=${SHA:-76aecc63}; TAG=${TAG:-enterprise_latest-$SHA}; L=/root/CANARY.log
log(){ echo "$(date -u +%T) $OUT $*" >> $L; }
while pgrep -f 'tpc_batc[h] ' >/dev/null; do sleep 20; done
until curl -sf "https://hub.docker.com/v2/repositories/0chaindev/zs3/tags/$TAG" >/dev/null; do sleep 20; done
cd /opt/run-blimp-main && git pull -q --ff-only; log "run-blimp $(git log -1 --format=%h)"
if ! docker inspect -f '{{.Config.Image}}' minioserver | grep -q $SHA; then
  mkdir -p /root/gwlogs; docker logs minioserver > /root/gwlogs/gw_$(date -u +%m%d_%H%M%S).log 2>&1
  BLIMP_ENV=$ENVF ./blimp --update zs3 --tag zs3=$TAG >> $L 2>&1
  for i in $(seq 1 60); do curl -sf -o /dev/null http://127.0.0.1:9000/minio/health/live && break; sleep 5; done
  sleep 20
fi
docker inspect -f '{{.Config.Image}}' minioserver | grep -q $SHA || { log "IMAGE NOT UPDATED - abort"; exit 1; }
log "image $(docker inspect -f '{{.Config.Image}}' minioserver)"
set -a; . $ENVF; set +a
[ "$IRC" != - ] && ICEBERG_URL=$IRC; [ "$S3E" != - ] && S3_ENDPOINT=$S3E
T=$(cat /opt/0chain/zs3server/environment/admin_token)
BODY=$(python3 -c 'import json,os;e=os.environ;print(json.dumps({"source":"customer","iceberg_url":e["ICEBERG_URL"],"warehouse":e["WAREHOUSE"],"namespace":e["NAMESPACE"],"bucket":e["ORIGIN_BUCKET"],"s3_endpoint":e.get("S3_ENDPOINT",""),"s3_key":e.get("S3_KEY",""),"s3_secret":e.get("S3_SECRET",""),"s3_region":e.get("REGION","us-east-1"),"via_router":False}))')
R=$(curl -s -m 30 -X POST http://127.0.0.1:9000/admin/source/configure -H "Authorization: Bearer $T" -H "Content-Type: application/json" -d "$BODY")
echo "$R" | grep -q '"status": *"ok"' || { log "WIRE FAILED $(echo $R | head -c 200) - abort"; exit 1; }
NS=$(docker exec minioserver cat /data/zs3/source_configure.json | python3 -c "import json,sys; print(json.load(sys.stdin)['CUSTOMER']['IRC_NAMESPACE'])")
[ "$NS" = "$NAMESPACE" ] || { log "WIRING ns=$NS want $NAMESPACE - abort"; exit 1; }
log "wired ns=$NS"
# EVICT_ALL=0 (fix canaries): skip the whole-node evict (7-9 min for ~1,300
# families); tpc_batch's phase 0 still evicts each query's own family, so every
# canaried query authors cold. The full regression keeps EVICT_ALL=1.
if [ "${EVICT_ALL:-1}" = 1 ]; then
python3 /root/evict_all.py > /root/$OUT.evict 2>&1; log "evict $(cut -c1-120 /root/$OUT.evict)"
else
log "evict_all skipped (EVICT_ALL=0) — per-query family evicts only"
fi
if [ "${SKIP_ROLLBACK:-0}" = 1 ]; then log "rollback skipped (SKIP_ROLLBACK=1) — warm run"; else
mkdir -p /tmp/rb && cp /root/rollback.py /tmp/rb/ && cd /tmp/rb && /root/.blimp_venv/bin/python rollback.py $ENVF apply > /root/$OUT.rollback 2>&1 || { log "ROLLBACK FAILED"; exit 1; }
grep -q False /root/$OUT.rollback && { log "ROLLBACK FALSE"; exit 1; }
log "rollback $(grep -c '^rolled.*True' /root/$OUT.rollback) tables; start"
fi
# EXPIRE_ABANDONED=1: drop the snapshots the rollback abandoned (metadata only) —
# ~1,170 per table made every tick's diff 30-60 ms/table and the seeder's first
# append 217 s (2026-10-10). User-approved after the 64d995f6 regression.
if [ "${EXPIRE_ABANDONED:-0}" = 1 ]; then
  (cd /tmp/rb && /root/.blimp_venv/bin/python /tmp/rb/expire_abandoned.py $ENVF apply > /root/$OUT.expire 2>&1) && log "expired abandoned snapshots: $(grep -c 'expired ->' /root/$OUT.expire) tables" || log "EXPIRE FAILED (run continues)"
fi
# A rollback sends no notify, so a gateway trusting its stale flags serves the
# previous run's MVs until the drift poller next looks: v_q67_rank's kept chart
# authored "FRESH" over the rolled-back source and tick 1 paid the 20-26 s
# rebuild (ABPC_R2/R3, node 37, 2026-10-21). Tell it, as the tick's appends do.
if [ "${SKIP_ROLLBACK:-0}" != 1 ]; then
  RB=$(awk '$2=="current" && $3!=$7 {print $1}' /root/$OUT.rollback)
  for tb in $RB; do
    curl -s -m 60 http://127.0.0.1:9000/admin/source/snapshot_changed -H "Authorization: Bearer $T" -H "Content-Type: application/json" \
      -d "{\"namespace\":\"$NAMESPACE\",\"table\":\"$tb\",\"trigger\":\"canary-rollback\"}" >/dev/null
  done
  log "rollback notified $(echo $RB | wc -w) table(s)"
fi
printf "$LIST\n" > /root/$OUT.list
cd /opt/run-blimp-main && TPC_BATCH_EVICT=${TPC_BATCH_EVICT:-1} TPC_BATCH_TICKS=${TPC_BATCH_TICKS:-2} BLIMP_ENV=$ENVF TPC_BATCH_OUT=/root/$OUT HOLD_MAX=86400 ./tpc_batch /root/$OUT.list > /root/$OUT.out 2>&1
log "DONE"
