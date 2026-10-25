#!/bin/bash
# cang.sh <envf> <irc|-> <s3e|-> <sha> <label> <list> — one cold canary (author+tick+verify)
ENVF=$1; IRC=$2; S3E=$3; SHA=$4; LBL=$5; LIST=$6; L=/root/CANARY.log
TAG=${CANG_TAG:-enterprise_latest-$SHA}
until curl -sf "https://hub.docker.com/v2/repositories/0chaindev/zs3/tags/$TAG" >/dev/null; do sleep 20; done
# ONE RUN AT A TIME, BY LOCK: two queued launchers both saw the node idle and
# started together (REGH/REGD on 65, 2026-10-23). The lock is held for the run.
exec 9>/root/cang.lock; flock 9
while pgrep -f "tpc_batc[h] " >/dev/null; do sleep 15; done
T="SHA=$SHA TAG=$TAG HOLD_MAX=86400 EXPIRE_ABANDONED=1 EVICT_FAMILY_AFTER=0 EVICT_ALL=${EVICT_ALL:-0} DELTA_GATE_WAIT_S=3 SKIP_ROLLBACK=${SKIP_ROLLBACK:-0} TPC_BATCH_EVICT=${TPC_BATCH_EVICT:-1} TPC_BATCH_TICKS=${TPC_BATCH_TICKS:-1} TPC_BATCH_VERIFY=1"
env $T bash /root/canary_node.sh $ENVF $IRC $S3E "$LIST" $LBL
echo "$(date -u +%T) ${LBL}_ALL_DONE" >> $L
