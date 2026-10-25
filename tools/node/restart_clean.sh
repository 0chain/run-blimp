#!/bin/bash
# restart_clean.sh <env> <irc|-> <s3e|-> <label> <argsfile>: stop the running clean pass, update to zs3:latest, start a new one
ENVF=$1; IRC=$2; S3E=$3; LBL=$4; ARGS=$5
for pat in "bash /root/CLEAN" "^bash /root/cang.sh" "canary_node.sh" "tpc_batc[h] " "bench_cdc.sh" "blimp --query"; do pkill -f "$pat"; done
sleep 5
pgrep -fa "tpc_batc[h] |canary_node|bench_cd[c]" > /root/$LBL.leftover
cd /opt/run-blimp-main && BLIMP_ENV=$ENVF ./blimp --update zs3 --tag zs3=latest > /root/$LBL.update.log 2>&1
for i in $(seq 1 60); do curl -sf -o /dev/null http://127.0.0.1:9000/minio/health/live && break; sleep 5; done; sleep 20
docker image inspect $(docker inspect minioserver --format "{{.Image}}") --format "{{.RepoDigests}}" >> /root/$LBL.update.log
CANG_TAG=latest bash /root/cang.sh $ENVF $IRC $S3E latest $LBL "$(cat $ARGS)"
