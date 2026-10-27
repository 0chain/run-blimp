#!/bin/sh
# ycsb_s3.sh <ycsb home> <s3 endpoint> <access key> <secret> [label] [recordcount] [operationcount]
#
# YCSB's core workloads A–F against an S3 endpoint (the ZS3 gateway) with the
# s3 binding: each workload loads its records into a fresh bucket and runs its
# operation mix; throughput and per-operation latency go to /root/YCSB_<label>_<w>.log
# and one summary line per workload to /root/YCSB_<label>.out. Defaults are
# YCSB's own (recordcount=operationcount=1000, 10 fields x 100 bytes).
# Workload E is all SCANs; if the s3 binding does not implement scan its
# summary shows the errors (kept, so the gap is visible).
set -u
YH=$1; EP=$2; AK=$3; SK=$4; LBL=$(echo "${5:-run}" | tr "A-Z_" "a-z-"); RC=${6:-1000}; OC=${7:-1000}
OUT=/root/YCSB_$LBL.out
: > "$OUT"
for w in a b c d e f; do
  b="ycsb-$LBL-$w-$(date +%s)"
  P="-p table=$b -p s3.endPoint=$EP -p s3.accessKeyId=$AK -p s3.secretKey=$SK -p s3.region=us-east-1 -p recordcount=$RC -p operationcount=$OC"
  LOG=/root/YCSB_${LBL}_$w.log
  # the s3 binding uses `table` as the bucket and does not create it
  AWS_ACCESS_KEY_ID=$AK AWS_SECRET_ACCESS_KEY=$SK "${PY3:-python3}" -c "import boto3,sys; boto3.client('s3', endpoint_url='$EP', region_name='us-east-1').create_bucket(Bucket='$b')" > "$LOG" 2>&1 || { echo "workload$w bucket create failed: $(tail -1 "$LOG")" >> "$OUT"; continue; }
  { "$YH/bin/ycsb.sh" load s3 -P "$YH/workloads/workload$w" $P -s && \
    "$YH/bin/ycsb.sh" run s3 -P "$YH/workloads/workload$w" $P -s; } >> "$LOG" 2>&1
  rc=$?
  tp_load=$(grep -m1 "\[OVERALL\], Throughput" "$LOG" | awk -F', ' '{print $3}')
  tp_run=$(grep "\[OVERALL\], Throughput" "$LOG" | tail -1 | awk -F', ' '{print $3}')
  lat=$(grep -E "^\[(READ|UPDATE|INSERT|SCAN|READ-MODIFY-WRITE)\], (AverageLatency|99thPercentileLatency)" "$LOG" | tail -10 | awk -F', ' '{printf "%s %s=%s ", $1, $2, $3}')
  echo "workload$w rc=$rc load_ops_s=${tp_load:-?} run_ops_s=${tp_run:-?} $lat" >> "$OUT"
done
