#!/usr/bin/env bash
# test_cache.sh — Blimp STORAGE/CACHE cluster test suite (setup-aware, one command).
#
# Runs, in order, from a client box that is IN THE CLUSTER'S VPC (so every path uses
# the gateway PRIVATE IP and external S3 goes through the VPC S3 gateway endpoint —
# no egress):
#   1. warp        — S3 PUT/GET + 1KiB TTFB (conc 16 for 2/1)
#   2. fio         — NFS write + cold sequential read
#   3. mlperf      — resnet50 (dlio) via mountpoint-s3 (accel-4/rt-16/pf-32)
#   4. cache       — read-through cache: direct-S3 vs cache-S3 (router) vs
#                    cache-mp-s3 vs cache-NFS, over an external S3 origin bucket
#   5. eviction    — router capacity eviction (pct high/low) fires + reclaims
#
# PREREQS (one-time): run ./setup_tests.sh to provision this box in the cluster
# VPC with warp/fio/dlio/mount-s3/awscli, an S3 VPC gateway endpoint, and an IAM
# instance profile that can read the origin bucket. The router (zus-router :8088) must
# be repointed at the origin bucket (see README "read-through cache setup").
#
# Endpoints are taken from env if set, else prompted. Save them in an env file and
# `source` it to re-run non-interactively:
#   GW=10.10.150.76 GW_AK=... GW_SK=... ORIGIN_BUCKET=blimp-tpcds1000-aps1 \
#   REGION=ap-south-1 EC=2/1 ./run_all.sh
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"

ask(){ local cur="${!1:-}"; if [ -n "$cur" ]; then printf '%s' "$cur"; return; fi
  printf '%s' "$2 " >&2; read -r v; printf '%s' "$v"; }

echo "==================== Blimp cluster storage test suite ===================="
echo "Enter the cluster endpoints (all reachable on the VPC PRIVATE network)."
GW=$(ask GW            "Gateway PRIVATE IP (serves NFS :2049, S3 :9000, router :8088):")
GW_AK=$(ask GW_AK      "Gateway S3 access key:")
GW_SK=$(ask GW_SK      "Gateway S3 secret key:")
ROUTER="${ROUTER:-http://$GW:8088}"
NFS_MNT="${NFS_MNT:-/mnt/zusnfs}"
EC="${EC:-2/1}"
ORIGIN_BUCKET=$(ask ORIGIN_BUCKET "External S3 origin bucket for the read-through cache (e.g. blimp-tpcds1000-aps1):")
REGION="${REGION:-ap-south-1}"
# Prefix under the origin bucket to read. The default used to be a hardcoded
# store_returns, which at SF1 is the SMALLEST fact (~17 MB in ONE object) — so the
# read-through cache was benchmarked over a set far too small to measure, and the
# summary reported the router as slower than direct S3 purely from per-request
# overhead. Pick the LARGEST prefix actually present instead; a bigger dataset
# makes the cache leg meaningful, and an explicit CACHE_TABLE still wins.
# run_router.sh already defaults to `store_sales catalog_sales` (~163 GiB at
# SF1000, deliberately larger than RAM so warm reads cannot be page-cache-served).
# Passing ONE small prefix here overrode that good default with the WORST case.
# Take the largest few prefixes actually present instead, and pass them all.
if [ -z "${CACHE_TABLE:-}" ]; then
  CACHE_TABLE=$(for t in store_sales catalog_sales web_sales inventory store_returns catalog_returns web_returns; do
      sz=$(aws s3 ls "s3://$ORIGIN_BUCKET/$t/" --recursive --region "$REGION" 2>/dev/null | awk '{s+=$3} END{print s+0}')
      [ "${sz:-0}" -gt 0 ] && echo "$sz $t"
    done | sort -rn | head -3 | awk '{printf "%s%s", (NR>1?" ":""), $2}')
  CACHE_TABLE="${CACHE_TABLE:-store_returns}"   # nothing probed (no aws creds / empty bucket)
  echo "[cache] auto-selected origin prefixes: $CACHE_TABLE"
fi
export GW GW_AK GW_SK ROUTER NFS_MNT EC ORIGIN_BUCKET REGION

echo ""
echo "gateway=$GW  EC=$EC  router=$ROUTER  origin=s3://$ORIGIN_BUCKET/$CACHE_TABLE ($REGION)"
echo "----------------------------------------------------------------------"
# capture our own output so the final summary can be extracted from it
CAP="${CAP:-/tmp/test_cache_out.log}"; : > "$CAP"
exec > >(tee -a "$CAP") 2>&1
run(){ echo; echo ">>> $1"; shift; "$@"; }

# Leg selector. --storage was all-or-nothing, so re-measuring ONE leg re-paid the
# others: every mlperf attempt first re-ran ~12 min of warp and ~7 min of fio for
# numbers already in hand. run_cluster.sh has always accepted the legs
# individually; this just exposes that.
#   STORAGE_LEGS=cache          only the read-through cache + write->read legs
#   STORAGE_LEGS=mlperf         only mlperf
#   STORAGE_LEGS=warp,fio       several
#   (unset / all)               everything, as before
LEGS="${STORAGE_LEGS:-all}"
want(){ case "$LEGS" in all|"") return 0;; esac; case ",$LEGS," in *",$1,"*) return 0;; esac; return 1; }
[ "$LEGS" != "all" ] && echo "[legs] running only: $LEGS"

# GUARANTEED cleanup — runs on completion, Ctrl-C, or any abort. Bench leftovers
# (warp --keep-data, the kept mlperf dataset, fio files) pile ~100GB onto the
# eblobber allocation; past the 80% watermark the capacity evictor then drains
# the read-through cache every 5m sweep (evictions spike, hit-rate collapses —
# seen live 2026-07-17: alloc 94.7%, evictor freed every cache fill).
# KEEP_BENCH_DATA=1 skips it (e.g. to rerun mlperf without the slow regen).
CLEANED=0
cleanup_bench(){
  [ "$CLEANED" = "1" ] && return 0; CLEANED=1
  [ "${KEEP_BENCH_DATA:-0}" = "1" ] && { echo "-- cleanup skipped (KEEP_BENCH_DATA=1) --"; return 0; }
  echo ""
  echo "-- cleanup: removing bench artifacts from the cluster --"
  sudo umount /mnt/mps3 /mnt/mps3_cache 2>/dev/null
  ( export AWS_ACCESS_KEY_ID="$GW_AK" AWS_SECRET_ACCESS_KEY="$GW_SK" AWS_REGION=us-east-1
    for b in ttfb1k warpprobe warpbench mlperf-bench; do
      aws s3 rb "s3://$b" --force --endpoint-url "http://$GW:9000" >/dev/null 2>&1
    done )
  if mountpoint -q "$NFS_MNT" 2>/dev/null; then
    sudo rm -rf "$NFS_MNT/bench-fio" "$NFS_MNT/mlperf-bench" 2>/dev/null
  fi
  echo "   removed: ttfb1k/warpprobe/warpbench/mlperf-bench buckets + bench-fio/mlperf NFS dirs"
}
trap cleanup_bench EXIT INT TERM

# 1) warp (S3 :9000) — 1KiB TTFB then 96MiB PUT/GET at EC concurrency (16 for 2/1)
want warp && run "1/5 warp TTFB (1KiB, conc=1)"   env GW="$GW" NFS="$GW" EC="$EC" AK="$GW_AK" SK="$GW_SK" "$HERE/run_cluster.sh" ttfb
want warp && run "   warp PUT/GET (96MiB, conc=16)" env GW="$GW" NFS="$GW" EC="$EC" AK="$GW_AK" SK="$GW_SK" "$HERE/run_cluster.sh" warp

# 2) fio (NFS) — write + cold sequential read
want fio && run "2/5 fio (NFS write + cold seq read)" env GW="$GW" NFS="$GW" EC="$EC" AK="$GW_AK" SK="$GW_SK" "$HERE/run_cluster.sh" fio

# 3) mlperf resnet50 via mountpoint-s3, accel-4 / rt-16 / pf-32 (generate once, keep)
want mlperf && run "3/5 mlperf resnet50 (mp-s3, accel-4/rt-20/pf-40)" \
  env GW="$GW" NFS="$GW" EC="$EC" AK="$GW_AK" SK="$GW_SK" \
  MLPERF_ACCELS=4 MLPERF_READ_THREADS=20 MLPERF_PREFETCH=40 MLPERF_IFACE=mps3 MLPERF_KEEP=1 \
  "$HERE/run_cluster.sh" mlperf

# 4) read-through cache: direct-S3 vs cache-S3 vs cache-mp-s3 vs cache-NFS
want cache && run "4/5 read-through cache ($CACHE_TABLE via router)" \
  env GW="$GW" ORIGIN_BUCKET="$ORIGIN_BUCKET" REGION="$REGION" TABLE="$CACHE_TABLE" \
  ROUTER="$ROUTER" NFS_MNT="$NFS_MNT" GW_AK="$GW_AK" GW_SK="$GW_SK" \
  "$HERE/run_router.sh"

# 5) eviction — informational: the router must be running with pct high/low (or
#    -cache-max-bytes) wired to the gateway /admin/alloc/usage. This step just reads
#    enough data through the router to cross the high watermark and prints the
#    router's evictor log so you can see it fire + reclaim. Configure via the README.
run "5/5 eviction (drive the cache past the high watermark; watch the evictor)" \
  bash -c '
    KEYS=$(aws s3 ls "s3://'"$ORIGIN_BUCKET"'/'"$CACHE_TABLE"'/" --recursive --region "'"$REGION"'" 2>/dev/null | awk "\$3+0>0{print \$4}")
    echo "$KEYS" | xargs -P32 -I{} curl -s -m600 -o /dev/null "'"$ROUTER"'/'"$ORIGIN_BUCKET"'/{}"
    echo "cache filled through the router — check the router logs for [evict] lines:"
    echo "   (on the gateway)  docker logs zus-router --since 60s | grep evict"
  '

echo ""
echo "============================ STORAGE/CACHE SUMMARY ============================"
awk '
  / TTFB: Avg/                { ttfb=$0 }
  /== warp PUT/               { sect="put" } /== warp GET/ { sect="get" }
  / \* Average:.*MiB\/s/      { if (sect=="put" && !put) put=$3; else if (sect=="get" && !get) get=$3 }
  /^  write: IOPS.*BW=/       { sub(/.*BW=/,""); sub(/ .*/,""); fw=$0 }
  /^  read: IOPS.*BW=/        { sub(/.*BW=/,""); sub(/ .*/,""); fr=$0 }
  /\[METRIC\] Training Accelerator/      { au=$NF; sub(/\(.*/,"",au); au=$(NF-1) }
  /\[METRIC\] Training Throughput/       { sm=$(NF-1) }
  /\[METRIC\] Training I\/O Throughput/  { io=$(NF-1) }
  /direct-S3 \(origin\)/      { d=$3 }  /cache-S3 \(router/   { c=$4 }
  /cache-mp-s3 \(warm\)/      { m=$3 }  /cache-NFS \(Ganesha/ { n=$4 }
  /speedup vs direct-S3/      { sp=$0; sub(/^ */,"",sp) }
  END{
    printf "  warp   S3 PUT %s MiB/s · GET %s MiB/s\n", put, get
    if (ttfb) { sub(/^ *\* */,"",ttfb); printf "  %s\n", ttfb }
    printf "  fio    NFS write %s · read %s\n", fw, fr
    if (io) printf "  mlperf AU %s%% · %s samples/s · %s MB/s\n", au, sm, io
    printf "  cache  direct-S3 %s · router %s · mp-s3 %s · NFS %s MB/s\n", d, c, m, n
    if (sp) printf "  %s\n", sp
  }' "$CAP"
echo "==============================================================================="
echo "==================== suite complete — compare to EXPECTED_TEST_RESULTS.md ===================="
