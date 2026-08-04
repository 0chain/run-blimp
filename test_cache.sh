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
# The origin is not always AWS S3 — when the customer had no data, blimp --setup
# stands MinIO up on THIS box and the origin bucket lives there. Every origin
# listing below must therefore carry its endpoint + keys, or it silently probes
# real s3.<REGION>.amazonaws.com for a bucket that does not exist there and the
# prefix auto-selection falls back to a table with no objects.
OEP=""
if [ -n "${S3_ENDPOINT:-}" ]; then
  OEP="--endpoint-url $S3_ENDPOINT"
  [ -n "${S3_KEY:-}" ] && export AWS_ACCESS_KEY_ID="$S3_KEY" AWS_SECRET_ACCESS_KEY="$S3_SECRET"
fi
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
      sz=$(aws s3 ls "s3://$ORIGIN_BUCKET/$t/" --recursive --region "$REGION" $OEP 2>/dev/null | awk '{s+=$3} END{print s+0}')
      [ "${sz:-0}" -gt 0 ] && echo "$sz $t"
    done | sort -rn | head -3 | awk '{printf "%s%s", (NR>1?" ":""), $2}')
  CACHE_TABLE="${CACHE_TABLE:-store_returns}"   # nothing probed (no aws creds / empty bucket)
  echo "[cache] auto-selected origin prefixes: $CACHE_TABLE"
fi
export GW GW_AK GW_SK ROUTER NFS_MNT EC ORIGIN_BUCKET REGION S3_ENDPOINT S3_KEY S3_SECRET

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
#   STORAGE_LEGS=cache          only the read-through cache leg
#   STORAGE_LEGS=mlperf         only mlperf
#   STORAGE_LEGS=warp,fio       several
#   (unset / all)               everything, as before
LEGS="${STORAGE_LEGS:-all}"
want(){ case "$LEGS" in all|"") return 0;; esac; case ",$LEGS," in *",$1,"*) return 0;; esac; return 1; }
[ "$LEGS" != "all" ] && echo "[legs] running only: $LEGS"

# NO AUTOMATIC CLEANUP. A trap used to delete the bench artifacts on every exit —
# including the mlperf dataset, which costs ~17 minutes to regenerate on a small
# client. Any rerun of the train leg (a different accel/rt/pf, or a rerun after a
# crash) therefore paid the full regenerate again, and a run that failed for an
# unrelated reason destroyed the dataset on its way out. The suite now LEAVES what
# it wrote; remove it deliberately when you are done:
#
#   AWS_ACCESS_KEY_ID=$GW_AK AWS_SECRET_ACCESS_KEY=$GW_SK AWS_REGION=us-east-1 \
#     aws s3 rb s3://mlperf-bench --force --endpoint-url http://$GW:9000
#   sudo rm -rf $NFS_MNT/bench-fio $NFS_MNT/mlperf-bench
#   for b in ttfb1k warpprobe warpbench; do aws s3 rb s3://$b --force \
#     --endpoint-url http://$GW:9000; done
#
# Watch the allocation while leftovers accumulate: past the 80% watermark the
# router's capacity evictor drains the read-through cache on every 5m sweep
# (seen live 2026-07-17 at alloc 94.7%). Check with:
#   curl -s "http://$GW:9000/admin/alloc/usage?token=zus-<cluster-id>"

# 1) warp (S3 :9000) — 1KiB TTFB then 96MiB PUT/GET at EC concurrency (16 for 2/1)
want warp && run "1/4 warp TTFB (1KiB, conc=1)"   env GW="$GW" NFS="$GW" EC="$EC" AK="$GW_AK" SK="$GW_SK" "$HERE/run_cluster.sh" ttfb
want warp && run "   warp PUT/GET (96MiB, conc=16)" env GW="$GW" NFS="$GW" EC="$EC" AK="$GW_AK" SK="$GW_SK" "$HERE/run_cluster.sh" warp

# 2) fio (NFS) — write + cold sequential read
want fio && run "2/4 fio (mount-s3 write + cold seq read)" env GW="$GW" NFS="$GW" EC="$EC" AK="$GW_AK" SK="$GW_SK" "$HERE/run_cluster.sh" fio

# 3) mlperf resnet50 via mountpoint-s3, accel-4 / rt-16 / pf-32 (generate once, keep)
# Params come from detect_ec (EC 2/1 -> accel 3 / rt 12 / pf 24), the SAME table
# the cluster's own benchmark uses. They used to be overridden to 4/20/40 — more
# ranks and threads than the cluster runs for the same EC — and on a 4 GB client
# the extra torch rank starved the mount-s3 daemon, which sits OUTSIDE the dlio
# memory cgroup: the FUSE mount died 14 files into a 476-file read and every rank
# then failed with "Transport endpoint is not connected". Override with
# MLPERF_ACCELS / MLPERF_READ_THREADS / MLPERF_PREFETCH on a bigger box.
want mlperf && run "3/4 mlperf resnet50 (mp-s3, EC-derived accel/rt/pf)" \
  env GW="$GW" NFS="$GW" EC="$EC" AK="$GW_AK" SK="$GW_SK" \
  MLPERF_IFACE=mps3 MLPERF_KEEP=1 \
  MLPERF_NUM_FILES="${MLPERF_NUM_FILES:-}" MLPERF_NUM_EVAL="${MLPERF_NUM_EVAL:-}" \
  MLPERF_ACCELS="${MLPERF_ACCELS:-}" MLPERF_REGEN="${MLPERF_REGEN:-}" \
  "$HERE/run_cluster.sh" mlperf

# 4) read-through cache: direct-S3 vs cache-S3 vs cache-mp-s3 vs cache-NFS
want cache && run "4/4 read-through cache ($CACHE_TABLE via router)" \
  env GW="$GW" ORIGIN_BUCKET="$ORIGIN_BUCKET" REGION="$REGION" TABLE="$CACHE_TABLE" \
  ROUTER="$ROUTER" NFS_MNT="$NFS_MNT" GW_AK="$GW_AK" GW_SK="$GW_SK" \
  S3_ENDPOINT="${S3_ENDPOINT:-}" S3_KEY="${S3_KEY:-}" S3_SECRET="${S3_SECRET:-}" \
  "$HERE/run_router.sh"

# NO EVICTION LEG. It reported no number: it re-read the whole set through the
# router purely to generate load, then told the operator to run
# `docker logs zus-router` ON THE GATEWAY — which the client box cannot do, it
# has no shell there. And its stated goal, "drive the cache past the high
# watermark", steers at the one condition that makes the evictor delete from a
# bucket that also holds source data and MV output. Eviction is verified by unit
# tests in the router repo, not by pushing a live cluster at its watermark.

echo ""
echo "============================ STORAGE/CACHE SUMMARY ============================"
# Print a line ONLY when that leg actually produced numbers. Unconditional
# printf emitted "warp S3 PUT  MiB/s · GET  MiB/s" with empty fields whenever a
# leg was skipped (or failed), which reads as a broken tool rather than a leg
# that never ran — and with STORAGE_LEGS that is now the normal case.
# Also surface the WRITE side of each test, not just the read: the cache-miss
# fill, the write→read leg, and mlperf's dataset generation were all measured but
# never reached the summary.
awk '
  / TTFB: Avg/                { ttfb=$0 }
  /== warp PUT/               { sect="put" } /== warp GET/ { sect="get" }
  / \* Average:.*MiB\/s/      { if (sect=="put" && !put) put=$3; else if (sect=="get" && !get) get=$3 }
  /^  write: IOPS.*BW=/       { sub(/.*BW=/,""); sub(/ .*/,""); fw=$0 }
  /^  read: IOPS.*BW=/        { sub(/.*BW=/,""); sub(/ .*/,""); fr=$0 }
  /mlperf write-NFS:/         { mw=$3 }
  /Accelerator Utilization/          { au=$0; sub(/.*: */,"",au); sub(/ *\(.*/,"",au) }
  /Training Throughput.*samples/     { sm=$0; sub(/.*: */,"",sm); sub(/ *\(.*/,"",sm) }
  /Training I\/O Throughput/         { io=$0; sub(/.*: */,"",io); sub(/ *\(.*/,"",io) }
  /cache-bench\] set:/        { setsz=$0; sub(/^.*set: /,"",setsz) }
  /cache-MISS→write:/         { cw=$2 }
  /direct-S3 \(origin\)/      { d=$3 }  /cache-S3 \(router/   { c=$4 }
  /cache-NFS \(Ganesha/ { n=$4 }
  /speedup vs direct-S3/      { sp=$0; sub(/^ */,"",sp) }
  END{
    any=0
    if (put || get) { printf "  warp    S3 PUT %s MiB/s · GET %s MiB/s\n", put, get; any=1
                      if (ttfb) { sub(/^ *\* */,"",ttfb); printf "  %s\n", ttfb } }
    if (fw || fr)   { printf "  fio     NFS write %s · read %s\n", fw, fr; any=1 }
    if (mw || io || au) { printf "  mlperf  write-NFS %s MB/s", (mw?mw:"n/a")
                      if (au || io) printf " · read AU %s%% · %s samples/s · %s MB/s", (au?au:"n/a"), (sm?sm:"n/a"), (io?io:"n/a")
                      else          printf " · read (train produced no metrics)"
                      printf "\n"; any=1 }
    if (d || c) {
      printf "  cache   set %s · direct-S3 %s · MISS→write %s · hit-S3 %s MB/s\n",
             (setsz?setsz:"?"), d, (cw?cw:"n/a"), c
      if (sp) printf "  %s\n", sp
      any=1 }
    if (!any) print "  (no leg produced numbers — see the log above for the failure)"
  }' "$CAP"
echo "==============================================================================="
echo "==================== suite complete — compare to EXPECTED_TEST_RESULTS.md ===================="
