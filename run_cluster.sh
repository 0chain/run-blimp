#!/usr/bin/env bash
# run_cluster.sh — the customer-runnable slice of the cluster's
# /opt/cdc/run_bench.sh. Only the 2/1 and 8/1 benchmark tests (warp put/get,
# 1KiB TTFB, fio, mlperf resnet50), with the SAME specs the in-cluster Blimp
# "Benchmarks" UI runs — but from this client box against the gateway + NFS.
# All cluster-internal machinery (summary JSON, /opt/cdc, UI wiring, bucket
# janitor) is stripped.
#
# Prereq: setup_tests.sh installed warp v1.1.4, fio 3.28, dlio-benchmark 2.0
# (/opt/dlio/venv_mlperf); box is in the cluster VPC.
#
# Usage:
#   GW=10.10.150.76 NFS=10.10.150.76 EC=2/1 AK=<gw-key> SK=<gw-secret> \
#   ./run_cluster.sh <warp|ttfb|fio|mlperf|all>
#
# EC=2/1 or 8/1 selects the same detect_ec params as run_bench. Overrides:
#   BENCH_DURATION (30) TTFB_DUR (30) WARP_OBJ_SIZE (96MiB) FIO_SIZE FIO_JOBS
#   MLPERF_NUM_FILES (200) NFS_MNT (/mnt/zusnfs). Disk-fill guard aborts at 90%.

set -uo pipefail
WHAT="${1:-all}"; GW="${GW:?set GW (gateway ip:host w/o port)}"; NFS="${NFS:-$GW}"
EC="${EC:-2/1}"; DUR="${BENCH_DURATION:-30}"; TTFB_DUR="${TTFB_DUR:-30}"; MNT="${NFS_MNT:-/mnt/zusnfs}"
OSZ="${WARP_OBJ_SIZE:-96MiB}"

# --- detect_ec: identical table to /opt/cdc/run_bench.sh ------------------------
#   2/1 -> warp-conc 16, fio 16 jobs / iodepth 16 / 2304 MiB per stream (36 GiB),
#          mlperf resnet50 accel 3 / read_threads 12 / prefetch 24 / batch 1200 / ntrain=34*7
#   8/1 -> warp-conc 64, fio 32 jobs / iodepth 8  / 4608 MiB per stream (144 GiB),
#          mlperf resnet50 accel 6 / read_threads 24 / prefetch 48 / batch 1200 / ntrain=136*7
d="${EC%%/*}"
if [ "$d" -ge 8 ]; then EC_CONC=64; EC_FIO_JOBS=32; EC_FIO_IODEPTH=8;  EC_FIO_MIB=4608
  EC_DATASET_GB=136; EC_ACCEL=6; EC_RT=24; EC_PF=48
else                    EC_CONC=16; EC_FIO_JOBS=16; EC_FIO_IODEPTH=16; EC_FIO_MIB=2304
  # 68 GiB set = 2x the 32 GiB gateway RAM: the warp GET can NOT be served from
  # gateway page cache (34 GiB barely exceeded RAM and read cache-favorably).
  EC_DATASET_GB=68;  EC_ACCEL=3; EC_RT=12; EC_PF=24; fi
FJ="${FIO_JOBS:-$EC_FIO_JOBS}"; FSZ="${FIO_SIZE:-${EC_FIO_MIB}M}"; FID="$EC_FIO_IODEPTH"
echo "[detect_ec] EC=$EC -> warp-conc=$EC_CONC fio-jobs=$EC_FIO_JOBS fio-iodepth=$EC_FIO_IODEPTH per-stream=${EC_FIO_MIB}MiB obj=$OSZ mlperf-accel=$EC_ACCEL rt=$EC_RT pf=$EC_PF"

W(){ warp "$1" --host="$GW:9000" --access-key="${AK:?set AK}" --secret-key="${SK:?set SK}" --tls=false --no-color "${@:2}"; }
# clean_bkt — remove a warp/ttfb scratch bucket after use (warp --keep-data leaves it
# on the allocation otherwise; that's what piled up 50+ GiB of warp* buckets). Uses
# the gateway S3 endpoint via awscli.
clean_bkt(){ export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_REGION=us-east-1
  for b in "$@"; do aws s3 rb "s3://$b" --force --endpoint-url "http://$GW:9000" >/dev/null 2>&1; done; }
disk_guard(){ mountpoint -q "$MNT" || return 0; local p; p=$(df --output=pcent "$MNT" 2>/dev/null|tail -1|tr -dc 0-9)
  echo "  [$MNT ${p:-?}% used]"; [ "${p:-0}" -ge 90 ] && { echo "!! ${p}% >=90% — abort (disk-fill guard)"; exit 1; }; }
mount_nfs(){ mountpoint -q "$MNT" && return 0; sudo mkdir -p "$MNT"
  if ! sudo mount -t nfs4 -o nconnect=16,rsize=1048576,wsize=1048576 "$NFS":/ "$MNT"; then
    # Fail LOUD and make callers skip: falling through would run fio against
    # the local disk under the mountpoint dir and report the client box's
    # speed as the cluster's (observed on an external-mode host, 2026-07-27).
    echo "SKIP: NFS $NFS:/ unreachable (external-mode host? NFS is VPC-only)"
    return 1
  fi
  echo "mounted $NFS:/ -> $MNT"; }

# --- 1KiB TTFB: PUT conc=EC_CONC keep-data, then conc=1 GET (pure first-byte) ----
bench_ttfb(){ echo "== 1KiB TTFB (PUT conc=$EC_CONC 10s, GET conc=1 ${TTFB_DUR}s) =="
  W put --bucket=ttfb1k --obj.size=1KiB --concurrent="$EC_CONC" --duration=10s --keep-data >/dev/null 2>&1
  W get --bucket=ttfb1k --list-existing --noclear --concurrent=1 --duration="${TTFB_DUR}s" 2>&1 | grep -iE 'TTFB:' | head -1
  clean_bkt ttfb1k; }

# --- warp PUT + GET, 96MiB objects, conc=EC_CONC (separate, not mixed) ----------
# run_bench sizes PUT_DUR from a rate probe so the PUT lands the FULL ~EC_DATASET_GB
# set (> gateway RAM) — a fixed 30s only writes ~24GB and the GET then reads a
# cache-favorable subset. Match it: probe 8s, set PUT_DUR = budget*1.1/rate (floor DUR).
bench_warp(){ local BUD=$(( ${WARP_BUDGET_MIB:-$(( EC_DATASET_GB * 1024 ))} ))
  echo "== warp probe (8s) to size PUT to ${BUD}MiB set =="
  local PR; PR=$(W put --bucket=warpprobe --obj.size="$OSZ" --concurrent="$EC_CONC" --duration=8s 2>&1 | grep -oE 'Average: [0-9.]+ [KMGT]?i?B/s' | head -1 | awk '{v=$2+0;u=$3; if(u~/GiB/)v*=1024; else if(u~/KiB/)v/=1024; printf "%d",v}')
  [ -z "$PR" ] || [ "$PR" -le 0 ] && PR=0
  local PDUR="${WARP_PUT_DUR:-$DUR}"; [ "$PR" -gt 0 ] && { PDUR=$(( BUD * 11 / 10 / PR )); [ "$PDUR" -lt "$DUR" ] && PDUR="$DUR"; }
  echo "== warp PUT $OSZ (conc=$EC_CONC ${PDUR}s -> ~${BUD}MiB, probe ${PR}MiB/s) =="
  W put --bucket=warpbench --obj.size="$OSZ" --concurrent="$EC_CONC" --duration="${PDUR}s" --keep-data 2>&1 | grep -iE 'Average:|Errors:' | head -2
  disk_guard
  echo "== warp GET $OSZ (conc=$EC_CONC list-existing ${DUR}s, blobber-served) =="
  W get --bucket=warpbench --list-existing --noclear --concurrent="$EC_CONC" --duration="${DUR}s" 2>&1 | grep -iE 'Average:|Errors:' | head -2
  clean_bkt warpbench warpprobe; }

# --- fio write + read on the NFS mount (exact run_bench flags) --------------------
# WRITE: direct/libaio at EC iodepth. READ: run_bench restarts minioserver+zus-nfs
# and drop_caches to force cold-from-blobber, then reads SEQUENTIAL + buffered
# (--rw=read --ioengine=psync) — random+O_DIRECT is a cache-defeating FLOOR, not the
# achievable rate. A CLIENT box can't restart the gateway container or drop its page
# cache, so cold is instead guaranteed by a working set > gateway RAM: NJ*FSZ (2/1 =
# 16*2304M = 36 GiB > an 8-vCPU gw's ~31 GB), read sequentially and buffered.
bench_fio(){ mount_nfs || return 0; disk_guard; local D="$MNT/bench-fio"; sudo mkdir -p "$D"
  echo "== fio WRITE ($FJ jobs x $FSZ, bs=1M, iodepth=$FID, direct/libaio, full pass) =="
  sudo fio --name=w --directory="$D" --rw=write --bs=1M --size="$FSZ" --numjobs="$FJ" --iodepth="$FID" --direct=1 --fallocate=none --ioengine=libaio --group_reporting 2>&1 | grep -iE 'WRITE:'
  disk_guard; sync
  echo "== fio SEQ READ (buffered psync, blobber-served: set ${FJ}x${FSZ} > gw RAM) =="
  sudo fio --name=w --directory="$D" --rw=read --bs=1M --size="$FSZ" --numjobs="$FJ" --ioengine=psync --group_reporting 2>&1 | grep -iE 'READ:'
  sudo rm -rf "$D"; disk_guard; }

# --- mlperf resnet50 (dlio 2.0) over MOUNTPOINT-S3 (the mlperf-mp-s3 read path) --
# NOT the NFS mount: mlperf reads via mount-s3 (FUSE over the gateway S3), same as
# the cluster's mlperf-mp-s3 button.
bench_mlperf(){ : "${AK:?set AK}" "${SK:?set SK}"; mount_nfs
  # dlio imports mpi4py, which dlopen()s libmpi at startup. Distro OpenMPI is NOT
  # on the default PATH/LD_LIBRARY_PATH (Amazon Linux keeps it under
  # /usr/lib64/openmpi), so dlio died with "RuntimeError: cannot load MPI library"
  # during dataset generation and mlperf produced NO numbers at all — the suite
  # printed its two banner lines and moved on. Prepend whichever prefix exists.
  local mpidir
  for mpidir in /usr/lib64/openmpi /usr/lib/x86_64-linux-gnu/openmpi /usr/lib64/mpich; do
    if [ -x "$mpidir/bin/mpirun" ]; then
      PATH="$mpidir/bin:$PATH"
      LD_LIBRARY_PATH="$mpidir/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
      export PATH LD_LIBRARY_PATH
      break
    fi
  done
  if ! /opt/dlio/venv_mlperf/bin/python -c 'from mpi4py import MPI' >/dev/null 2>&1; then
    echo "!! mlperf SKIPPED: no working MPI runtime (dlio needs mpi4py + libmpi)."
    echo "   install it:  sudo dnf install -y openmpi openmpi-devel   (or openmpi-bin libopenmpi-dev)"
    return 0
  fi
  local BKT="${MLPERF_BUCKET:-mlperf-bench}" MPS3="${MPS3_MNT:-/mnt/mps3}" DLIO=/opt/dlio/venv_mlperf/bin/dlio_benchmark
  # EXACT run_bench resnet50 profile (zs3-init line 10936 / train line 11173):
  #   2/1 -> accel 3 / read_threads 12 / prefetch 24 / batch 1200 / ntrain 34*7=238 / neval 64
  #   8/1 -> accel 6 / read_threads 24 / prefetch 48 / batch 1200 / ntrain 136*7=952 / neval 64
  local ACC="${MLPERF_ACCELS:-$EC_ACCEL}" RT="${MLPERF_READ_THREADS:-$EC_RT}" PF="${MLPERF_PREFETCH:-$EC_PF}"
  local BATCH="${MLPERF_BATCH:-1200}" EP="${MLPERF_EPOCHS:-1}"
  local NF="${MLPERF_NUM_FILES:-$(( EC_DATASET_GB * 7 ))}" NE="${MLPERF_NUM_EVAL:-64}"
  export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_REGION=us-east-1
  aws s3 mb "s3://$BKT" --endpoint-url "http://$GW:9000" >/dev/null 2>&1 || true
  local NG="$MNT/$BKT/resnet50"; sudo mkdir -p "$NG"; sudo chown "$(id -u)" "$NG"
  # GENERATE via NFS write-through (train + eval splits) — data lands on the blobbers.
  # REUSE: the 238-file generate is slow AND intermittently hits a dlio/Hydra error,
  # so generate ONCE and KEEP it (MLPERF_KEEP=1, the default); every later accel/rt/pf
  # sweep reuses the SAME dataset instead of regenerating. Skip generate when the
  # train split already holds the expected file count.
  local HAVE; HAVE=$(find "$NG/train" -name '*.tfrecord' 2>/dev/null | wc -l | tr -d ' ')
  if [ "${HAVE:-0}" -ge "$NF" ] && [ "${MLPERF_REGEN:-0}" != 1 ]; then
    echo "== mlperf resnet50: REUSING existing dataset ($HAVE train files in $NG) =="
  else
    echo "== mlperf resnet50: generate $NF train + $NE eval via NFS (~$(( NF*143/1024 ))GiB) =="
    "$DLIO" workload=resnet50_h100 ++workload.dataset.data_folder="$NG" \
      ++workload.dataset.num_files_train="$NF" ++workload.dataset.num_files_eval="$NE" \
      ++workload.workflow.generate_data=True ++workload.workflow.train=False 2>&1 | grep -iE 'Generation done|error' | tail -1
  fi
  disk_guard
  # TRAIN interface: MLPERF_IFACE=mps3 (default, reads via mountpoint-s3 FUSE) or
  # =nfs (reads straight from the NFS/Ganesha mount — the fio read path). accel =
  # mpirun -np; evaluation=False (the correct key, NOT workflow.eval) so /valid is
  # never opened.
  local IFACE="${MLPERF_IFACE:-mps3}" DF
  if [ "$IFACE" = nfs ]; then
    DF="$NG"   # read directly from the NFS mount where generate wrote the data
  else
    sudo mkdir -p "$MPS3"; sudo chown "$(id -u)" "$MPS3"
    mountpoint -q "$MPS3" || mount-s3 --endpoint-url "http://$GW:9000" --allow-delete --allow-overwrite "$BKT" "$MPS3"
    DF="$MPS3/resnet50"
  fi
  local L="$DLIO"; [ "$ACC" -gt 1 ] && L="mpirun --allow-run-as-root --bind-to none --use-hwthread-cpus --oversubscribe -np $ACC $DLIO"
  echo "== mlperf resnet50 TRAIN via $IFACE (accel=$ACC rt=$RT pf=$PF batch=$BATCH epochs=$EP) =="
  $L workload=resnet50_h100 ++workload.dataset.data_folder="$DF" \
    ++workload.dataset.num_files_train="$NF" ++workload.dataset.num_files_eval="$NE" \
    ++workload.workflow.train=True ++workload.workflow.evaluation=False ++workload.workflow.generate_data=False \
    ++workload.reader.read_threads="$RT" ++workload.reader.batch_size="$BATCH" ++workload.reader.prefetch_size="$PF" \
    ++workload.train.epochs="$EP" 2>&1 | grep -iE '\[METRIC\]|samples/second|MB/second' | tail -5
  [ "$IFACE" = nfs ] || fusermount -u "$MPS3" 2>/dev/null || true
  # KEEP the generated dataset by default so sweeps reuse it (MLPERF_KEEP=1). Set
  # MLPERF_KEEP=0 to reclaim the ~34GiB after a one-off run.
  [ "${MLPERF_KEEP:-1}" = 1 ] || sudo rm -rf "$NG" 2>/dev/null || true
  disk_guard; }

case "$WHAT" in
  warp) bench_warp;; ttfb) bench_ttfb;; fio) bench_fio;; mlperf) bench_mlperf;;
  all) bench_ttfb; bench_warp; bench_fio; bench_mlperf;;
  *) echo "usage: $0 <warp|ttfb|fio|mlperf|all>"; exit 1;;
esac
