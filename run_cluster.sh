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
# WARP_CONC overrides the EC-derived warp/ttfb concurrency (e.g. push a 2/1 cluster
# to conc=64 to see if more parallel GET streams lift blobber-served read throughput).
EC_CONC="${WARP_CONC:-$EC_CONC}"
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
  # Start clean. warp PUT uses --keep-data (the GET reads back what PUT wrote),
  # so a killed/hung PUT leaves the full ~EC_DATASET_GB set on the blobbers. That
  # both skews the next GET and — on a disk sized for ONE dataset — starves the
  # very next write into disk_full (which mount-s3/warp then hang on). Purge the
  # warp scratch buckets BEFORE writing, same as bench_fio does for its bucket.
  clean_bkt warpbench warpprobe
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

# --- fio write + read over MOUNTPOINT-S3 (was: NFS mount) --------------------------
# Switched off NFS (2026-08-04): NFS :2049 is VPC-only by security group, so the
# external kit could never run this leg against a real deployment, and the product
# read path being certified is mount-s3 (same as mlperf). mount-s3 constraints
# baked in (each cost a wedge to learn on a live box):
#   * NO --direct/libaio: FUSE has no O_DIRECT; fio's open O_WRONLY without
#     O_TRUNC on an existing key is EPERM under mount-s3, and the default
#     layout-then-reopen pattern deadlocked buffered writes with ZERO bytes on
#     the wire. --create_on_open=1 makes the measured write BE the file create.
#   * psync engine both legs; end_fsync=1 so upload-on-close completion is
#     inside the timed window (otherwise the last part is untimed).
#   * RAM guard: mount-s3 buffers in-flight parts in memory; 6 concurrent 1 GiB
#     streams wedged a 3 GB client (I/O errors, mount needed a remount). Cap
#     jobs at (RAM_GB) with a floor of 2.
#   * unique run dir: mp-s3 keys can't be re-laid-out in place.
# READ: sequential buffered psync — same cold-by-working-set rationale as before
# (set > gateway RAM), served via ranged GETs + mount-s3 readahead.
bench_fio(){ : "${AK:?set AK}" "${SK:?set SK}"
  local BKT="${FIO_BUCKET:-bench-fio}" MP="${MPS3_FIO_MNT:-/mnt/mps3-fio}"
  local RAMG NJ; RAMG=$(awk '/MemTotal/{printf "%d", $2/1048576}' /proc/meminfo)
  NJ=$FJ; [ "$RAMG" -lt "$NJ" ] && { NJ=$RAMG; [ "$NJ" -lt 2 ] && NJ=2
    echo "  [fio] client has ${RAMG}G RAM -> $NJ jobs instead of $FJ (mount-s3 buffer guard)"; }
  # mount-s3 CONCURRENCY cap (separate from the RAM guard): one mount-s3 daemon
  # stalls its multipart-upload COMPLETION path under many big concurrent files.
  # Observed on a 16-vCPU/32 GiB box: 16 x 2.3 GiB writes finished to the FUSE
  # mount but mount-s3 only uploaded 9/16, wedging the rest in fio's end_fsync
  # with zero network I/O on either side. Cap concurrent writers (RAM was not
  # the limit — it was mount-s3's upload concurrency). Tune via MPS3_FIO_MAX_JOBS.
  local MPCAP="${MPS3_FIO_MAX_JOBS:-4}"
  [ "$NJ" -gt "$MPCAP" ] && { echo "  [fio] mount-s3 concurrency cap: $NJ -> $MPCAP writers (MPS3_FIO_MAX_JOBS)"; NJ=$MPCAP; }
  command -v mount-s3 >/dev/null 2>&1 || { echo "!! fio SKIPPED: mount-s3 not installed"; return 0; }
  AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" aws --endpoint-url "http://$GW:9000" s3 mb "s3://$BKT" >/dev/null 2>&1 || true
  # Start each fio run against a CLEAN bucket. mount-s3 stalls its upload
  # pipeline writing into a bucket carrying accumulated data from prior runs
  # (a killed run leaves GiBs behind, and the next fio then hangs even at low
  # concurrency). Purge the bench bucket's contents first — objects here are
  # scratch, regenerated every run.
  AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" aws --endpoint-url "http://$GW:9000" s3 rm "s3://$BKT/" --recursive --only-show-errors >/dev/null 2>&1 || true
  sudo mkdir -p "$MP"
  grep -qs '^user_allow_other' /etc/fuse.conf || \
    echo user_allow_other | sudo tee -a /etc/fuse.conf >/dev/null 2>&1 || true
  # Always remount: `mountpoint -q` reports a stale FUSE endpoint as mounted.
  fusermount -u "$MP" 2>/dev/null || sudo umount -l "$MP" 2>/dev/null || true
  # mount-s3 REFUSES a non-empty mountpoint ("fuse: mountpoint is not empty").
  # If a prior run (or a manual use of this path as a plain dir) left files
  # behind, recreate the mountpoint clean. Guarded: only when it is NOT a live
  # mount, so we never rm through an active FUSE endpoint.
  if ! mountpoint -q "$MP" && [ -n "$(ls -A "$MP" 2>/dev/null)" ]; then
    sudo rm -rf "$MP" && sudo mkdir -p "$MP"
  fi
  if ! sudo -E env AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" \
       mount-s3 --allow-other --force-path-style --endpoint-url "http://$GW:9000" \
       --allow-delete --allow-overwrite --metadata-ttl minimal "$BKT" "$MP"; then
    echo "!! fio SKIPPED: could not mount s3://$BKT at $MP via mountpoint-s3"; return 0
  fi
  local D="$MP/bench-fio-$$"; sudo mkdir -p "$D"
  echo "== fio WRITE ($NJ jobs x $FSZ, bs=1M, psync/create_on_open, mount-s3) =="
  # HARD TIMEOUT: mp-s3 FUSE write of large files can WEDGE — fio hangs in end_fsync
  # while mount-s3 uploads 0 bytes (measured 2026-08-05: 4.2 MB/s on a 4xlarge
  # gateway, full wedge on a c6in.large). Cap it so the suite can NEVER hang; on
  # timeout, say so and point at native S3 (the warp PUT above) for real write BW.
  local FIO_WR_TO="${FIO_WRITE_TIMEOUT:-180}"
  # --percentile_list=50:99 + grep the clat line so the summary can report clat p50/p99
  # (previously only the WRITE:/READ: bandwidth line was kept, so clat showed n/a).
  if ! sudo timeout -s KILL "$FIO_WR_TO" fio --name=w --directory="$D" --rw=write --bs=1M --size="$FSZ" --numjobs="$NJ" \
       --ioengine=psync --create_on_open=1 --fallocate=none --end_fsync=1 --percentile_list=50:99 --group_reporting 2>&1 | grep -iE 'WRITE:|clat perc|th=\['; then
    echo "  !! fio mp-s3 WRITE stalled/timed out (>${FIO_WR_TO}s) — mount-s3 FUSE write wedges on large files; use native S3 (warp PUT above) for write throughput"
  fi
  echo "== fio SEQ READ (buffered psync, blobber-served: set ${NJ}x${FSZ} > gw RAM) =="
  sudo fio --name=w --directory="$D" --rw=read --bs=1M --size="$FSZ" --numjobs="$NJ" \
    --ioengine=psync --fallocate=none --percentile_list=50:99 --group_reporting 2>&1 | grep -iE 'READ:|clat perc|th=\['
  sudo rm -rf "$D"
  fusermount -u "$MP" 2>/dev/null || sudo umount -l "$MP" 2>/dev/null || true; }

# --- mlperf resnet50 (dlio 2.0) over MOUNTPOINT-S3 (the mlperf-mp-s3 read path) --
# NOT the NFS mount: mlperf reads via mount-s3 (FUSE over the gateway S3), same as
# the cluster's mlperf-mp-s3 button.
bench_mlperf(){ : "${AK:?set AK}" "${SK:?set SK}"
  # mount_nfs's return value was DISCARDED here (bench_fio checks it, mlperf did
  # not). With no NFS client installed the mount fails, `sudo mkdir -p "$NG"`
  # then creates a plain LOCAL directory under the mountpoint, and dlio generates
  # the whole dataset onto the client's own disk — 30 GiB of it, reported as
  # "mlperf write-NFS: N MB/s" as though it had been written to the Blimp node.
  # A cluster write measurement that never touched the cluster. Skip instead.
  # 2026-08-04: mlperf now writes AND reads over mountpoint-s3 (no NFS) — the
  # datagen is mounted-s3 too, not just the train read. NFS :2049 is VPC-only
  # and being retired; mp-s3 is the certified product path. mount-s3 must exist.
  command -v mount-s3 >/dev/null 2>&1 || { echo "!! mlperf SKIPPED: mount-s3 not installed (run 'blimp --setup')"; return 0; }
  # dlio imports mpi4py, which dlopen()s libmpi at startup. Distro OpenMPI is NOT
  # on the default PATH/LD_LIBRARY_PATH (Amazon Linux keeps it under
  # /usr/lib64/openmpi), so dlio died with "RuntimeError: cannot load MPI library"
  # during dataset generation and mlperf produced NO numbers at all — the suite
  # printed its two banner lines and moved on. Prepend whichever prefix exists.
  # dlio's TFRecord generator shells out to `tfrecord2idx`, which ships with
  # NVIDIA DALI (a CUDA package with no place on a CPU-only client). Without it
  # generation died with FileNotFoundError before producing a single number. The
  # kit carries its own implementation — put the kit dir on PATH so dlio finds it.
  # NOTE: resolve the kit dir HERE. This script runs under `set -u` and has no
  # $HERE of its own (that lives in test_cache.sh), so referencing it aborted the
  # whole leg with "HERE: unbound variable" before mlperf ran a single step.
  local kitdir; kitdir="$(cd "$(dirname "$0")" && pwd)"
  PATH="$kitdir:$PATH"; export PATH
  # Hydra hides the real exception behind "Set the environment variable
  # HYDRA_FULL_ERROR=1 for a complete stack trace", so a failed generate/train
  # reported only that sentence plus mpirun's "exit code 1" — useless. Always on:
  # this is a benchmark, a full traceback costs nothing and saves a whole run.
  export HYDRA_FULL_ERROR=1
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
  # MEMORY GUARD, copied from the cluster (zs3-init.go run_bench). dlio's datagen
  # writes through /dev/shm and `mpirun -np ACCEL` spawns ACCEL torch ranks; none
  # of it is memory-bounded, so a run can exhaust RAM and wedge the whole box —
  # which is exactly what happened here (the client stopped answering SSH mid
  # training). Run every dlio invocation in a transient systemd scope capped at a
  # fraction of RAM, with swap off so it cannot thrash. An overrun then OOM-kills
  # only the dlio tree and the leg fails cleanly. Datagen gets the higher cap: it
  # is sequential, but streaming large files accrues page cache in the cgroup.
  local MEMKB; MEMKB=$(awk '/MemTotal/{print $2}' /proc/meminfo 2>/dev/null || echo 0)
  local MLPERF_MEM_CAP="${MLPERF_MEM_CAP:-$(( MEMKB * 45 / 100 ))K}"
  local MLPERF_GEN_MEM_CAP="${MLPERF_GEN_MEM_CAP:-$(( MEMKB * 85 / 100 ))K}"
  # Carry PATH/LD_LIBRARY_PATH INTO the scope. `sudo systemd-run` starts a fresh
  # environment (sudo resets PATH via secure_path, systemd-run does not inherit),
  # so the OpenMPI prefix prepended above was invisible inside the scope and the
  # train step died with "Failed to find executable mpirun: No such file or
  # directory" even though mpirun was installed. Pass them through explicitly.
  MEMRUN(){ local cap="$1"; shift
    if command -v systemd-run >/dev/null 2>&1 && [ "${MEMKB:-0}" -gt 0 ]; then
      sudo systemd-run --scope --quiet -p MemoryMax="$cap" -p MemorySwapMax=0 -- \
        env PATH="$PATH" LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}" HYDRA_FULL_ERROR=1 "$@"
    else "$@"; fi; }
  # Stale OpenMPI session dirs make mpirun fail on a rerun; the cluster clears
  # them before every generate.
  rm -rf /tmp/ompi.* /tmp/openmpi-sessions-* /tmp/pmix.* 2>/dev/null || true
  # EXACT run_bench resnet50 profile (zs3-init line 10936 / train line 11173):
  #   2/1 -> accel 3 / read_threads 12 / prefetch 24 / batch 1200 / ntrain 34*7=238 / neval 64
  #   8/1 -> accel 6 / read_threads 24 / prefetch 48 / batch 1200 / ntrain 136*7=952 / neval 64
  # TUNING NOTE (2026-08-04, cluster 1785851761080 / c6in.4xlarge, 66 GB set over
  # mp-s3): the EC-2/1 default (accel 3 / rt 12 / pf 24) is AU-bound and reads
  #   1682 MB/s (AU 98%). Bumping to accel 4 / rt 16 / pf 32 lifts it to ~2164 MB/s
  #   (AU 96%, ~15.4k -> ~19.8k samples/s) — more torch ranks + read threads +
  #   prefetch depth saturate the read path harder. Override for that run with:
  #     MLPERF_ACCELS=4 MLPERF_READ_THREADS=16 MLPERF_PREFETCH=32
  #   (needs a >=16 vCPU client, else the accel-1 mount-s3 OOM guard below forces 1).
  #   ACID on/off makes ~no difference here (mlperf is AU-bound; see
  #   devOps/0chain-debug/zs3_acid_concurrent_download_corruption_20260804.md).
  local ACC="${MLPERF_ACCELS:-$EC_ACCEL}" RT="${MLPERF_READ_THREADS:-$EC_RT}" PF="${MLPERF_PREFETCH:-$EC_PF}"
  # ACCEL 1 ON ANY CLIENT SMALLER THAN A *.4xlarge (16 vCPU).
  # The EC-derived profile (2/1 -> accel 3) is the CLUSTER's run_bench profile and
  # assumes a cluster-sized driver. On a small client it runs `mpirun -np 3`: three
  # torch ranks x RT read threads all pulling through ONE mount-s3 daemon, which
  # lives OUTSIDE the dlio memory cgroup. On a c6in.large (2 vCPU / 3.8 GB) that
  # drove mount-s3 to 1.58 GB RSS and the kernel global-OOM-killed it mid-train:
  #   Out of memory: Killed process 29520 (mount-s3) anon-rss:1579852kB
  #   -> every rank: "Transport endpoint is not connected", no [METRIC] at all.
  # accel=1 also skips mpirun entirely (see the `ACC > 1` guard below), so there is
  # no MPI oversubscription of a 2-core box either. MLPERF_ACCELS still overrides.
  local NCPU; NCPU=$(nproc 2>/dev/null || echo 1)
  if [ -z "${MLPERF_ACCELS:-}" ] && [ "${NCPU:-1}" -lt 16 ]; then
    [ "$ACC" -gt 1 ] && echo "  [mlperf] client has $NCPU vCPU (< 16 = *.4xlarge) -> accel 1 instead of $ACC (mount-s3 OOM guard)"
    ACC=1
  fi
  # GATEWAY-SIZE guard: mlperf reads are SERVED by the gateway, so a small gateway
  # (e.g. c6in.large, 2 vCPU) can't feed accel>1 — the train stalls waiting for data
  # (AU ~45%). accel 1 on a gateway smaller than .4xlarge (<16 vCPU), EC accel (3/4/6)
  # on a .4xlarge or bigger. Needs CLUSTER_ID + the box's IAM ec2:DescribeInstances.
  if [ -z "${MLPERF_ACCELS:-}" ] && [ -n "${CLUSTER_ID:-}" ] && command -v aws >/dev/null 2>&1; then
    local _reg GWTYPE
    _reg=$(curl -s --max-time 3 http://169.254.169.254/latest/meta-data/placement/region 2>/dev/null)
    GWTYPE=$(aws ec2 describe-instances --region "${_reg:-${REGION:-us-east-1}}" \
      --filters "Name=tag:Name,Values=cluster-${CLUSTER_ID}-zs3server" "Name=instance-state-name,Values=running" \
      --query 'Reservations[].Instances[].InstanceType' --output text 2>/dev/null | head -1)
    case "$GWTYPE" in
      *.large|*.xlarge|*.2xlarge)
        [ "$ACC" -gt 1 ] && echo "  [mlperf] gateway $GWTYPE (< .4xlarge) serves the reads -> accel 1 instead of $ACC (small-gateway read guard)"
        ACC=1 ;;
    esac
  fi
  local BATCH="${MLPERF_BATCH:-1200}" EP="${MLPERF_EPOCHS:-1}"
  local NF="${MLPERF_NUM_FILES:-$(( EC_DATASET_GB * 7 ))}" NE="${MLPERF_NUM_EVAL:-64}"
  export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_REGION=us-east-1
  aws s3 mb "s3://$BKT" --endpoint-url "http://$GW:9000" >/dev/null 2>&1 || true
  # Mount the mlperf bucket over mountpoint-s3 UP FRONT — datagen writes into it
  # and train reads from it, so the whole workload is mp-s3 (no NFS). Always
  # remount: `mountpoint -q` reports a stale FUSE endpoint as mounted.
  sudo mkdir -p "$MPS3"; sudo chown "$(id -u)" "$MPS3"
  grep -qs '^user_allow_other' /etc/fuse.conf || echo user_allow_other | sudo tee -a /etc/fuse.conf >/dev/null 2>&1 || true
  fusermount -u "$MPS3" 2>/dev/null || sudo umount -l "$MPS3" 2>/dev/null || true
  # mount-s3 refuses a non-empty mountpoint (it fails with a confusing "No such
  # file or directory (os error 2)" here). A prior run's `mkdir -p $MPS3/resnet50`
  # can leave a LOCAL resnet50/ dir when $MPS3 was unmounted, so recreate the
  # mountpoint clean when it is not a live mount.
  if ! mountpoint -q "$MPS3" && [ -n "$(sudo ls -A "$MPS3" 2>/dev/null)" ]; then
    sudo rm -rf "$MPS3" && sudo mkdir -p "$MPS3"
  fi
  if ! sudo -E env AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" \
       mount-s3 --allow-other --force-path-style --endpoint-url "http://$GW:9000" \
       --allow-delete --allow-overwrite --metadata-ttl minimal "$BKT" "$MPS3"; then
    echo "!! mlperf SKIPPED: could not mount s3://$BKT at $MPS3 via mountpoint-s3"; return 0
  fi
  local NG="$MPS3/resnet50"; sudo mkdir -p "$NG"
  # GENERATE over mountpoint-s3 (train + eval splits) — data lands on the blobbers.
  # REUSE: the 238-file generate is slow AND intermittently hits a dlio/Hydra error,
  # so generate ONCE and KEEP it (MLPERF_KEEP=1, the default); every later accel/rt/pf
  # sweep reuses the SAME dataset instead of regenerating. Skip generate when the
  # train split already holds the expected file count.
  local HAVE; HAVE=$(find "$NG/train" -name '*.tfrecord' 2>/dev/null | wc -l | tr -d ' ')
  if [ "${HAVE:-0}" -ge "$NF" ] && [ "${MLPERF_REGEN:-0}" != 1 ]; then
    echo "== mlperf resnet50: REUSING existing dataset ($HAVE train files in $NG) =="
  else
    echo "== mlperf resnet50: generate $NF train + $NE eval (~$(( NF*143/1024 ))GiB), native-S3 upload =="
    # 🚨 DO NOT generate straight into the mp-s3 mount. mountpoint-s3 FUSE WRITE of
    # large files from a client WEDGES / is ~200x slower than native S3 (measured
    # 2026-08-05: native warp 913 MB/s vs mp-s3 fio 4.2 MB/s on a c6in.4xlarge
    # gateway, and mp-s3 fully WEDGED — 0 bytes uploaded, fio stuck in fsync — on a
    # c6in.large gateway). dlio-over-mp-s3 datagen therefore stalled the whole
    # suite. Instead: generate to a LOCAL scratch dir (fast local disk), then upload
    # via `aws s3 cp` (native S3 MULTIPART, the same fast path warp uses). TRAIN
    # below still READS via mp-s3 ($NG) — mp-s3 READ is fine, only WRITE wedges.
    local g0 g1 gsec gbytes LG
    LG="${MLPERF_LOCAL_GEN:-/var/tmp/mlperf-gen}/resnet50"
    sudo rm -rf "$LG" 2>/dev/null; sudo mkdir -p "$LG"; sudo chown -R "$(id -u)" "$(dirname "$LG")"
    g0=$(date +%s)
    MEMRUN "$MLPERF_GEN_MEM_CAP" "$DLIO" workload=resnet50_h100 ++workload.dataset.data_folder="$LG" \
      ++workload.dataset.num_files_train="$NF" ++workload.dataset.num_files_eval="$NE" \
      ++workload.workflow.generate_data=True ++workload.workflow.train=False 2>&1 | grep -iE 'Generation done|error' | tail -1
    # native-S3 multipart upload of the whole generated tree into the bucket (fast);
    # train then reads it back through the mp-s3 mount at $NG.
    aws s3 cp "$LG/" "s3://$BKT/resnet50/" --recursive --endpoint-url "http://$GW:9000" --only-show-errors 2>&1 | tail -2
    g1=$(date +%s); gsec=$(( g1 - g0 )); [ "$gsec" -lt 1 ] && gsec=1
    gbytes=$(du -sb "$LG" 2>/dev/null | awk '{print $1+0}')
    [ "${gbytes:-0}" -gt 0 ] && \
      echo "  mlperf gen+native-upload: $(( gbytes / gsec / 1000000 )) MB/s (${gsec}s, $(( gbytes / 1048576 )) MiB)"
    sudo rm -rf "$LG" 2>/dev/null   # dataset now lives in the bucket; reclaim client scratch
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
    # ALWAYS unmount first. `mountpoint -q` reports a STALE FUSE endpoint as
    # mounted, so "mount only if not mounted" skipped the remount and training
    # then read through a dead mount:
    #   FailedPreconditionError: /mnt/mps3/resnet50/train/img_001_of_476.tfrecord;
    #   Transport endpoint is not connected
    # A previous run's cleanup (or a killed mount-s3) leaves exactly that behind,
    # so every subsequent mlperf run failed until the box was rebooted.
    fusermount -u "$MPS3" 2>/dev/null || sudo umount -l "$MPS3" 2>/dev/null || true
    # Non-empty mountpoint guard (mount-s3 refuses it — see the datagen mount).
    if ! mountpoint -q "$MPS3" && [ -n "$(sudo ls -A "$MPS3" 2>/dev/null)" ]; then
      sudo rm -rf "$MPS3" && sudo mkdir -p "$MPS3"
    fi
    # Mount as ROOT with --allow-other. dlio runs inside the memory scope via
    # `sudo systemd-run`, i.e. as root, and a FUSE mount is private to the user
    # that created it — so an ec2-user mount gave every rank
    #   PermissionError: [Errno 13] Permission denied: '/mnt/mps3/resnet50/train'
    # The cluster never sees this because its whole driver runs as root.
    # --allow-other needs user_allow_other in /etc/fuse.conf; add it if absent.
    grep -qs '^user_allow_other' /etc/fuse.conf || \
      echo user_allow_other | sudo tee -a /etc/fuse.conf >/dev/null 2>&1 || true
    # --force-path-style: without it, mount-s3 defaults to virtual-hosted-style
    # addressing (bucket prepended to the host, e.g. "$BKT.$GW"). That works by
    # accident when $GW is a bare IP (SDKs special-case IPs to path-style), which
    # is why this passed in vpc mode — but in external mode $GW is a real
    # hostname (zus-<id>-0.zus.network) and "$BKT.zus-<id>-0.zus.network" was
    # never provisioned in DNS, so every request failed with
    # AWS_IO_DNS_INVALID_NAME (cluster 1785632294645, cross-region client,
    # 2026-08-02). Path-style is correct for a MinIO-compatible endpoint either
    # way, so force it unconditionally rather than branch on IP-vs-hostname.
    if ! sudo -E env AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" \
         mount-s3 --allow-other --force-path-style --endpoint-url "http://$GW:9000" --allow-delete --allow-overwrite "$BKT" "$MPS3"; then
      echo "!! mlperf SKIPPED: could not mount s3://$BKT at $MPS3 via mountpoint-s3"
      return 0
    fi
    # Prove the mount is actually readable before handing it to dlio — a mount
    # that reports success but cannot list is the same failure one step later.
    if ! sudo ls "$MPS3" >/dev/null 2>&1; then
      echo "!! mlperf SKIPPED: $MPS3 mounted but is not readable (stale endpoint?)"
      fusermount -u "$MPS3" 2>/dev/null || true
      return 0
    fi
    DF="$MPS3/resnet50"
  fi
  # Absolute mpirun: PATH lookups have already bitten us once inside the memory
  # scope, and a wrong/absent mpirun fails the leg minutes in rather than at once.
  local MPIRUN; MPIRUN=$(command -v mpirun || echo /usr/lib64/openmpi/bin/mpirun)
  local L="$DLIO"; [ "$ACC" -gt 1 ] && L="$MPIRUN --allow-run-as-root --bind-to none --use-hwthread-cpus --oversubscribe -np $ACC $DLIO"
  echo "== mlperf resnet50 TRAIN via $IFACE (accel=$ACC rt=$RT pf=$PF batch=$BATCH epochs=$EP) =="
  # Capture, don't blind-grep. Piping straight into `grep [METRIC]` means a train
  # step that CRASHES prints absolutely nothing — which is exactly what happened
  # (2026-08-01): generation succeeded, training died, and the leg emitted only
  # its banner, so the suite looked like it had simply produced no numbers. Show
  # the metrics on success; show the tail of the real failure otherwise.
  local tout trc
  tout=$(MEMRUN "$MLPERF_MEM_CAP" $L workload=resnet50_h100 ++workload.dataset.data_folder="$DF" \
    ++workload.dataset.num_files_train="$NF" ++workload.dataset.num_files_eval="$NE" \
    ++workload.workflow.train=True ++workload.workflow.evaluation=False ++workload.workflow.generate_data=False \
    ++workload.reader.read_threads="$RT" ++workload.reader.batch_size="$BATCH" ++workload.reader.prefetch_size="$PF" \
    ++workload.train.epochs="$EP" 2>&1); trc=$?
  if printf '%s\n' "$tout" | grep -qiE '\[METRIC\]|samples/second|MB/second'; then
    printf '%s\n' "$tout" | grep -iE '\[METRIC\]|samples/second|MB/second' | tail -8
    # AU% is THE MLPerf Storage verdict — it is what says whether the storage kept
    # the accelerator fed (>=90% is the passing bar), and it was buried in a
    # tail -5 of raw dlio lines that could push it off. Pull the three headline
    # numbers out by name and restate them on one line. Value = everything after
    # the last ": ", so it survives dlio changing the label's word count.
    local au sm io
    # dlio prints "<value> (<stddev>)" — keep the value, drop the parenthesised
    # stddev, or the summary reads "AU 64.6854 (0.0000)%".
    metval(){ tail -1 | sed "s/.*: *//; s/ *(.*//"; }
    au=$(printf '%s\n' "$tout" | grep -i "Accelerator Utilization" | metval)
    sm=$(printf '%s\n' "$tout" | grep -iE "Training Throughput.*samples" | metval)
    io=$(printf '%s\n' "$tout" | grep -iE "Training I/O Throughput" | metval)
    echo "  mlperf read ($IFACE, accel=$ACC rt=$RT pf=$PF): AU ${au:-n/a}% · ${sm:-n/a} samples/s · ${io:-n/a} MB/s"
  else
    echo "!! mlperf TRAIN produced no metrics (exit $trc) — last output:"
    printf '%s\n' "$tout" | tail -12 | sed 's/^/   | /'
  fi
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
