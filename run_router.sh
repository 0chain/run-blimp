#!/usr/bin/env bash
# run_router.sh — read-through storage-cache benchmark.
#
# Reads the SAME dataset (a TPC-DS table prefix, e.g. store_returns ~20 GB) FOUR
# ways and compares throughput:
#   1. direct-S3     : straight from the external S3 origin bucket (baseline)
#   2. cache-S3      : through the cluster read-through Router (:8088) — S3 interface
#   3. cache-mp-s3   : the cached objects via mountpoint-s3 (FUSE) — needs GW_AK/GW_SK
#   4. cache-NFS     : the same cached objects via the gateway NFS/Ganesha mount
#
# The Router (zus-router :8088) fronts the origin bucket: on a miss it fetches from
# the origin and writes the object into the gateway's eblobber-backed cache bucket;
# on a hit it serves from there. Because the cache bucket IS a gateway S3 bucket, the
# cached objects are also visible on the NFS mount at <NFS_MNT>/<BUCKET>/<key> — that
# is the cache-NFS path (no second copy).
#
# Everything runs over PRIVATE addressing: the box→gateway on the VPC private IP, and
# box/gateway→S3 through the VPC S3 gateway endpoint (in-region, no egress). The box
# authenticates to S3 with its IAM instance role; the Router uses the gateway's role.
#
# Prereqs (done by the VPC/cache setup): VPC S3 gateway endpoint; box has an IAM
# instance profile with s3:GetObject on the bucket; Router repointed at the bucket:
#   docker run … 0chaindev/router:latest -listen :8088 \
#     -cache-endpoint http://127.0.0.1:9000 -cache-access-key <MU> -cache-secret-key <MP> \
#     -s3-endpoint https://s3.<region>.amazonaws.com -s3-region <region> \
#     -bucket <BUCKET> -cache-buckets <BUCKET>            # empty -s3-access-key => IMDS role
#
# Usage:
#   GW=10.10.150.76 ORIGIN_BUCKET=blimp-tpcds1000-aps1 REGION=ap-south-1 \
#   TABLE=store_returns ./run_router.sh
# Optional: ROUTER (http://$GW:8088), NFS_MNT (/mnt/zusnfs), PAR (parallel readers),
#           MAX_BYTES (cap the read set; default = whole prefix), COLD_FILL (1=time
#           the cold cache-fill too), GW_AK/GW_SK (gateway minio creds — enables the
#           cache-mp-s3 path via mountpoint-s3 of the gateway cache bucket).

set -uo pipefail
GW="${GW:?set GW (gateway private ip)}"
BKT="${ORIGIN_BUCKET:?set ORIGIN_BUCKET}"
REGION="${REGION:-ap-south-1}"
# TABLES: space-separated prefixes. Default store_sales+catalog_sales = ~163 GiB,
# 5x the 32 GiB gateway/client RAM — warm reads CANNOT be page-cache-served
# (store_returns alone was 18.6 GiB < RAM and measured RAM, not the cache).
TABLES="${TABLES:-${TABLE:-store_sales catalog_sales}}"
TABLE="${TABLES%% *}"   # first prefix — kept for the summary/JSON label
ROUTER="${ROUTER:-http://$GW:8088}"
MNT="${NFS_MNT:-/mnt/zusnfs}"
# These readers are NETWORK-bound (S3 GET / NFS read), not CPU-bound, so sizing
# concurrency off core count throttles a small node for no reason: on a 2-vCPU
# client PAR came out 4 while warp drove the SAME gateway at concurrency 16 and
# measured 574 MiB/s, versus 26 MB/s here. Floor it at 16 so the comparison is
# against the cache, not against the client's core count.
PAR="${PAR:-$(( $(nproc) * 2 < 16 ? 16 : $(nproc) * 2 ))}"
MAX_BYTES="${MAX_BYTES:-0}"   # 0 = read the whole prefix

# drop the CLIENT page/dentry cache + a fresh NFS mount so every warm path is
# measured against the CLUSTER, never local RAM.
drop_client_cache(){ sync; echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null 2>&1 || true; }
remount_nfs(){ sudo umount -l "$MNT" 2>/dev/null || true; mount_nfs; }

mbps(){ awk -v b="$1" -v s="$2" 'BEGIN{ if(s<=0)s=0.0001; printf "%.0f", b/1e6/s }'; }
now(){ date +%s.%N; }
el(){ awk -v a="$1" -v z="$2" 'BEGIN{d=z-a; if(d<0)d=0; printf "%.2f", d}'; }

mount_nfs(){ mountpoint -q "$MNT" && return 0; sudo mkdir -p "$MNT"
  sudo mount -t nfs4 -o nconnect=16,rsize=1048576,wsize=1048576 "$GW":/ "$MNT"; }

echo "[cache-bench] origin=s3://$BKT/{$TABLES}/  region=$REGION  router=$ROUTER  par=$PAR"

# --- enumerate the object set (key<TAB>size), optionally capped to MAX_BYTES -------
MAP=""
for T in $TABLES; do
  MAP="$MAP$(aws s3 ls "s3://$BKT/$T/" --recursive --region "$REGION" 2>/dev/null | awk '$3+0>0{print $4"\t"$3}')
"
done
MAP=$(printf '%s' "$MAP" | awk 'NF')
[ -n "$MAP" ] || { echo "no objects under s3://$BKT/{$TABLES}/ (creds/endpoint?)"; exit 1; }
if [ "$MAX_BYTES" -gt 0 ]; then
  MAP=$(echo "$MAP" | awk -v cap="$MAX_BYTES" 'BEGIN{a=0} {if(a>=cap)exit; print; a+=$2}')
fi
KEYS=$(echo "$MAP" | cut -f1)
NOBJ=$(echo "$KEYS" | wc -l | tr -d ' ')
BYTES=$(echo "$MAP" | awk '{s+=$2}END{print s+0}')
echo "[cache-bench] set: $NOBJ objects, $(awk -v b="$BYTES" 'BEGIN{printf "%.1f", b/1073741824}') GiB"

# ============================ 1) DIRECT S3 (origin) ===============================
# Straight GET from the origin bucket to /dev/null, parallel. VPC S3 endpoint keeps
# it private + in-region; the box IAM role authenticates.
t0=$(now)
echo "$KEYS" | xargs -P"$PAR" -I{} aws s3api get-object --bucket "$BKT" --key "{}" \
  --region "$REGION" /dev/stdout >/dev/null 2>&1
t1=$(now); DIRECT=$(mbps "$BYTES" "$(el "$t0" "$t1")"); DIRECT_S=$(el "$t0" "$t1")
echo "  direct-S3 : $DIRECT MB/s (${DIRECT_S}s)"

# ============================ 2) CACHE FILL (cold) ================================
# FULL-object GET through the Router so the whole object is fetched from origin and
# written into the gateway cache bucket (a range GET would only cache a slice, so the
# NFS path below would miss). This is the COLD read (origin -> cache). Timed only if
# COLD_FILL=1 (it includes the origin fetch, so it's ~direct + cache-write).
t0=$(now)
echo "$KEYS" | xargs -P"$PAR" -I{} curl -s -m3600 -o /dev/null "$ROUTER/$BKT/{}"
t1=$(now); FILL_S=$(el "$t0" "$t1")
[ "${COLD_FILL:-0}" = 1 ] && echo "  cache-fill: $(mbps "$BYTES" "$FILL_S") MB/s (${FILL_S}s, cold origin->cache)"

# ============================ 3) CACHE-S3 (warm) =================================
# Second GET of the same set through the Router — now served from the eblobber cache.
drop_client_cache
t0=$(now)
echo "$KEYS" | xargs -P"$PAR" -I{} curl -s -m3600 -o /dev/null "$ROUTER/$BKT/{}"
t1=$(now); WARM_S3=$(mbps "$BYTES" "$(el "$t0" "$t1")"); WARM_S3_S=$(el "$t0" "$t1")
echo "  cache-S3  : $WARM_S3 MB/s (${WARM_S3_S}s, router->eblobber cache)"

# ============================ 3b) CACHE-mp-s3 (warm) =============================
# Read the cached objects via mountpoint-s3 (FUSE-over-S3) of the gateway cache
# bucket. Needs the GATEWAY minio creds (GW_AK/GW_SK) to mount the cache bucket on
# :9000. Skipped if mount-s3 or the gateway creds are unavailable.
# HONEST BYTES: throughput = bytes of objects that actually EXIST on the view ÷
# time (2026-07-22: dividing the FULL set by wall time while dd silently no-opped
# on missing objects reported 8501 MB/s — 68 Gbit/s on a 25 Gbps NIC, impossible).
WARM_MPS3="n/a"
if command -v mount-s3 >/dev/null 2>&1 && [ -n "${GW_AK:-}" ]; then
  MPS3=/mnt/mps3_cache; sudo mkdir -p "$MPS3"; sudo chown "$(id -u)" "$MPS3"; fusermount -u "$MPS3" 2>/dev/null || true
  if AWS_ACCESS_KEY_ID="$GW_AK" AWS_SECRET_ACCESS_KEY="$GW_SK" mount-s3 --endpoint-url "http://$GW:9000" --region us-east-1 "$BKT" "$MPS3" 2>/dev/null; then
    drop_client_cache
    HITMAP=$(echo "$MAP" | awk -v m="$MPS3" -F'\t' '{ if (system("test -f \"" m "/" $1 "\"")==0) print }')
    HITB=$(echo "$HITMAP" | awk -F'\t' '{s+=$2}END{print s+0}'); HITN=$(echo "$HITMAP" | awk 'NF' | wc -l | tr -d ' ')
    [ "$HITN" -lt "$NOBJ" ] && echo "  (mp-s3: only $HITN/$NOBJ objects present — measuring those $(awk -v b="$HITB" 'BEGIN{printf "%.1f",b/1073741824}') GiB)"
    t0=$(now); echo "$HITMAP" | cut -f1 | xargs -P"$PAR" -I{} dd if="$MPS3/{}" of=/dev/null bs=1M 2>/dev/null
    t1=$(now); WARM_MPS3=$(mbps "$HITB" "$(el "$t0" "$t1")")
    fusermount -u "$MPS3" 2>/dev/null || true
    echo "  cache-mp-s3: $WARM_MPS3 MB/s (mountpoint-s3 over cache bucket, $HITN objs)"
  fi
fi

# ============================ 4) CACHE-NFS (warm) ================================
# The same cached objects live at <MNT>/<BKT>/<key> (the cache bucket is a gateway S3
# bucket == NFS namespace). Read them via Ganesha to /dev/null. Fresh mount + client
# cache drop; throughput over the bytes that are actually present (see 3b note).
remount_nfs
drop_client_cache
NHITMAP=$(echo "$MAP" | awk -v m="$MNT/$BKT" -F'\t' '{ if (system("test -f \"" m "/" $1 "\"")==0) print }')
NHITB=$(echo "$NHITMAP" | awk -F'\t' '{s+=$2}END{print s+0}'); NHITN=$(echo "$NHITMAP" | awk 'NF' | wc -l | tr -d ' ')
[ "$NHITN" -lt "$NOBJ" ] && echo "  (note: only $NHITN/$NOBJ objects present on NFS — measuring those $(awk -v b="$NHITB" 'BEGIN{printf "%.1f",b/1073741824}') GiB)"
t0=$(now)
echo "$NHITMAP" | cut -f1 | xargs -P"$PAR" -I{} sh -c 'dd if="$1/$2" of=/dev/null bs=1M 2>/dev/null' _ "$MNT/$BKT" "{}"
t1=$(now); WARM_NFS=$(mbps "$NHITB" "$(el "$t0" "$t1")"); WARM_NFS_S=$(el "$t0" "$t1")
echo "  cache-NFS : $WARM_NFS MB/s (${WARM_NFS_S}s, eblobber cache via Ganesha, $NHITN objs)"

# ===================== is the cache even TURNED ON? ==============================
# The router only caches buckets listed in its -cache-buckets flag. `blimp --setup`
# wires the gateway's SOURCE over the admin API but leaves the cache retarget
# optional (hookup_cluster_source.sh), so on a default cluster the router runs with
# -cache-buckets EMPTY and is a pass-through proxy:
#     [evict] disabled (buckets=0 ... cacheHighBytes=0)
# Every cache leg then reports 0 MB/s / 0 objects, which reads as broken storage
# rather than "the cache was never enabled" — and the cache-S3 number is really
# just the router proxying to origin, not a cache hit. /iostats settles it: with
# the bucket untracked there are ZERO hits AND zero misses, because the requests
# never enter the cache path.
CACHE_ON="unknown"
IOSTATS=$(curl -s -m 10 "$ROUTER/iostats" 2>/dev/null || true)
if [ -n "$IOSTATS" ]; then
  CACHE_ON=$(printf '%s' "$IOSTATS" | python3 -c '
import json,sys
try: d=json.load(sys.stdin)
except Exception: print("unknown"); raise SystemExit
act = (d.get("cache_hits",0) or 0) + (d.get("cache_misses",0) or 0) + (d.get("bytes_written_to_cache",0) or 0)
print("off" if act == 0 else "on")' 2>/dev/null || echo unknown)
fi

# ================================ summary ========================================
echo ""
echo "==================== read-through cache: $TABLES ($(awk -v b="$BYTES" 'BEGIN{printf "%.1f",b/1073741824}') GiB) ===================="
if [ "$CACHE_ON" = "off" ]; then
  echo "  ⚠ THE READ-THROUGH CACHE IS NOT ENABLED FOR THIS BUCKET."
  echo "    The router reports 0 hits AND 0 misses, i.e. these reads never entered the"
  echo "    cache path — it is started with an empty -cache-buckets and is proxying"
  echo "    straight to the origin. So 'cache-S3' below is NOT a cache hit, and"
  echo "    cache-mp-s3 / cache-NFS find 0 objects because nothing was ever cached."
  echo "    Enable it, then re-run:   ./hookup_cluster_source.sh"
fi
printf '  %-26s %8s MB/s\n' "direct-S3 (origin)"        "$DIRECT"
printf '  %-26s %8s MB/s\n' "cache-S3 (router, warm)"   "$WARM_S3"
printf '  %-26s %8s MB/s\n' "cache-mp-s3 (warm)"        "$WARM_MPS3"
printf '  %-26s %8s MB/s\n' "cache-NFS (Ganesha, warm)" "$WARM_NFS"
# A speedup ratio over a tiny set is NOISE, not a measurement. At SF1 the whole
# origin bucket is ~330 MB and the default table prefix is ONE 17 MB object, so
# both legs are dominated by per-request latency and the ratio swings wildly —
# observed 0.8x on one run and 0.2x on the next, from the same 1-object set, which
# reads as "the read-through cache is 5x SLOWER than S3" when it measures nothing
# of the sort. Print the throughputs (they are still real) but withhold the ratio
# and say why, so nobody quotes it.
MIN_RATIO_BYTES=${MIN_RATIO_BYTES:-1073741824}   # 1 GiB
if [ "${BYTES:-0}" -lt "$MIN_RATIO_BYTES" ] 2>/dev/null; then
  awk -v b="$BYTES" -v n="$NOBJ" -v min="$MIN_RATIO_BYTES" 'BEGIN{
    printf "  speedup vs direct-S3: NOT REPORTED — the set is %.2f GiB in %d object(s), under the %.0f GiB\n", b/1073741824, n, min/1073741824
    printf "    minimum for a meaningful ratio. Both legs are per-request-latency bound at this size.\n"
    printf "    Point CACHE_TABLE at a larger prefix, or set MIN_RATIO_BYTES to override.\n"}'
else
  awk -v d="$DIRECT" -v s="$WARM_S3" -v m="$WARM_MPS3" -v n="$WARM_NFS" 'BEGIN{
    if(d>0){ printf "  speedup vs direct-S3: cache-S3 %.1fx  mp-s3 %s  cache-NFS %.1fx\n", s/d, (m=="n/a"?"n/a":sprintf("%.1fx",m/d)), n/d }}'
fi
# physics gate: c6in.4xlarge networking is 25 Gbps baseline, "up to 50 Gbps"
# burst ≈ 6250 MB/s absolute ceiling — any single-path number above it means
# bytes were not actually transferred (stale view / cache no-op; 2026-07-22 saw
# 8501/9650 MB/s = 68/77 Gbps from exactly that).
for v in "$WARM_S3" "$WARM_MPS3" "$WARM_NFS"; do
  case "$v" in n/a) continue;; esac
  [ "${v:-0}" -gt 6400 ] 2>/dev/null && echo "  ⚠ SUSPECT: $v MB/s exceeds the 50 Gbps NIC burst ceiling — treat as invalid"
done
printf '{"table":"%s","bytes":%s,"objects":%s,"mbps":{"direct_s3":%s,"cache_s3":%s,"cache_mp_s3":"%s","cache_nfs":%s}}\n' \
  "$TABLE" "$BYTES" "$NOBJ" "$DIRECT" "$WARM_S3" "$WARM_MPS3" "$WARM_NFS"
