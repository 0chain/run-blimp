#!/usr/bin/env bash
# run_router.sh — read-through storage-cache benchmark.
#
# Reads the SAME dataset (a TPC-DS table prefix, e.g. store_returns ~20 GB) FOUR
# ways and compares throughput:
#   1. direct-S3     : straight from the external S3 origin bucket (baseline)
#   2. cache-S3      : through the cluster read-through Router (:8088) — S3 interface
#   3. cache-NFS     : the same cached objects via the gateway NFS/Ganesha mount
# (a mountpoint-s3 leg was removed: it mounted the gateway :9000 directly, so the
#  router was not in the path and it was not measuring the read-through cache.)
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
#           the cold cache-fill too), GW_AK/GW_SK (gateway minio creds — used to
#           verify the fill actually landed on the blimp node).

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

# --- WHERE the origin bucket lives -------------------------------------------
# The origin is not always AWS S3. When the customer had no data, `blimp --setup`
# stands a MinIO up ON THIS BOX and the origin bucket lives there — so every
# origin call below needs its endpoint and keys. Without this, `aws s3 ls/cp/
# get-object` went to real s3.<REGION>.amazonaws.com for a bucket that only
# exists in the local MinIO: the bench set could not be created, the object
# enumeration came back empty, and the leg exited "no objects under s3://…".
# Blank S3_ENDPOINT = AWS S3, IAM role — the original behaviour, unchanged.
S3_ENDPOINT="${S3_ENDPOINT:-}"
OEP=""
if [ -n "$S3_ENDPOINT" ]; then
  OEP="--endpoint-url $S3_ENDPOINT"
  if [ -n "${S3_KEY:-}" ]; then
    export AWS_ACCESS_KEY_ID="$S3_KEY" AWS_SECRET_ACCESS_KEY="$S3_SECRET"
  fi
fi

# drop the CLIENT page/dentry cache + a fresh NFS mount so every warm path is
# measured against the CLUSTER, never local RAM.
drop_client_cache(){ sync; echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null 2>&1 || true; }
remount_nfs(){ sudo umount -l "$MNT" 2>/dev/null || true; mount_nfs; }

gib_(){ awk -v b="$1" 'BEGIN{printf "%.2f", b/1073741824}'; }
mbps(){ awk -v b="$1" -v s="$2" 'BEGIN{ if(s<=0)s=0.0001; printf "%.0f", b/1e6/s }'; }
now(){ date +%s.%N; }
el(){ awk -v a="$1" -v z="$2" 'BEGIN{d=z-a; if(d<0)d=0; printf "%.2f", d}'; }

mount_nfs(){ mountpoint -q "$MNT" && return 0; sudo mkdir -p "$MNT"
  sudo mount -t nfs4 -o nconnect=16,rsize=1048576,wsize=1048576 "$GW":/ "$MNT"; }

echo "[cache-bench] origin=s3://$BKT/{$TABLES}/  region=$REGION  router=$ROUTER  par=$PAR"

# --- the read set: OUR OWN data, sized above 2x the gateway's RAM ----------------
# The router test needs a set the gateway cannot simply hold in memory, or the
# "warm" read is served from its page cache and the number says nothing about the
# cache. The customer's data does not qualify and cannot be made to: at SF1 the
# whole origin is 328 MB against ~3.7 GiB of gateway RAM, in 3 objects.
#
# So MAKE the data here on the client, upload it to the origin, and read it back
# through the router. 10 GB by default = 2.7x a 4 GiB gateway. It is written in
# parts because a single object is a single stream — one 10 GB GET measures one
# TCP connection, not the cluster. Set BENCH_PARTS=1 for a literal single file.
#
# BENCH_GB=0 skips generation and uses whatever is already in the bucket.
BENCH_PREFIX="${BENCH_PREFIX:-_blimp_cachebench}"
# 10 GiB in 10 PARTS. The two numbers do different jobs — do not collapse them.
#
# TOTAL SIZE defeats the page caches on the read path. A hit is served
# client <- router <- gateway MinIO <- 3 eblobbers <- disk, and the client can
# only drop its OWN page cache; the gateway's and the eblobbers' are unreachable
# without a shell on the Blimp node. So the set has to be too big to sit in them.
# At EC 2/1 a logical N GiB set lands ~N/2 GiB of data per blobber, so against
# 3.8 GB per blobber:
#     10 GiB -> ~5 GiB/blobber  = above RAM, mostly must come off disk
#      6 GiB -> ~3 GiB/blobber  = fits in RAM
#      3 GiB -> ~1.5 GiB/blobber = fits easily; the "hit" is then RAM end-to-end
#                                  and nothing client-side would reveal it.
# Re-derive this if the cluster shape changes: the bar is per-blobber shard share
# > per-blobber RAM, NOT total set > gateway RAM.
#
# PART COUNT gives concurrency. One 10 GB object is ONE TCP stream, so a single
# huge file measures one connection rather than the cluster. Set BENCH_PARTS=1
# only when you deliberately want the single-stream number.
#
# Repeating a pass does NOT substitute for size — re-reading a RAM-resident set
# just measures RAM again. Repeats only quantify variance (worth having: a 3 GiB
# set swung 696 -> 369 MB/s between identical runs).
BENCH_GB="${BENCH_GB:-10}"
BENCH_PARTS="${BENCH_PARTS:-10}"
if [ "${BENCH_GB:-0}" -gt 0 ]; then
  want_bytes=$(( BENCH_GB * 1073741824 ))
  have=$(aws s3 ls "s3://$BKT/$BENCH_PREFIX/" --recursive --region "$REGION" $OEP 2>/dev/null \
         | awk '{s+=$3} END{print s+0}')
  if [ "${have:-0}" -ge "$want_bytes" ]; then
    echo "[cache-bench] reusing existing $(awk -v b="$have" 'BEGIN{printf "%.1f",b/1073741824}') GiB bench set in $BENCH_PREFIX/"
  else
    part_mb=$(( BENCH_GB * 1024 / BENCH_PARTS ))
    echo "[cache-bench] creating ${BENCH_GB} GB on this client ($BENCH_PARTS x ${part_mb} MiB) and uploading to s3://$BKT/$BENCH_PREFIX/ …"
    gen=$(mktemp -d /var/tmp/blimp-bench.XXXXXX)
    i=0
    while [ "$i" -lt "$BENCH_PARTS" ]; do
      i=$((i+1))
      # /dev/urandom, not /dev/zero: zeros are trivially compressible and any
      # layer that compresses would flatter every number downstream.
      dd if=/dev/urandom of="$gen/part-$i.bin" bs=1M count="$part_mb" 2>/dev/null &
      [ $((i % 4)) -eq 0 ] && wait
    done
    wait
    up0=$(now)
    # Do NOT discard the upload's stderr and then report its nominal size as
    # throughput. A failed cp (wrong endpoint / no origin creds) returned in
    # ~2s and printed "uploaded 10 GB … 4178 MB/s" for zero bytes transferred —
    # the same fiction the honest-bytes fix removed from the read legs. Report
    # the rate over what the origin ACTUALLY holds afterwards, and say so when
    # the upload failed.
    if ! aws s3 cp "$gen/" "s3://$BKT/$BENCH_PREFIX/" --recursive --region "$REGION" $OEP --only-show-errors; then
      echo "[cache-bench] !! upload to s3://$BKT/$BENCH_PREFIX/ FAILED (endpoint/creds?) — cannot measure the cache"
      rm -rf "$gen"; exit 1
    fi
    up1=$(now)
    got=$(aws s3 ls "s3://$BKT/$BENCH_PREFIX/" --recursive --region "$REGION" $OEP 2>/dev/null | awk '{s+=$3} END{print s+0}')
    echo "[cache-bench] uploaded $(awk -v b="${got:-0}" 'BEGIN{printf "%.1f",b/1073741824}') GiB to the origin in $(el "$up0" "$up1")s ($(mbps "${got:-0}" "$(el "$up0" "$up1")") MB/s client→origin)"
    rm -rf "$gen"
  fi
  # Read ONLY the bench set: mixing in the customer's 3 small parquet files would
  # dilute the result with per-request-latency-bound reads.
  TABLES="$BENCH_PREFIX"
fi

# --- enumerate the object set (key<TAB>size), optionally capped to MAX_BYTES -------
MAP=""
for T in $TABLES; do
  MAP="$MAP$(aws s3 ls "s3://$BKT/$T/" --recursive --region "$REGION" $OEP 2>/dev/null | awk '$3+0>0{print $4"\t"$3}')
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

# router_get — GET every key through the router and return "<bytes> <errors>".
#
# HONEST BYTES. The router legs used to divide the SET size by wall time, so an
# object the router failed to serve still counted its full size: `curl -s -o
# /dev/null` exits 0 on a 404/403 having transferred nothing. That reported
# cache-MISS→write at 611 MB/s while direct-S3 on the SAME set was 240 MB/s —
# impossible, since the miss path must fetch those bytes FROM S3 and additionally
# write them. Sum %{size_download} instead and surface any non-200.
router_get(){
  echo "$KEYS" | xargs -P"$PAR" -I{} \
    curl -s -m3600 -o /dev/null -w '%{size_download} %{http_code}\n' "$ROUTER/$BKT/{}" \
  | awk '{b+=$1; if ($2!=200) e++} END{printf "%d %d", b+0, e+0}'
}

# ============================ 1) DIRECT S3 (origin) ===============================
# Straight GET from the origin bucket to /dev/null, parallel. VPC S3 endpoint keeps
# it private + in-region; the box IAM role authenticates.
# SAME CLIENT AS THE ROUTER LEGS, AND THE SIGNING IS NOT TIMED.
#
# This used to run `aws s3api get-object` once PER OBJECT — a full Python
# interpreter start each time, measured at 0.76s per object — INSIDE the timed
# window, while the router legs below use curl. So the two legs measured two
# different clients, the origin baseline came out ~25% low, and the cache MISS
# appeared FASTER than reading the origin directly (599 vs 388 MB/s). That is
# impossible: the miss path is this same origin read PLUS a hop through the
# router plus the proxying. Any "the cache beats the origin" reading came from
# the measurement, not the cluster.
#
# Presign every key up front (outside the timer), then fetch with the identical
# curl + concurrency the router legs use. Now the only difference between the
# legs is the path, which is the thing being compared.
PRESIGNED=$(mktemp)
for k in $KEYS; do
  if [ -n "$S3_ENDPOINT" ]; then
    aws s3 presign "s3://$BKT/$k" --endpoint-url "$S3_ENDPOINT" --region "$REGION" --expires-in 7200 2>/dev/null
  else
    aws s3 presign "s3://$BKT/$k" --region "$REGION" --expires-in 7200 2>/dev/null
  fi
done > "$PRESIGNED"
npre=$(awk 'NF' "$PRESIGNED" | wc -l | tr -d ' ')
if [ "${npre:-0}" -lt "$NOBJ" ]; then
  echo "  !! presigned only $npre/$NOBJ origin URLs — direct-S3 baseline would be partial; skipping it"
  DIRECT=0; DIRECT_S=0
else
  t0=$(now)
  xargs -P"$PAR" -I{} curl -s -m3600 -o /dev/null -w '%{size_download} %{http_code}\n' "{}" < "$PRESIGNED" \
    | awk '{b+=$1; if($2!=200)e++} END{printf "%d %d", b+0, e+0}' > "$PRESIGNED.out"
  t1=$(now)
  DIRECT_B=$(cut -d' ' -f1 "$PRESIGNED.out"); DIRECT_ERR=$(cut -d' ' -f2 "$PRESIGNED.out")
  DIRECT_S=$(el "$t0" "$t1"); DIRECT=$(mbps "${DIRECT_B:-0}" "$DIRECT_S")
  echo "  direct-S3 : $DIRECT MB/s (${DIRECT_S}s, $(gib_ "${DIRECT_B:-0}") GiB recv${DIRECT_ERR:+, $DIRECT_ERR non-200})"
fi
rm -f "$PRESIGNED" "$PRESIGNED.out"

# ==================== 2) CACHE MISS → WRITE onto the blimp node ==================
# FULL-object GET through the Router so the whole object is fetched from origin and
# written into the gateway cache bucket (a range GET would only cache a slice, so
# the file views below would read a hole).
#
# This IS the write half of the read-through test: a miss makes the router pull
# from the source (SF1 origin) and WRITE it onto the blimp node, so the rate here
# is the cache-fill/write speed. It was previously timed only under COLD_FILL=1,
# so by default the suite reported the read half and silently dropped the write —
# half the test. Report it always; COLD_FILL=0 suppresses it.
t0=$(now)
FILL_OUT=$(router_get)
t1=$(now); FILL_S=$(el "$t0" "$t1")
FILL_B=${FILL_OUT%% *}; FILL_ERR=${FILL_OUT##* }
[ "${COLD_FILL:-1}" = 1 ] && {
  echo "  cache-MISS→write: $(mbps "$FILL_B" "$FILL_S") MB/s (${FILL_S}s, origin→router→blimp node; includes the origin fetch)"
  [ "${FILL_B:-0}" -lt "$BYTES" ] && echo "    (transferred $(awk -v b="$FILL_B" 'BEGIN{printf "%.2f",b/1073741824}') of $(awk -v b="$BYTES" 'BEGIN{printf "%.2f",b/1073741824}') GiB${FILL_ERR:+, $FILL_ERR non-200 response(s)}) — rate is over bytes ACTUALLY received"
}

# ===================== WAIT FOR THE FILL TO ACTUALLY LAND =======================
# The warm pass used to start the instant the fill pass returned, and therefore
# measured a SECOND ORIGIN FETCH while reporting it as "router->eblobber cache":
#   cache_hits 0 · cache_misses 20 · bytes_served_from_cache 0
#   bytes_served_from_origin 21474836480   (both 10 GiB passes)
#   bytes_written_to_cache    4294967296   (4 of 10 objects)
# Two independent reasons, both need waiting out:
#   1. the Router's cache ingest is ASYNC (ingestWholeObjectOnce) — a 10 GiB fill
#      into the eblobbers is still in flight when the fill GETs have returned;
#   2. the Router memoizes cache membership with a NEGATIVE TTL (30s, router
#      68abcdf), so an object that HAS landed is still assumed absent until that
#      entry expires.
# Poll the cache bucket on the gateway until every object is present (or give up
# loudly), then sleep past the negative TTL. Needs the gateway creds; without
# them, say the warm number is not a verified hit rather than implying it is.
CACHE_VERIFIED=0
cached_count(){ AWS_ACCESS_KEY_ID="$GW_AK" AWS_SECRET_ACCESS_KEY="$GW_SK" \
  aws s3 ls "s3://$BKT/$BENCH_PREFIX/" --recursive --region us-east-1 \
  --endpoint-url "http://$GW:9000" 2>/dev/null | awk '$3+0>0' | wc -l | tr -d ' '; }
if [ -n "${GW_AK:-}" ]; then
  # ONE PASS CANNOT FILL THE CACHE. The Router admits only
  # ZUS_ROUTER_SPOOL_CONCURRENCY (default 4) concurrent spools; every GET beyond
  # that is served straight from origin and NEVER cached — silently, with no log
  # line. So a PAR=16 fill over 10 objects caches exactly 4 of them, and the
  # "warm" pass that follows is mostly origin reads wearing the cache's label
  # (observed: 4/10, then 8/10, then 4/10 on a fresh prefix; the router logged
  # exactly four "Successfully cached" lines each time).
  # Re-drive the set until every object is resident, and say how many passes it
  # took — that number IS the finding, not an implementation detail to hide.
  fill_deadline=$(( $(date +%s) + ${CACHE_FILL_TIMEOUT:-900} ))
  pass=1
  while :; do
    incache=$(cached_count)
    [ "${incache:-0}" -ge "$NOBJ" ] && { CACHE_VERIFIED=1; break; }
    [ "$(date +%s)" -ge "$fill_deadline" ] && break
    echo "  cache fill pass $pass: ${incache:-0}/$NOBJ resident — re-driving the set (router admits ${ZUS_ROUTER_SPOOL_CONCURRENCY:-4} spools/pass)"
    router_get >/dev/null
    sleep 8
    pass=$((pass+1))
  done
  if [ "$CACHE_VERIFIED" = 1 ]; then
    echo "  cache fill complete: $incache/$NOBJ objects on the blimp node after $pass pass(es)"
  else
    echo "  !! cache fill INCOMPLETE: ${incache:-0}/$NOBJ objects after ${CACHE_FILL_TIMEOUT:-900}s / $pass pass(es)"
    echo "     the warm pass below will re-fetch the missing ones from origin"
  fi
  # Outlast the negative membership memo, or every read below misses regardless.
  echo "  settling ${CACHE_NEG_TTL_WAIT:-35}s past the router's negative cache-membership TTL…"
  sleep "${CACHE_NEG_TTL_WAIT:-35}"
else
  echo "  (no gateway creds — cannot verify the fill landed; the warm number below is NOT a confirmed cache hit)"
fi

# ============================ 3) CACHE-S3 (warm) =================================
# Second GET of the same set through the Router — now served from the eblobber cache.
drop_client_cache
t0=$(now)
WARM_OUT=$(router_get)
t1=$(now); WARM_S3_S=$(el "$t0" "$t1")
WARM_B=${WARM_OUT%% *}; WARM_ERR=${WARM_OUT##* }
WARM_S3=$(mbps "$WARM_B" "$WARM_S3_S")
# PROVE the warm pass was served from the cache. The router's own counters are
# the only authority: a number labelled "cache" that the router recorded as a
# MISS is an origin fetch wearing the cache's label.
HITS_AFTER=$(curl -s -m10 "$ROUTER/iostats" 2>/dev/null | python3 -c '
import json,sys
try: d=json.load(sys.stdin)
except Exception: print("? ?"); raise SystemExit
print(d.get("cache_hits",0), d.get("cache_misses",0))' 2>/dev/null || echo "? ?")
echo "  cache-S3  : $WARM_S3 MB/s (${WARM_S3_S}s, router->eblobber cache; router counters hits/misses=${HITS_AFTER// //})"
case "$HITS_AFTER" in
  "0 "*) echo "    ⚠ the router recorded ZERO cache hits — this number is an ORIGIN fetch, not a cache read" ;;
esac
[ "${WARM_B:-0}" -lt "$BYTES" ] && echo "    (transferred $(awk -v b="$WARM_B" 'BEGIN{printf "%.2f",b/1073741824}') of $(awk -v b="$BYTES" 'BEGIN{printf "%.2f",b/1073741824}') GiB${WARM_ERR:+, $WARM_ERR non-200 response(s)}) — rate is over bytes ACTUALLY received"

# NOTE: the mountpoint-s3 leg that used to sit here has been REMOVED. It mounted
# the gateway's S3 endpoint (:9000) directly, so the router was NOT in the path at
# all — it measured the gateway S3 face, not the read-through cache, and reported
# 545 MB/s next to the router's 394 as if the two were comparable paths. Keeping a
# non-router number inside the router benchmark invites exactly that comparison.
# The gateway S3 face is already covered by the warp leg of --storage.

# ============================ 4) CACHE-NFS (warm) ================================
# OFF BY DEFAULT (CACHE_NFS=1 to include). The cache is measured over the two S3
# paths only — the router (:8088) and mountpoint-s3 against the gateway (:9000) —
# because S3 is the fastest read path and the one customers actually use.
#
# The Ganesha view reads the SAME bytes (the cache bucket is a gateway S3 bucket,
# and the gateway exports that one namespace over both S3 and NFS — there is no
# second copy), so it adds a third number for the same stored object while
# introducing a POSIX/FUSE path that is neither the fast path nor the shipped one.
# Keep it available for diagnosing the NFS face specifically; the raw NFS
# throughput of the cluster is already covered by the fio leg of --storage.
WARM_NFS="n/a"
if [ "${CACHE_NFS:-0}" = "1" ]; then
  remount_nfs
  drop_client_cache
  NHITMAP=$(echo "$MAP" | awk -v m="$MNT/$BKT" -F'\t' '{ if (system("test -f \"" m "/" $1 "\"")==0) print }')
  NHITB=$(echo "$NHITMAP" | awk -F'\t' '{s+=$2}END{print s+0}'); NHITN=$(echo "$NHITMAP" | awk 'NF' | wc -l | tr -d ' ')
  [ "$NHITN" -lt "$NOBJ" ] && echo "  (note: only $NHITN/$NOBJ objects present on NFS — measuring those $(awk -v b="$NHITB" 'BEGIN{printf "%.1f",b/1073741824}') GiB)"
  t0=$(now)
  echo "$NHITMAP" | cut -f1 | xargs -P"$PAR" -I{} sh -c 'dd if="$1/$2" of=/dev/null bs=1M 2>/dev/null' _ "$MNT/$BKT" "{}"
  t1=$(now); WARM_NFS=$(mbps "$NHITB" "$(el "$t0" "$t1")"); WARM_NFS_S=$(el "$t0" "$t1")
  echo "  cache-NFS : $WARM_NFS MB/s (${WARM_NFS_S}s, eblobber cache via Ganesha, $NHITN objs)"
fi

# ===================== is the cache even TURNED ON? ==============================
# The router only caches buckets listed in its -cache-buckets flag. `blimp --setup`
# wires the gateway's SOURCE over the admin API but leaves the cache retarget
# done by `blimp --setup` (wire_router_cache); if that step was skipped the
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
  echo "    nothing was ever cached, so no hit can be measured."
  echo "    Enable it by re-running:  blimp --setup"
fi
printf '  %-26s %8s MB/s\n' "direct-S3 (origin)"        "$DIRECT"
printf '  %-26s %8s MB/s\n' "cache-S3 (router, warm)"   "$WARM_S3"
[ "$WARM_NFS" != "n/a" ] && printf '  %-26s %8s MB/s\n' "cache-NFS (Ganesha, warm)" "$WARM_NFS"
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
  awk -v d="$DIRECT" -v s="$WARM_S3" -v n="$WARM_NFS" 'BEGIN{
    if(d>0){ printf "  speedup vs direct-S3: cache-S3 %.1fx%s\n", s/d, (n=="n/a"?"":sprintf("  cache-NFS %.1fx", n/d)) }}'
fi
# physics gate: c6in.4xlarge networking is 25 Gbps baseline, "up to 50 Gbps"
# burst ≈ 6250 MB/s absolute ceiling — any single-path number above it means
# bytes were not actually transferred (stale view / cache no-op; 2026-07-22 saw
# 8501/9650 MB/s = 68/77 Gbps from exactly that).
for v in "$WARM_S3" "$WARM_NFS"; do
  case "$v" in n/a) continue;; esac
  [ "${v:-0}" -gt 6400 ] 2>/dev/null && echo "  ⚠ SUSPECT: $v MB/s exceeds the 50 Gbps NIC burst ceiling — treat as invalid"
done
printf '{"table":"%s","bytes":%s,"objects":%s,"mbps":{"direct_s3":%s,"cache_s3":%s,"cache_nfs":"%s"}}\n' \
  "$TABLE" "$BYTES" "$NOBJ" "$DIRECT" "$WARM_S3" "$WARM_NFS"

# NO write->read LEG HERE. It wrote 64 objects over NFS and read them back with
# warp and mountpoint-s3 against the gateway's S3 endpoint (:9000) — the ROUTER
# is not in either path, so it measured the gateway's read face, not the
# read-through cache, while sitting inside the router benchmark and inviting
# exactly that comparison. Its own note already said so: "this is a READ-PATH
# measurement, not a read-through-cache one". The gateway's write and read faces
# are covered by the fio and warp legs of --storage.
