#!/usr/bin/env bash
# test_cache.sh — Blimp STORAGE suite, run by `blimp --storage`:
#   1. warp        — S3 PUT/GET + 1KiB TTFB
#   2. mlperf      — resnet50 (dlio) via mountpoint-s3
#
# PREREQS: `blimp --setup` installs warp/dlio/mount-s3/awscli.
#
# Endpoints are taken from env if set, else prompted:
#   GW=<gateway-ip> GW_AK=... GW_SK=... CLUSTER_TOKEN=... EC=2/1 ./test_cache.sh
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"

ask(){ local cur="${!1:-}"; if [ -n "$cur" ]; then printf '%s' "$cur"; return; fi
  # No controlling terminal => return empty (the ${VAR:?} guards below then abort
  # cleanly); a real terminal => prompt on it.
  { : < /dev/tty; } 2>/dev/null || { printf '' ; return; }
  printf '%s' "$2 " >&2; read -r v </dev/tty; printf '%s' "$v"; }

echo "==================== Blimp cluster storage test suite ===================="
echo "Enter the cluster endpoints (all reachable on the VPC PRIVATE network)."
GW=$(ask GW            "Gateway PRIVATE IP (serves NFS :2049, S3 :9000):")
GW_AK=$(ask GW_AK      "Gateway S3 access key:")
GW_SK=$(ask GW_SK      "Gateway S3 secret key:")
# Fail fast with the missing name instead of running warp/mlperf against blank
# wiring (which "measures nothing"). ask() returns empty on a no-tty box rather
# than blocking; this turns that into a clear abort. blimp --storage pre-sets all
# three from ~/.blimp_env, so this only fires when the suite is mis-invoked.
: "${GW:?empty — set GW (blimp --storage passes it from ~/.blimp_env)}"
: "${GW_AK:?empty — set GW_AK (blimp --storage passes it from ~/.blimp_env)}"
: "${GW_SK:?empty — set GW_SK (blimp --storage passes it from ~/.blimp_env)}"
EC="${EC:-2/1}"
REGION="${REGION:-ap-south-1}"
export GW GW_AK GW_SK EC REGION

# AUTO-REAP the benchmark scratch buckets on exit (completion, error, or Ctrl-C),
# so a run never leaves warp/mlperf data filling the allocation disk. These are PURE SCRATCH
# buckets (warp PUT/GET set, 1 KiB TTFB probe, the mlperf dataset) — regenerated
# next run. Set BENCH_KEEP=1 to keep them (e.g. iterate mlperf accel/rt/pf without
# the ~17 min regenerate). Only ever removes these known bench buckets — never data.
reap_bench_buckets(){
  [ -n "${BENCH_KEEP:-}" ] && { echo "[reap] BENCH_KEEP set — leaving bench scratch buckets"; return 0; }
  command -v aws >/dev/null 2>&1 || return 0
  # With no args (the EXIT trap) reap EVERYTHING; with args reap just those buckets
  # (used to free warp scratch BEFORE the mlperf leg — see below).
  local buckets="${*:-warpbench warpprobe ttfb1k mlperf-bench}"
  echo "[reap] removing bench scratch buckets ($buckets) to free the allocation disk"
  for b in $buckets; do
    AWS_ACCESS_KEY_ID="$GW_AK" AWS_SECRET_ACCESS_KEY="$GW_SK" AWS_REGION="${REGION:-us-east-1}" \
      aws s3 rb "s3://$b" --force --endpoint-url "http://$GW:9000" >/dev/null 2>&1 || true
  done
}
trap reap_bench_buckets EXIT

# wait_disk_reclaim — after a logical reap (aws s3 rb) the eblobber deletes the
# objects but frees the PHYSICAL shard files on its own GC cadence; on a small
# single-box 2-blobber EC 2/1 node that lag can be tens of seconds. mlperf's
# generate that starts on unreclaimed disk trips the blobber's >90% write
# threshold. There is no peer to redirect to on a single node, so WAIT for the physical disk
# to actually free before proceeding. Polls /admin/alloc/usage (disk_* fields);
# best-effort — proceeds with a warning on timeout (the disk_guard still gates).
wait_disk_reclaim(){
  command -v curl >/dev/null 2>&1 || return 0
  [ -n "${CLUSTER_TOKEN:-}" ] || return 0
  local need_gb="${WAIT_RECLAIM_FREE_GB:-55}" timeout="${WAIT_RECLAIM_TIMEOUT:-300}"
  local start now u dt du free_gb waited=0 last_du="" stable=0 du0=""
  start=$(date +%s)
  echo "[reclaim] waiting for physical disk to free >= ${need_gb} GiB after reap (timeout ${timeout}s)"
  while :; do
    u=$(curl -s -m 8 "http://$GW:9000/admin/alloc/usage" -H "Authorization: Bearer $CLUSTER_TOKEN" 2>/dev/null)
    dt=$(printf '%s' "$u" | grep -oE '"disk_capacity_bytes":[0-9]+' | grep -oE '[0-9]+')
    du=$(printf '%s' "$u" | grep -oE '"disk_used_bytes":[0-9]+' | grep -oE '[0-9]+')
    now=$(date +%s); waited=$(( now - start ))
    if [ -n "$dt" ] && [ -n "$du" ] && [ "$dt" -gt 0 ]; then
      # disk_used_bytes is Bfree-based (counts the ~5% root-reserved blocks as
      # free), so (dt-du) OVERSTATES what an unprivileged writer can use by the
      # reserve. The blobber writes as a normal user, so subtract 5% of capacity
      # to get the USABLE free (matches `df` avail) — else the target fires early
      # and mlperf's tail still hits the threshold.
      free_gb=$(( (dt - du - dt / 20) / 1073741824 ))
      [ "$free_gb" -lt 0 ] && free_gb=0
      # Enough usable headroom for the mlperf set -> go.
      if [ "$free_gb" -ge "$need_gb" ]; then
        echo "[reclaim] free=${free_gb} GiB >= ${need_gb} GiB after ${waited}s — proceeding"; return 0
      fi
      # Reclaim finished below target (disk_used stopped dropping for 3 polls):
      # nothing more will free; go with whatever we have rather than hang. Only
      # honour "stable" AFTER disk_used has actually dropped from the first
      # reading — otherwise a slow-to-START blobber GC (du still at the post-reap
      # peak) reads as "stable" and we proceed on unreclaimed disk (the bug).
      [ -z "$du0" ] && du0="$du"
      if [ "$du" = "$last_du" ]; then stable=$(( stable + 1 )); else stable=0; fi
      last_du="$du"
      if [ "$stable" -ge 3 ] && [ "$du" -lt "$du0" ]; then
        echo "[reclaim] disk_used stable at $(( du / 1073741824 )) GiB (free=${free_gb} GiB) after ${waited}s — reclaim done, proceeding"; return 0
      fi
      [ $(( waited % 20 )) -lt 5 ] && echo "  [reclaim] free=${free_gb} GiB (want ${need_gb}) … ${waited}s"
    fi
    if [ "$waited" -ge "$timeout" ]; then
      echo "!! [reclaim] timeout ${timeout}s — physical disk did not free to ${need_gb} GiB (free=${free_gb:-?} GiB); proceeding anyway (disk_guard still applies)"; return 0
    fi
    sleep 5
  done
}

echo ""
echo "gateway=$GW  EC=$EC ($REGION)"
echo "----------------------------------------------------------------------"
# capture our own output so the final summary can be extracted from it
CAP="${CAP:-$(mktemp -t test_cache_out.XXXXXX)}"; : > "$CAP"
exec > >(tee -a "$CAP") 2>&1
run(){ echo; echo ">>> $1"; shift; "$@"; }

# --- register each leg as a run on the node panel's Benchmarks tab (same type +
# labels as a panel-started run). Fleet-token bearer; best-effort.
BL_TOK="${CLUSTER_TOKEN:?fleet token required (CLUSTER_TOKEN)}"
bl_post(){ # <bid> <status> <type> <metrics_json> [logfile]
  [ -n "${CLUSTER_ID:-}" ] && [ -n "${GW:-}" ] && command -v python3 >/dev/null 2>&1 || return 0
  BL_ID="$1" BL_ST="$2" BL_TY="$3" BL_M="$4" BL_LOG="${5:-}" BL_GW="$GW" BL_CID="$CLUSTER_ID" BL_TOK="$BL_TOK" python3 - <<'PY' 2>/dev/null || true
import os,json,re,urllib.request
m=json.loads(os.environ["BL_M"] or "[]")
lg=""
try:
    if os.environ.get("BL_LOG"): lg=re.sub(r"\x1b\[[0-9;]*m","",open(os.environ["BL_LOG"],errors="replace").read())[-200000:]
except Exception: pass
d=json.dumps({"id":os.environ["BL_ID"],"type":os.environ["BL_TY"],"status":os.environ["BL_ST"],"log":lg,
  "summary":{"type":os.environ["BL_TY"],"metrics":m,"config":"run-blimp client-side run","status":os.environ["BL_ST"]}}).encode()
r=urllib.request.Request("http://%s:9401/bench/import"%os.environ["BL_GW"],data=d,
  headers={"Authorization":"Bearer "+os.environ["BL_TOK"],"Content-Type":"application/json"},method="POST")
try: urllib.request.urlopen(r,timeout=15)
except Exception: pass
PY
}
# parse a captured leg log into [label,value] rows (same labels as the panel).
# throughput normalised to decimal MB/s (GB/s >=1000).
bl_parse(){ # <type> <logfile>  -> metrics json on stdout
  BL_TYPE="$1" BL_LOG="$2" python3 - <<'PY' 2>/dev/null || echo '[]'
import os,re,json
t=os.environ["BL_TYPE"]
try: c=re.sub(r"\x1b\[[0-9;]*m","",open(os.environ["BL_LOG"],errors="replace").read())
except Exception: c=""
def to_mb(v,u):
    v=float(v); u=u.lower()
    return v*(1073.741824 if u.startswith("gib") else 1.048576 if u.startswith("mib") else 1000.0 if u.startswith("gb") else 1.0 if u.startswith("mb") else 0.001048576 if u.startswith("kib") else 0.001)
def fmt(mb): return "%.2f GB/s"%(mb/1000.0) if mb>=1000 else "%d MB/s"%round(mb)
def thr(s,paren=False):
    if paren:
        m=re.search(r"\(([0-9.]+)\s*(GB/s|MB/s|KB/s)\)",s)
        if m: return fmt(to_mb(m.group(1),m.group(2)))
    m=re.search(r"([0-9.]+)\s*(GiB/s|MiB/s|KiB/s|GB/s|MB/s|KB/s)",s)
    return fmt(to_mb(m.group(1),m.group(2))) if m else None
def g(rx,s,gr=1):
    m=re.search(rx,s); return m.group(gr) if m else None
rows=[]
if t=="warp":
    put=get=ttfb=None; ctx=None
    for ln in c.splitlines():
        s=ln.strip()
        if "== warp PUT" in s: ctx="P"
        elif "== warp GET" in s: ctx="G"
        elif s.startswith("* Average:"):
            v=thr(s)
            if v and ctx=="P": put=v
            elif v and ctx=="G": get=v
        elif "TTFB:" in s:
            med=g(r"Median:\s*([0-9a-z]+)",s); p99=g(r"99th:\s*([0-9a-z]+)",s)
            if med or p99: ttfb="%s/%s"%(med or "n/a",p99 or "n/a")
    rows=[["spec","warp S3 PUT/GET 96MiB conc16 + 1KiB TTFB (client->gateway)"],["PUT",put or "n/a"],["GET",get or "n/a"],["GET TTFB p50/p99",ttfb or "n/a"]]
elif t=="mlperf resnet50":
    au=io=acc=rt=pf=None
    for ln in c.splitlines():
        if "mlperf read" in ln:
            au=g(r"AU\s*([0-9.]+)",ln); io=g(r"([0-9.]+)\s*MB/s",ln)
            acc=g(r"accel=([0-9]+)",ln); rt=g(r"rt=([0-9]+)",ln); pf=g(r"pf=([0-9]+)",ln)
    spec="resnet50 dlio via mount-s3 · accel %s · read_threads %s · prefetch %s (client-side train)"%(acc or "?",rt or "?",pf or "?")
    rows=[["spec",spec],["resnet50 accel-%s"%(acc or "?"),"AU %s%%, %s MB/s"%(au or "n/a",io or "n/a")]]
print(json.dumps(rows))
PY
}

# Leg selector (run_cluster.sh accepts the legs individually).
#   STORAGE_LEGS=mlperf         only mlperf
#   STORAGE_LEGS=warp,mlperf    several
#   (unset / all)               everything, as before
LEGS="${STORAGE_LEGS:-all}"
want(){ case "$LEGS" in all|"") return 0;; esac; case ",$LEGS," in *",$1,"*) return 0;; esac; return 1; }
[ "$LEGS" != "all" ] && echo "[legs] running only: $LEGS"

# CLEANUP is AUTOMATIC (reap_bench_buckets on EXIT, defined above). To iterate the
# mlperf train leg without regenerating the dataset each time, run with
# BENCH_KEEP=1 to keep the scratch buckets; then remove them deliberately when done:
#   BENCH_KEEP=1 blimp --storage        # keep warp/mlperf data across runs
#   AWS_ACCESS_KEY_ID=$GW_AK AWS_SECRET_ACCESS_KEY=$GW_SK AWS_REGION=us-east-1 \
#     aws s3 rb s3://mlperf-bench --force --endpoint-url http://$GW:9000
# Watch the allocation with:
#   curl -s "http://$GW:9000/admin/alloc/usage" -H "Authorization: Bearer $CLUSTER_TOKEN"

# 1) warp — registered as ONE internal "warp" run (PUT + GET + TTFB), tiers by cdc
if want warp; then
  WB="warp_$(date +%s)"; WL=$(mktemp); bl_post "$WB" running warp '[]'; sleep 6
  run "1/2 warp TTFB (1KiB, conc=1)"   env GW="$GW" NFS="$GW" EC="$EC" AK="$GW_AK" SK="$GW_SK" "$HERE/run_cluster.sh" ttfb 2>&1 | tee -a "$WL"
  run "   warp PUT/GET (96MiB, conc=16)" env GW="$GW" NFS="$GW" EC="$EC" AK="$GW_AK" SK="$GW_SK" "$HERE/run_cluster.sh" warp 2>&1 | tee -a "$WL"
  bl_post "$WB" done warp "$(bl_parse warp "$WL")" "$WL"; rm -f "$WL"
  # Free the warp PUT/GET scratch NOW, before mlperf generates its ~66 GiB dataset.
  # Both land on the same small on-prem allocation disk; leaving warp's set resident
  # until the EXIT trap means the mlperf generate runs on top of it and trips the
  # gateway's >90% write-threshold. Only when mlperf actually follows; BENCH_KEEP
  # still keeps everything (reap no-ops).
  if want mlperf; then reap_bench_buckets warpbench warpprobe ttfb1k; wait_disk_reclaim; fi
fi

# 2) mlperf resnet50 via mountpoint-s3 (generate once, keep). Params come from
# run_cluster.sh's detect_ec, the same table the panel's benchmark uses. Override
# with MLPERF_ACCELS / MLPERF_READ_THREADS / MLPERF_PREFETCH on a bigger box.
if want mlperf; then
  MB="mlperf_$(date +%s)"; ML=$(mktemp); bl_post "$MB" running "mlperf resnet50" '[]'; sleep 6
  run "2/2 mlperf resnet50 (mp-s3, EC-derived accel/rt/pf)" \
    env GW="$GW" NFS="$GW" EC="$EC" AK="$GW_AK" SK="$GW_SK" \
    MLPERF_IFACE=mps3 MLPERF_KEEP=1 CLUSTER_ID="$CLUSTER_ID" REGION="$REGION" \
    MLPERF_NUM_FILES="${MLPERF_NUM_FILES:-}" MLPERF_NUM_EVAL="${MLPERF_NUM_EVAL:-}" \
    MLPERF_ACCELS="${MLPERF_ACCELS:-}" MLPERF_REGEN="${MLPERF_REGEN:-}" \
    "$HERE/run_cluster.sh" mlperf 2>&1 | tee -a "$ML"
  # A train that dies (corrupt tfrecord, mount-s3 drop, OOM) prints no "mlperf read"
  # summary line — post it as FAILED, not a misleading "done" with accel-?/AU n/a.
  if grep -q "mlperf read" "$ML"; then MST=done; else MST=failed; fi
  bl_post "$MB" "$MST" "mlperf resnet50" "$(bl_parse 'mlperf resnet50' "$ML")" "$ML"; rm -f "$ML"
fi

echo ""
echo "============================ STORAGE/CACHE SUMMARY ============================"
# Print a line ONLY when that leg actually produced numbers.
awk '
  / TTFB: Avg/                { ttfb=$0 }
  /== warp PUT/               { sect="put" } /== warp GET/ { sect="get" }
  / \* Average:.*MiB\/s/      { if (sect=="put" && !put) put=$3; else if (sect=="get" && !get) get=$3 }
  /mlperf write-NFS:/         { mw=$3 }
  /Accelerator Utilization/          { au=$0; sub(/.*: */,"",au); sub(/ *\(.*/,"",au) }
  /Training Throughput.*samples/     { sm=$0; sub(/.*: */,"",sm); sub(/ *\(.*/,"",sm) }
  /Training I\/O Throughput/         { io=$0; sub(/.*: */,"",io); sub(/ *\(.*/,"",io) }
  END{
    any=0
    if (put || get) { printf "  warp    S3 PUT %s MiB/s · GET %s MiB/s\n", put, get; any=1
                      if (ttfb) { sub(/^ *\* */,"",ttfb); printf "  %s\n", ttfb } }
    if (mw || io || au) { printf "  mlperf  write-NFS %s MB/s", (mw?mw:"n/a")
                      if (au || io) printf " · read AU %s%% · %s samples/s · %s MB/s", (au?au:"n/a"), (sm?sm:"n/a"), (io?io:"n/a")
                      else          printf " · read (train produced no metrics)"
                      printf "\n"; any=1 }
    if (!any) print "  (no leg produced numbers — see the log above for the failure)"
  }' "$CAP"
echo "==============================================================================="
echo "==================== suite complete (full log: $CAP) ===================="
