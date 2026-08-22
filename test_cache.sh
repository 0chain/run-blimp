#!/usr/bin/env bash
# test_cache.sh — Blimp STORAGE/CACHE cluster test suite (setup-aware, one command).
#
# Runs, in order, from a client box that is IN THE CLUSTER'S VPC (so every path uses
# the gateway PRIVATE IP and external S3 goes through the VPC S3 gateway endpoint —
# no egress):
#   1. warp        — S3 PUT/GET + 1KiB TTFB (conc 16 for 2/1)
#   2. mlperf      — resnet50 (dlio) via mountpoint-s3 (accel-4/rt-16/pf-32)
#
# PREREQS (one-time): run ./setup_tests.sh to provision this box in the cluster
# VPC with warp/dlio/mount-s3/awscli and an S3 VPC gateway endpoint.
#
# Endpoints are taken from env if set, else prompted. Save them in an env file and
# `source` it to re-run non-interactively:
#   GW=10.10.150.76 GW_AK=... GW_SK=... \
#   REGION=ap-south-1 EC=2/1 ./run_all.sh
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"

ask(){ local cur="${!1:-}"; if [ -n "$cur" ]; then printf '%s' "$cur"; return; fi
  printf '%s' "$2 " >&2; read -r v; printf '%s' "$v"; }

echo "==================== Blimp cluster storage test suite ===================="
echo "Enter the cluster endpoints (all reachable on the VPC PRIVATE network)."
GW=$(ask GW            "Gateway PRIVATE IP (serves NFS :2049, S3 :9000):")
GW_AK=$(ask GW_AK      "Gateway S3 access key:")
GW_SK=$(ask GW_SK      "Gateway S3 secret key:")
EC="${EC:-2/1}"
REGION="${REGION:-ap-south-1}"
export GW GW_AK GW_SK EC REGION

# AUTO-REAP the benchmark scratch buckets on exit (completion, error, or Ctrl-C) —
# same policy as the cluster's run_bench.sh, so a run never leaves warp/mlperf data
# filling the allocation disk (the small on-prem allocations fill fast: a 66 GiB
# mlperf dataset alone pins an 8x12GB cluster at 90%). These are PURE SCRATCH
# buckets (warp PUT/GET set, 1 KiB TTFB probe, the mlperf dataset) — regenerated
# next run. Set BENCH_KEEP=1 to keep them (e.g. iterate mlperf accel/rt/pf without
# the ~17 min regenerate). Only ever removes these known bench buckets — never data.
reap_bench_buckets(){
  [ -n "${BENCH_KEEP:-}" ] && { echo "[reap] BENCH_KEEP set — leaving bench scratch buckets"; return 0; }
  command -v aws >/dev/null 2>&1 || return 0
  echo "[reap] removing bench scratch buckets (warpbench/warpprobe/ttfb1k/mlperf-bench) to free the allocation disk"
  for b in warpbench warpprobe ttfb1k mlperf-bench; do
    AWS_ACCESS_KEY_ID="$GW_AK" AWS_SECRET_ACCESS_KEY="$GW_SK" AWS_REGION="${REGION:-us-east-1}" \
      aws s3 rb "s3://$b" --force --endpoint-url "http://$GW:9000" >/dev/null 2>&1 || true
  done
}
trap reap_bench_buckets EXIT

echo ""
echo "gateway=$GW  EC=$EC ($REGION)"
echo "----------------------------------------------------------------------"
# capture our own output so the final summary can be extracted from it
CAP="${CAP:-/tmp/test_cache_out.log}"; : > "$CAP"
exec > >(tee -a "$CAP") 2>&1
run(){ echo; echo ">>> $1"; shift; "$@"; }

# --- register each leg as an ORDINARY internal run (identical type + labels to the
# gateway's own run_bench warp/mlperf rows). cdc auto-fills the tier columns by
# running run_bench's __tiers__ sampler between the running and done imports. Token
# = zus-<CLUSTER_ID>; best-effort (skips silently if wiring/py absent).
BL_TOK="${CLUSTER_TOKEN:-zus-${CLUSTER_ID:-}}"
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
# parse a captured leg log into internal-format [label,value] rows (identical labels
# to run_bench). throughput normalised to decimal MB/s (GB/s >=1000).
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

# Leg selector. --storage was all-or-nothing, so re-measuring ONE leg re-paid the
# others: every mlperf attempt first re-ran ~12 min of warp and ~7 min of fio for
# numbers already in hand. run_cluster.sh has always accepted the legs
# individually; this just exposes that.
#   STORAGE_LEGS=mlperf         only mlperf
#   STORAGE_LEGS=warp,mlperf    several
#   (unset / all)               everything, as before
LEGS="${STORAGE_LEGS:-all}"
want(){ case "$LEGS" in all|"") return 0;; esac; case ",$LEGS," in *",$1,"*) return 0;; esac; return 1; }
[ "$LEGS" != "all" ] && echo "[legs] running only: $LEGS"

# CLEANUP is AUTOMATIC (reap_bench_buckets on EXIT, defined above) — same as the
# cluster's run_bench.sh, so a run never leaves warp/mlperf data filling the small
# on-prem allocation. To iterate the mlperf train leg (different accel/rt/pf) without
# paying the ~17 min dataset regenerate each time, run with BENCH_KEEP=1 to keep the
# scratch buckets; then remove them deliberately when done:
#   BENCH_KEEP=1 blimp --storage        # keep warp/mlperf data across runs
#   AWS_ACCESS_KEY_ID=$GW_AK AWS_SECRET_ACCESS_KEY=$GW_SK AWS_REGION=us-east-1 \
#     aws s3 rb s3://mlperf-bench --force --endpoint-url http://$GW:9000
# Watch the allocation with:
#   curl -s "http://$GW:9000/admin/alloc/usage?token=zus-<cluster-id>"

# 1) warp — registered as ONE internal "warp" run (PUT + GET + TTFB), tiers by cdc
if want warp; then
  WB="warp_$(date +%s)"; WL=$(mktemp); bl_post "$WB" running warp '[]'; sleep 6
  run "1/2 warp TTFB (1KiB, conc=1)"   env GW="$GW" NFS="$GW" EC="$EC" AK="$GW_AK" SK="$GW_SK" "$HERE/run_cluster.sh" ttfb 2>&1 | tee -a "$WL"
  run "   warp PUT/GET (96MiB, conc=16)" env GW="$GW" NFS="$GW" EC="$EC" AK="$GW_AK" SK="$GW_SK" "$HERE/run_cluster.sh" warp 2>&1 | tee -a "$WL"
  bl_post "$WB" done warp "$(bl_parse warp "$WL")" "$WL"; rm -f "$WL"
fi

# 3) mlperf resnet50 via mountpoint-s3, accel-4 / rt-16 / pf-32 (generate once, keep)
# Params come from detect_ec (EC 2/1 -> accel 3 / rt 12 / pf 24), the SAME table
# the cluster's own benchmark uses. They used to be overridden to 4/20/40 — more
# ranks and threads than the cluster runs for the same EC — and on a 4 GB client
# the extra torch rank starved the mount-s3 daemon, which sits OUTSIDE the dlio
# memory cgroup: the FUSE mount died 14 files into a 476-file read and every rank
# then failed with "Transport endpoint is not connected". Override with
# MLPERF_ACCELS / MLPERF_READ_THREADS / MLPERF_PREFETCH on a bigger box.
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
echo "==================== suite complete — compare to EXPECTED_TEST_RESULTS.md ===================="
