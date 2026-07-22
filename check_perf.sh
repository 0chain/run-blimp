#!/usr/bin/env bash
# check_perf.sh — grade a run's performance numbers against the 2026-07-17
# reference (today's fresh-4xlarge E2E) with a ±20% band:
#   within ±20% of reference  → PASS
#   >20% away, BAD direction   → LOW/HIGH  → regression (exit non-zero)
#   >20% away, GOOD direction  → LOW/HIGH  → informational (not a fail)
# "Bad direction" = below ref for throughput, above ref for a timing/latency.
#
#   ./check_perf.sh <run.log>        # parse an e2e/test log
# Reference values are today's measured numbers; override any with env
# PERFREF_<KEY>=<n>. Band width via BAND (default 0.20).
set -u
LOG="${1:?usage: check_perf.sh <run.log>}"
BAND="${BAND:-0.20}"
[ -f "$LOG" ] || { echo "no such log: $LOG"; exit 2; }

# key | reference | direction (hi=higher-better throughput, lo=lower-better time)
read -r -d '' REF <<'EOF'
warp_ttfb_p99_ms      5      lo
warp_put_mibs         875    hi
warp_get_mibs         3247   hi
fio_write_mibs        1142   hi
fio_read_mibs         1132   hi
mlperf_au_pct         92.3   hi
mlperf_samples        18090  hi
mlperf_mbs            1978   hi
cache_direct_mbs      1011   hi
cache_router_mbs      3141   hi
cache_mps3_mbs        2326   hi
cache_nfs_mbs         1659   hi
spark_full_s          85.5   lo
blimp_author_s        24.1   lo
initial_materialize_s 12.0   lo
upsert_full_s         38.5   lo
incr_merge_s          5.0    lo
EOF

# ---- extract each metric from the log (best-effort; missing = skipped) ----------
num(){ grep -oE "$1" "$LOG" 2>/dev/null | tail -1 | grep -oE '[0-9]+(\.[0-9]+)?' | tail -1; }
declare -A GOT
GOT[warp_ttfb_p99_ms]=$(grep -oE '99th: [0-9]+ ?ms' "$LOG" | tail -1 | grep -oE '[0-9]+' | tail -1)
GOT[warp_put_mibs]=$(grep -oE 'PUT 96MiB.*Average: [0-9.]+ MiB/s|Average: [0-9.]+ MiB/s, [0-9.]+ obj/s' "$LOG" | grep -oE '[0-9.]+ MiB/s' | head -1 | grep -oE '[0-9.]+')
GOT[warp_get_mibs]=$(awk '/warp GET 96MiB/{f=1} f&&/Average:/{print $3; exit}' "$LOG" | grep -oE '[0-9.]+')
GOT[fio_write_mibs]=$(grep -oE 'WRITE: bw=[0-9.]+MiB/s' "$LOG" | tail -1 | grep -oE '[0-9.]+')
GOT[fio_read_mibs]=$(grep -oE 'READ: bw=[0-9.]+MiB/s' "$LOG" | tail -1 | grep -oE '[0-9.]+')
GOT[mlperf_au_pct]=$(grep -oiE 'AU[: ]+[0-9.]+ ?%' "$LOG" | tail -1 | grep -oE '[0-9.]+')
GOT[mlperf_samples]=$(grep -oiE '[0-9,]+ samples/s' "$LOG" | tail -1 | tr -d ',' | grep -oE '[0-9]+')
GOT[mlperf_mbs]=$(grep -oiE 'storage[_ ]?(io|throughput)[^0-9]*[0-9]+ ?MB/s|mlperf[^0-9]*[0-9]+ ?MB/s' "$LOG" | tail -1 | grep -oE '[0-9]+' | tail -1)
GOT[cache_direct_mbs]=$(grep -oE 'direct-S3[^0-9]*[0-9]+ MB/s' "$LOG" | tail -1 | grep -oE '[0-9]+' | tail -1)
GOT[cache_router_mbs]=$(grep -oE 'cache-S3[^0-9]*[0-9]+ MB/s' "$LOG" | tail -1 | grep -oE '[0-9]+' | tail -1)
GOT[cache_mps3_mbs]=$(grep -oE 'cache-mp-s3[^0-9]*[0-9]+ MB/s' "$LOG" | tail -1 | grep -oE '[0-9]+' | tail -1)
GOT[cache_nfs_mbs]=$(grep -oE 'cache-NFS[^0-9]*[0-9]+ MB/s' "$LOG" | tail -1 | grep -oE '[0-9]+' | tail -1)
GOT[spark_full_s]=$(grep -oE 'Spark full recompute [0-9.]+s|spark: [0-9.]+s' "$LOG" | tail -1 | grep -oE '[0-9.]+')
GOT[blimp_author_s]=$(grep -oE 'author\+shape\+materialize [0-9.]+s' "$LOG" | tail -1 | grep -oE '[0-9.]+')
GOT[initial_materialize_s]=$(grep -oE 'initial materialize [0-9]+ms' "$LOG" | tail -1 | grep -oE '[0-9]+' | awk '{print $1/1000}')
GOT[upsert_full_s]=$(grep -oE 'blimp\(post-upsert\): materialize=[0-9]+ms' "$LOG" | tail -1 | grep -oE '[0-9]+' | awk 'NR==1{print $1/1000}')
GOT[incr_merge_s]=$(grep -oE 'merge_ms=[0-9]+|merge_ms [0-9]+' "$LOG" | tail -1 | grep -oE '[0-9]+' | awk 'NR==1{print $1/1000}')

# ---- grade ----------------------------------------------------------------------
printf '%-24s %10s %10s  %-6s %s\n' METRIC MEASURED REF BAND VERDICT
REG=0; N=0
while read -r key ref dir; do
  [ -z "$key" ] && continue
  v="${GOT[$key]:-}"
  if [ -z "$v" ]; then printf '%-24s %10s %10s  %-6s %s\n' "$key" "-" "$ref" "±${BAND}" "SKIP (not in log)"; continue; fi
  N=$((N+1))
  read -r verdict bad <<<"$(python3 -c "
v=float('$v'); ref=float('$ref'); band=float('$BAND'); d='$dir'
lo=ref*(1-band); hi=ref*(1+band)
if lo<=v<=hi: print('PASS 0')
elif v<lo: print(('LOW %d'%(1 if d=='hi' else 0)))   # below ref: bad for throughput
else:       print(('HIGH %d'%(1 if d=='lo' else 0)))  # above ref: bad for a timing
")"
  tag="$verdict"; [ "$bad" = 1 ] && { tag="$verdict ⚠REGRESSION"; REG=$((REG+1)); }
  printf '%-24s %10s %10s  %-6s %s\n' "$key" "$v" "$ref" "±${BAND}" "$tag"
done <<<"$REF"

echo
echo "graded $N metrics · $REG regression(s) (bad-direction >±${BAND})"
echo "reference = 2026-07-17 fresh-4xlarge E2E; PASS = within ±$(python3 -c "print(int($BAND*100))")% ; LOW/HIGH = deviation; ⚠ = wrong direction"
exit $([ "$REG" -eq 0 ] && echo 0 || echo 1)
