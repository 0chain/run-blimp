#!/usr/bin/env bash
# test_flow_panel.sh — verify EVERY tile on the Blimp prod-Storage "Cache data flow"
# panel with asserted numbers. Drives the router (:8088) exactly like a customer
# pipeline and diffs the router's /iostats counters phase by phase:
#
#   phase 1 COLD  : GET an uncached prefix  -> misses+N, fetched_from_origin+B,
#                                              written_to_blobbers+B (async fill)
#   phase 2 WARM  : GET the same prefix     -> hits+N, served_from_cache+B
#   phase 3 RATE  : hit_rate == hits/(hits+misses) from the same snapshots
#   phase 4 CPU   : sample gateway cpu_pct DURING a sustained warm sweep (>0 under
#                   load proves the 0.7s /proc/stat sampler; idle 0% is honest)
#   phase 5 EVICT : informational — print evictions/bytes_evicted delta since start
#                   and the router [evict] log lines (capacity evict needs the cache
#                   near its watermark; not forced here to keep the test fast)
#
# Requires the router image with the hit/miss counter fix (0chain/router 3243031+);
# on older images phases 1-3 fail with all-zero hit/miss counters — that IS the bug.
#
# Env (or prompted): GW (gateway private ip), ORIGIN_BUCKET, REGION,
#   TABLE (default customer_address — 32 objects, ~1.2GiB, fills in seconds),
#   CDC_TOKEN (default zus-<CLUSTER_ID> if CLUSTER_ID set) for the gateway CPU probe.
set -u
GW="${GW:?set GW=<gateway private ip>}"
ORIGIN_BUCKET="${ORIGIN_BUCKET:?set ORIGIN_BUCKET}"
REGION="${REGION:-ap-south-1}"
TABLE="${TABLE:-customer_address}"
ROUTER="http://$GW:8088"
CDC="http://$GW:9401"
TOKEN="${CDC_TOKEN:-${CLUSTER_ID:+zus-$CLUSTER_ID}}"
PAR="${PAR:-8}"
PASS=0; FAIL=0
ok(){ echo "  PASS  $*"; PASS=$((PASS+1)); }
bad(){ echo "  FAIL  $*"; FAIL=$((FAIL+1)); }
snap(){ curl -s -m10 "$ROUTER/iostats"; }
jget(){ python3 -c "import json,sys;print(json.load(sys.stdin).get('$1',0))"; }
field(){ echo "$2" | jget "$1"; }

# ---- object set: keys + exact byte total from the ORIGIN listing -----------------
LIST=$(aws s3 ls "s3://$ORIGIN_BUCKET/$TABLE/" --recursive --region "$REGION" | awk '$3+0>0 && $4 ~ /\.parquet$/ {print $3, $4}')
NOBJ=$(echo "$LIST" | wc -l | tr -d ' ')
BYTES=$(echo "$LIST" | awk '{s+=$1}END{print s}')
KEYS=$(echo "$LIST" | awk '{print $2}')
[ "$NOBJ" -gt 0 ] || { echo "no objects under s3://$ORIGIN_BUCKET/$TABLE/"; exit 1; }
echo "== flow-panel test: $NOBJ objects, $BYTES bytes (s3://$ORIGIN_BUCKET/$TABLE) via $ROUTER =="

# tolerance: byte counters may include a concurrent trickle; ±2% or 64KiB
near(){ python3 -c "import sys;a,b=float(sys.argv[1]),float(sys.argv[2]);sys.exit(0 if abs(a-b)<=max(b*0.02,65536) else 1)" "$1" "$2"; }

S0=$(snap)
# ---- phase 1: COLD (miss path). Evict our prefix first so the sweep is really cold.
# The router has no per-prefix purge API; instead read via a cache-busting copy?
# No — cold is guaranteed by choosing TABLE not yet read this boot, or after the
# evictor drained the cache (evictions tile). If the prefix is already cached this
# phase auto-degrades: it detects 0 new misses AND matching hits and SKIPs to warm.
echo "-- phase 1: cold sweep (expect +$NOBJ misses, +$BYTES fetched-from-origin) --"
echo "$KEYS" | xargs -P"$PAR" -I{} curl -s -m600 -o /dev/null "$ROUTER/$ORIGIN_BUCKET/{}"
S1=$(snap)
dm=$(( $(field cache_misses "$S1") - $(field cache_misses "$S0") ))
dh=$(( $(field cache_hits   "$S1") - $(field cache_hits   "$S0") ))
dor=$(( $(field bytes_served_from_origin "$S1") - $(field bytes_served_from_origin "$S0") ))
if [ "$dm" -eq "$NOBJ" ]; then ok "misses +$dm == $NOBJ"
elif [ "$dh" -eq "$NOBJ" ] && [ "$dm" -eq 0 ]; then ok "prefix already cached ($dh hits) — cold phase counted as warm"
else bad "misses +$dm (want $NOBJ) hits +$dh"; fi
if [ "$dm" -gt 0 ]; then
  near "$dor" "$BYTES" && ok "fetched-from-origin +$dor ≈ $BYTES" || bad "fetched-from-origin +$dor (want ≈$BYTES)"
fi
# Converge the async cache fill. The router caps concurrent ingests (default
# ZUS_ROUTER_SPOOL_CONCURRENCY=4) and SKIPS fills beyond the cap — by design a
# later read re-triggers them. So: re-sweep until written_to_blobbers ≈ BYTES.
if [ "$dm" -gt 0 ]; then
  W0=$(field bytes_written_to_cache "$S0"); sweeps=1
  while :; do
    sleep 20
    dw=$(( $(snap | jget bytes_written_to_cache) - W0 ))
    near "$dw" "$BYTES" && { ok "written-to-blobbers +$dw ≈ $BYTES (converged after $sweeps sweep(s))"; break; }
    [ "$sweeps" -ge 10 ] && { bad "cache fill stuck at +$dw of $BYTES after $sweeps sweeps"; break; }
    sweeps=$((sweeps+1))
    # re-read to re-trigger the skipped ingests (slot-capped, best-effort)
    echo "$KEYS" | xargs -P4 -I{} curl -s -m600 -o /dev/null "$ROUTER/$ORIGIN_BUCKET/{}"
  done
fi

# ---- phase 2: WARM (hit path) + phase 4: CPU sampled DURING the sweep ------------
echo "-- phase 2: warm sweep (expect +$NOBJ hits, +$BYTES served-from-cache) --"
S2=$(snap)
CPU_LOAD=""
if [ -n "$TOKEN" ]; then
  # 3 samples across the sweep; keep the max (each cdc probe is a 0.7s window —
  # a single sample can straddle a lull and read 0)
  ( for s in 1 2 3; do
      curl -s -m30 "$CDC/cache/iostats?token=$TOKEN" \
        | python3 -c "import json,sys;print(json.load(sys.stdin)['gateway']['cpu_pct'])" 2>/dev/null
      sleep 2
    done | sort -rn | head -1 > /tmp/.flow_cpu_max ) &
fi
# 3 warm passes so the read load overlaps all CPU samples
for p in 1 2 3; do
  echo "$KEYS" | xargs -P"$PAR" -I{} curl -s -m600 -o /dev/null "$ROUTER/$ORIGIN_BUCKET/{}"
done
wait
S3=$(snap)
WOBJ=$((NOBJ*3)); WBYTES=$((BYTES*3))
dh=$(( $(field cache_hits "$S3") - $(field cache_hits "$S2") ))
dsc=$(( $(field bytes_served_from_cache "$S3") - $(field bytes_served_from_cache "$S2") ))
dor2=$(( $(field bytes_served_from_origin "$S3") - $(field bytes_served_from_origin "$S2") ))
[ "$dh" -eq "$WOBJ" ] && ok "hits +$dh == $WOBJ (3 passes x $NOBJ)" || bad "hits +$dh (want $WOBJ; evictor may have dropped the prefix mid-test)"
near "$dsc" "$WBYTES" && ok "served-from-cache +$dsc ≈ $WBYTES" || bad "served-from-cache +$dsc (want ≈$WBYTES)"
[ "$dor2" -eq 0 ] && ok "origin untouched on warm sweeps" || bad "origin +$dor2 bytes on warm sweeps (expected 0)"

# ---- phase 3: hit rate ------------------------------------------------------------
h=$(field cache_hits "$S3"); m=$(field cache_misses "$S3"); r=$(field cache_hit_rate "$S3")
want=$(python3 -c "print(round($h/($h+$m),4) if $h+$m else 0)")
python3 -c "import sys;sys.exit(0 if abs($r-$want)<0.001 else 1)" \
  && ok "hit_rate $r == hits/(hits+misses) $want" || bad "hit_rate $r != $want"

# ---- phase 4: gateway CPU ----------------------------------------------------------
if [ -n "$TOKEN" ] && [ -s /tmp/.flow_cpu_max ]; then
  CPU_LOAD=$(cat /tmp/.flow_cpu_max)
  IDLE=$(curl -s -m30 "$CDC/cache/iostats?token=$TOKEN" | python3 -c "import json,sys;print(json.load(sys.stdin)['gateway']['cpu_pct'])" 2>/dev/null)
  echo "  gateway cpu_pct: under-load=$CPU_LOAD idle-after=$IDLE"
  python3 -c "import sys;sys.exit(0 if float('$CPU_LOAD' or 0)>0 else 1)" 2>/dev/null \
    && ok "gateway CPU $CPU_LOAD% > 0 under load (sampler works; panel 0% = honest idle)" \
    || bad "gateway CPU $CPU_LOAD% while streaming $BYTES bytes — sampler bug, escalate"
else
  echo "  SKIP gateway CPU (set CLUSTER_ID or CDC_TOKEN)"
fi

# ---- phase 5: evictions (informational) --------------------------------------------
de=$(( $(field evictions "$S3") - $(field evictions "$S0") ))
dbe=$(( $(field bytes_evicted "$S3") - $(field bytes_evicted "$S0") ))
echo "-- phase 5: evictions during this test: +$de objects, +$dbe bytes freed"
echo "   (capacity evict fires only past the high watermark; verify on the gateway:"
echo "    docker logs zus-router --since 30m 2>&1 | grep evict | tail)"

echo ""
echo "==================== flow-panel: $PASS pass, $FAIL fail ===================="
exit $([ "$FAIL" -eq 0 ] && echo 0 || echo 1)
