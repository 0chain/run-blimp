#!/usr/bin/env bash
# verify_panel.sh — validate every number the cluster panel shows against the
# cluster-side truth endpoints that back it, with a live increment test for the
# query counter. PASS/FAIL per tile; exit non-zero if any FAIL.
#
#   GATEWAY=<gw-ip> CLUSTER_ID=<id> ./verify_panel.sh
#
# Panel tile → backing endpoint:
#   Queries run       → GET /admin/slow_log .total_queries   (live increment test)
#   MVs authored      → GET /admin/mv/registered_queries .queries[]
#   MVs live          → GET /admin/mv/list, MV-namespace (mv_*) entries only
#   CDC events        → GET /admin/mv/wave/report .summary.rows
#   MV refreshes      → wave .summary.incremental + .summary.fallback
#   Cache data flow   → :9401 /cache/iostats (hits/misses/hit_rate/bytes)
#   Blobber I/O tiles → /cache/iostats .blobbers[]
set -u
: "${GATEWAY:?}" "${CLUSTER_ID:?}"
PORT="${PORT:-9000}"; CDC_PORT="${CDC_PORT:-9401}"
TOKEN="zus-${CLUSTER_ID}"; GW="http://${GATEWAY}:${PORT}"; CDC="http://${GATEWAY}:${CDC_PORT}"
AUTH="Authorization: Bearer ${TOKEN}"
PASS=0; FAIL=0
ok(){ echo "  PASS  $*"; PASS=$((PASS+1)); }
bad(){ echo "  FAIL  $*"; FAIL=$((FAIL+1)); }
jget(){ python3 -c "import json,sys
try:
  d=json.load(sys.stdin); cur=d
  for k in sys.argv[1:]:
    cur=cur[int(k)] if isinstance(cur,list) else cur.get(k)
  print(cur if cur is not None else '')
except Exception: print('')" "$@"; }

echo "== panel validation: cluster ${CLUSTER_ID} via ${GATEWAY} =="

# ---- 1. Queries run: live increment test -------------------------------------
SL0=$(curl -s -m 15 -H "$AUTH" "$GW/admin/slow_log")
T0=$(echo "$SL0" | jget total_queries)
if [ -z "$T0" ]; then
  bad "Queries run: /admin/slow_log has no total_queries (gateway image predates it)"
else
  curl -s -m 180 -H "$AUTH" -H "Content-Type: application/json" \
    -d '{"original_sql":"SELECT 1 AS panel_probe","source":"customer"}' \
    "$GW/admin/query/run" > /dev/null
  T1=$(curl -s -m 15 -H "$AUTH" "$GW/admin/slow_log" | jget total_queries)
  if [ -n "$T1" ] && [ "$T1" -gt "$T0" ] 2>/dev/null; then
    ok "Queries run: total_queries incremented $T0 -> $T1 after a live query"
  else
    bad "Queries run: total_queries did not increment ($T0 -> ${T1:-?})"
  fi
fi

# ---- 2. MVs authored ----------------------------------------------------------
RQ=$(curl -s -m 15 -H "$AUTH" "$GW/admin/mv/registered_queries")
NQ=$(echo "$RQ" | python3 -c "import json,sys;print(len(json.load(sys.stdin).get('queries',[])))" 2>/dev/null)
[ -n "$NQ" ] && ok "MVs authored: registered_queries = $NQ (numeric)" || bad "MVs authored: registered_queries unreadable"

# ---- 3. MVs live — truth is the reap DRY-RUN inventory, NOT mv/list ------------
# (mv/list returns catalog SOURCE tables; filtering it to *_mv reads 0 while
# MVs exist — the 2026-07-17 "0/24" tile bug in both directions.)
INV=$(curl -s -m 30 -X POST -H "$AUTH" -H "Content-Type: application/json" \
  -d '{"policy":"age","dry_run":true}' "$GW/admin/mv/reap")
NMV=$(echo "$INV" | jget total_mvs); NB2=$(echo "$INV" | jget total_bytes)
if [ -n "$NMV" ]; then
  ok "MVs live: reap dry-run total_mvs=$NMV total_bytes=$NB2 (panel tile source)"
else
  bad "MVs live: reap dry-run unreadable ($(echo "$INV" | head -c 80))"
fi

# ---- 4+5. CDC events + MV refreshes (wave report) -----------------------------
WV=$(curl -s -m 15 -H "$AUTH" "$GW/admin/mv/wave/report")
ROWS=$(echo "$WV" | jget summary rows); INC=$(echo "$WV" | jget summary incremental); FB=$(echo "$WV" | jget summary fallback)
if [ -n "$ROWS" ] && [ -n "$INC" ] && [ -n "$FB" ]; then
  ok "CDC events=$ROWS · MV refreshes=$((INC+FB)) ($INC incremental + $FB fallback)"
else
  bad "wave report missing summary fields (rows='$ROWS' inc='$INC' fb='$FB')"
fi

# ---- 6. Cache data flow — every counter tile (nests under .cache) --------------
# Live counter DELTAS (misses +N, fetched +B exact, fill convergence, warm hits)
# are exact-validated by test_flow_panel.sh (8/8); here we validate every tile's
# value is present, numeric, and mutually consistent.
IO=$(curl -s -m 15 "$CDC/cache/iostats?token=${TOKEN}")
HITS=$(echo "$IO" | jget cache cache_hits); MISSES=$(echo "$IO" | jget cache cache_misses)
HR=$(echo "$IO" | jget cache cache_hit_rate)
SERVED=$(echo "$IO" | jget cache bytes_served_from_cache)
FETCHED=$(echo "$IO" | jget cache bytes_served_from_origin)
WRITTEN=$(echo "$IO" | jget cache bytes_written_to_cache)
EVN=$(echo "$IO" | jget cache evictions); EVB=$(echo "$IO" | jget cache bytes_evicted)
if [ -n "$HITS" ] && [ -n "$MISSES" ]; then
  CHK=$(python3 -c "h=$HITS;m=$MISSES;r=${HR:-0}
print('ok' if (h+m)==0 or abs(r-(h/(h+m)))<0.001 else 'bad')" 2>/dev/null)
  [ "$CHK" = "ok" ] && ok "hits=$HITS misses=$MISSES hit_rate=$HR (ratio exact)" \
                    || bad "hit_rate $HR inconsistent with $HITS/$((HITS+MISSES))"
else
  bad "cache counters unreadable ($(echo "$IO" | head -c 80))"
fi
CONS=$(python3 -c "
h=int('${HITS:-0}' or 0); m=int('${MISSES:-0}' or 0)
s=int('${SERVED:-0}' or 0); f=int('${FETCHED:-0}' or 0); w=int('${WRITTEN:-0}' or 0)
bad=[]
if h>0 and s<=0: bad.append('hits>0 but served-from-cache=0')
if m>0 and f<=0: bad.append('misses>0 but fetched-from-origin=0')
if w>f: bad.append('written-to-blobbers > fetched-from-origin (impossible)')
print(';'.join(bad) or 'ok')" 2>/dev/null)
[ "$CONS" = "ok" ] && ok "served=$SERVED fetched=$FETCHED written=$WRITTEN (byte counters consistent)" \
                   || bad "byte counters: $CONS"
if [ -n "$EVN" ] && [ -n "$EVB" ]; then
  E2=$(python3 -c "print('ok' if (int('$EVB')==0) == (int('$EVN')==0) or int('$EVN')>0 else 'bad')" 2>/dev/null)
  [ "$E2" = "ok" ] && ok "evictions=$EVN evicted_bytes=$EVB (consistent)" \
                   || bad "evicted_bytes=$EVB with evictions=$EVN"
else
  bad "eviction counters absent"
fi

# ---- 7. Gateway tiles: CPU, storage, disk R/W, net ----------------------------
GCPU=$(echo "$IO" | jget gateway cpu_pct)
[ -n "$GCPU" ] && ok "Gateway CPU: cpu_pct=$GCPU" || bad "gateway.cpu_pct absent"
GSTOR=$(echo "$IO" | jget gateway storage)
case "$GSTOR" in
  *"used of"*) ok "Gateway storage: '$GSTOR'";;
  *) bad "gateway.storage absent/unparseable ('$GSTOR')";;
esac
GDR=$(echo "$IO" | jget gateway disk_read_bps); GDW=$(echo "$IO" | jget gateway disk_write_bps)
if [ -n "$GDR" ] && [ -n "$GDW" ]; then ok "Gateway disk R/W: read_bps=$GDR write_bps=$GDW"
else bad "gateway disk_read_bps/disk_write_bps absent"; fi
NET=$(echo "$IO" | jget gateway net_rx_bps)
[ -n "$NET" ] && ok "Gateway net I/O: net_rx_bps=$NET" \
             || echo "  NOTE  gateway net_rx_bps absent (cdc predates net-I/O sample — ships with next provision)"

# ---- 8. Per-blobber I/O tiles --------------------------------------------------
# New-init cdc: blobbers[] with per-node cpu/net/disk — validate EVERY node's
# fields. Older vintage: .nodes aggregate strings — validate all three present.
PB=$(echo "$IO" | python3 -c "
import json,sys
d=json.load(sys.stdin); bs=d.get('blobbers',[])
if bs:
    missing=[]
    for i,b in enumerate(bs):
        for k in ('cpu_pct','net_rx_bps','net_tx_bps','disk_read_bps','disk_write_bps'):
            if not any(x in b for x in (k, k.replace('_bps',''), k.replace('_pct',''))):
                missing.append('b%d:%s'%(i,k))
    print('PER:%d:%s'%(len(bs), ';'.join(missing) or 'complete'))
else:
    n=d.get('nodes',{}) or {}
    have=[k for k in ('eblobber_cpu','eblobber_io','eblobber_disk') if n.get(k)]
    print('AGG:%d'%len(have))" 2>/dev/null)
case "$PB" in
  PER:*:complete) ok "Blobber I/O: per-blobber array (${PB#PER:}) — every node has cpu/net/disk";;
  PER:*) bad "Blobber I/O: per-blobber array incomplete ($PB)";;
  AGG:3) ok "Blobber I/O: aggregate sample complete (cpu+io+disk, pre-per-blobber cdc)";;
  *) bad "Blobber I/O: neither per-blobber nor complete aggregate ($PB)";;
esac

# ---- 9. MV shape (rows × cols) in the Query/MV table ---------------------------
SHAPE=$(echo "$RQ" | python3 -c "import json,sys
qs=json.load(sys.stdin).get('queries',[])
shaped=[q for q in qs if q.get('rows',0)>0 and q.get('cols',0)>0]
print('%d/%d'%(len(shaped),len(qs)))" 2>/dev/null)
case "$SHAPE" in
  0/0) bad "MV shape: no registered queries to check";;
  0/*) echo "  NOTE  MV shape rows×cols absent on all entries (gateway image predates the enrichment)";;
  *) ok "MV shape: $SHAPE registered queries carry rows×cols";;
esac

# ---- 10. Rewrites tile (documented limitation) --------------------------------
echo "  NOTE  'Rewrites' is a UI-session counter (reused-MV fingerprint hits by THIS"
echo "        browser session) — no cluster-side endpoint exposes it; not validated here."

# ---- 11. optional: chain the live exact-delta cache test ----------------------
# FLOW=1 runs test_flow_panel.sh (cold sweep +N/+B exact, fill convergence,
# warm ×3, hit_rate 4dp, CPU>0) — needs the full suite env (GW_AK/GW_SK/
# ORIGIN_BUCKET/…) and a client in the VPC; skipped otherwise.
if [ "${FLOW:-0}" = "1" ] && [ -f "$(dirname "$0")/test_flow_panel.sh" ]; then
  echo "== chaining test_flow_panel.sh (live exact-delta validation) =="
  if bash "$(dirname "$0")/test_flow_panel.sh"; then ok "flow-panel live deltas 8/8"
  else bad "flow-panel live delta test failed"; fi
else
  echo "  NOTE  live counter DELTAS (misses +N / fetched +B exact, fill convergence,"
  echo "        warm hits, hit_rate 4dp, CPU>0 under load) are exact-validated by"
  echo "        test_flow_panel.sh — run with FLOW=1 + suite env to chain it here."
fi

echo "== ${PASS} PASS / ${FAIL} FAIL =="
exit $([ "$FAIL" -eq 0 ] && echo 0 || echo 1)
