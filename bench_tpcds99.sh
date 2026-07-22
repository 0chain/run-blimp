#!/usr/bin/env bash
# bench_tpcds99.sh — run ALL 99 TPC-DS queries through the optimizer and produce
# per-query + distribution stats:
#   pass 1 (cold): POST /admin/query/run per query — author_ms / materialize_ms /
#                  query_ms / engine / rows / status (fresh author unless the
#                  library already has it)
#   pass 2 (warm): re-run every OK query — result-cache / MV serve time
# Writes a TSV (qid, cold status, author_ms, materialize_ms, cold query_ms,
# engine, rows, warm query_ms, warm engine) to $OUT and prints distributions.
#
#   pass 3 (cdc):  ONE append commit to store_returns (+snapshot_changed), then
#                  re-run every OK query — measures the refresh blast radius:
#                  store_returns-sourced MVs refresh (incremental or full, from
#                  the wave log), everything else must still serve cached.
#
# Env: GW CLUSTER_ID [Q_DIR=~/tpcds_queries] [OUT=./tpcds99_results.tsv]
#      [TIMEOUT_S=900] [SOURCE=customer]
#      pass 3 additionally needs: ICEBERG_URL WAREHOUSE [REGION] [CDC_ROWS=50000]
#      (skipped, with a note, when they're unset)
# Heavy queries scan billions of rows on sf1000 — expect HOURS for the cold
# pass; each query is capped at TIMEOUT_S (default 200s — an MV too big to
# author in that window is recorded as timeout and we move on) — not fatal.
set -u
Q_DIR="${Q_DIR:-$HOME/tpcds_queries}"; OUT="${OUT:-./tpcds99_results.tsv}"
TIMEOUT_S="${TIMEOUT_S:-200}"; SOURCE="${SOURCE:-customer}"
CDC_ROWS="${CDC_ROWS:-50000}"; NAMESPACE="${NAMESPACE:-tpcds}"; REGION="${REGION:-ap-south-1}"
HERE="$(cd "$(dirname "$0")" && pwd)"
QAPI="http://$GW:9000"; TOKEN="zus-$CLUSTER_ID"
now(){ date +%s.%N; }; el(){ awk -v a="$1" -v z="$2" 'BEGIN{printf "%.0f",(z-a)*1000}'; }
J(){ python3 -c "import json,sys;print(json.load(sys.stdin).get('$1',''))" 2>/dev/null; }
# $2 (optional) = display label for the run row (qid), shown in the panel.
run_sql(){ curl -s -m "$TIMEOUT_S" "$QAPI/admin/query/run" -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "$(python3 -c 'import json,sys;b={"original_sql":sys.argv[1],"source":sys.argv[2]};
b.update({"label":sys.argv[3]} if len(sys.argv)>3 and sys.argv[3] else {});
import os
b.update({"skip_passthrough":True} if os.environ.get("SKIP_PASSTHROUGH")=="1" else {});print(json.dumps(b))' "$1" "$SOURCE" "${2:-}")"; }

[ -d "$Q_DIR" ] || { echo "FATAL: $Q_DIR missing (generate via duckdb tpcds extension)"; exit 1; }
printf 'qid\tstatus\tauthor_ms\tmaterialize_ms\tcold_query_ms\tengine\trows\twarm_query_ms\twarm_engine\tcdc_query_ms\tcdc_materialize_ms\tcdc_engine\tmv_table\n' > "$OUT"

T_ALL0=$(now); NOK=0; NERR=0
for qf in $(ls "$Q_DIR"/q*.sql | sort -V); do
  qid=$(basename "$qf" .sql); SQL=$(cat "$qf")
  T0=$(now); R=$(run_sql "$SQL" "$qid"); T1=$(now)
  ST=$(echo "$R" | J status); [ -z "$ST" ] && ST="timeout/err"
  AU=$(echo "$R" | J author_ms); MA=$(echo "$R" | J materialize_ms)
  QM=$(echo "$R" | J query_ms); EN=$(echo "$R" | J engine); RW=$(echo "$R" | J rows)
  if [ "$ST" = "ok" ]; then NOK=$((NOK+1)); else NERR=$((NERR+1)); fi
  MV=$(echo "$R" | J mv_table)
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s' "$qid" "$ST" "${AU:-0}" "${MA:-0}" "${QM:-0}" "${EN:--}" "${RW:-0}" >> "$OUT"
  # warm pass immediately (result cache / MV serve)
  if [ "$ST" = "ok" ]; then
    R2=$(run_sql "$SQL" "$qid"); Q2=$(echo "$R2" | J query_ms); E2=$(echo "$R2" | J engine)
    printf '\t%s\t%s' "${Q2:-0}" "${E2:--}" >> "$OUT"
  else
    printf '\t-\t-' >> "$OUT"
  fi
  # cdc columns filled by pass 3; placeholders + mv_table for the join
  printf '\t-\t-\t-\t%s\n' "${MV:--}" >> "$OUT"
  echo "  $qid: $ST author=${AU:-0}ms mat=${MA:-0}ms cold=${QM:-0}ms warm=${Q2:-—}ms engine=${EN:--} wall=$(el "$T0" "$T1")ms"
  Q2=""
done
T_ALL1=$(now)

# ---- pass 3: one CDC commit, then re-run every OK query ---------------------------
if [ -n "${ICEBERG_URL:-}" ] && [ -n "${WAREHOUSE:-}" ] && [ -f "$HERE/seed_tpcds.py" ]; then
  echo
  echo "== pass 3 (cdc): +$CDC_ROWS rows to store_returns, snapshot_changed, re-run all =="
  PY3="$HOME/venv_ib/bin/python3"; [ -x "$PY3" ] || PY3=python3
  "$PY3" "$HERE/seed_tpcds.py" --catalog "$ICEBERG_URL" --warehouse "$WAREHOUSE" \
    --namespace "$NAMESPACE" --table store_returns --rows "$CDC_ROWS" --s3-region "$REGION" 2>&1 | tail -1
  curl -s -m 60 "$QAPI/admin/source/snapshot_changed" -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"namespace\":\"$NAMESPACE\",\"table\":\"store_returns\",\"trigger\":\"tpcds99-cdc\"}" >/dev/null
  TMP=$(mktemp)
  head -1 "$OUT" > "$TMP"
  tail -n +2 "$OUT" | while IFS=$'\t' read -r qid st au ma qm en rw wq we _ _ _ mv; do
    if [ "$st" = "ok" ] && [ -f "$Q_DIR/$qid.sql" ]; then
      R3=$(run_sql "$(cat "$Q_DIR/$qid.sql")" "$qid")
      Q3=$(echo "$R3" | J query_ms); M3=$(echo "$R3" | J materialize_ms); E3=$(echo "$R3" | J engine)
      printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$qid" "$st" "$au" "$ma" "$qm" "$en" "$rw" "$wq" "$we" "${Q3:-0}" "${M3:-0}" "${E3:--}" "$mv" >> "$TMP"
      echo "  $qid: cdc query=${Q3:-?}ms materialize=${M3:-0}ms engine=${E3:--}"
    else
      printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t-\t-\t-\t%s\n' \
        "$qid" "$st" "$au" "$ma" "$qm" "$en" "$rw" "$wq" "$we" "$mv" >> "$TMP"
    fi
  done
  mv "$TMP" "$OUT"
  # wave modes for the refreshes this pass triggered
  echo "  wave modes (last 40):"
  curl -s -m 30 "$QAPI/admin/mv/wave/report?limit=40" -H "Authorization: Bearer $TOKEN" | python3 -c '
import json,sys
from collections import Counter
ws=json.load(sys.stdin).get("waves",[])
print("   ", Counter(w.get("mode","?") for w in ws))' 2>/dev/null
else
  echo "  NOTE: pass 3 (cdc) skipped — set ICEBERG_URL + WAREHOUSE (+ seed_tpcds.py) to enable"
fi

echo
echo "== TPC-DS 99 SUMMARY (cluster $CLUSTER_ID, source=$SOURCE) =="
echo "  total wall: $(awk -v a="$T_ALL0" -v z="$T_ALL1" 'BEGIN{printf "%.0f min",(z-a)/60}')  ·  ok=$NOK err=$NERR"
python3 - "$OUT" <<'PYEOF'
import sys, statistics as st
rows=[l.rstrip('\n').split('\t') for l in open(sys.argv[1])][1:]
ok=[r for r in rows if r[1]=='ok']
def num(v):
    try: return float(v)
    except: return 0.0
def dist(label, vals):
    v=sorted(x for x in vals if x>0)
    if not v: print('  %-28s n=0'%label); return
    p90=v[min(len(v)-1,int(len(v)*0.9))]
    print('  %-28s n=%d min=%.0f median=%.0f avg=%.0f p90=%.0f max=%.0f (ms)'%(label,len(v),v[0],st.median(v),st.mean(v),p90,v[-1]))
authored=[r for r in ok if num(r[2])>0]
reused=[r for r in ok if num(r[2])==0]
print('  authored fresh: %d · library/scheme reuse: %d · failed: %d'%(len(authored),len(reused),len(rows)-len(ok)))
dist('author_ms (fresh)', [num(r[2]) for r in authored])
dist('materialize_ms (fresh)', [num(r[3]) for r in authored])
dist('cold query_ms (all ok)', [num(r[4]) for r in ok])
dist('warm query_ms (all ok)', [num(r[7]) for r in ok])
warm0=[r for r in ok if r[7] not in ('-','') and num(r[7])==0]
print('  warm 0ms result-cache hits: %d/%d'%(len(warm0),len(ok)))
cdc=[r for r in ok if len(r)>11 and r[9] not in ('-','')]
if cdc:
    refreshed=[r for r in cdc if num(r[10])>0]
    cached=[r for r in cdc if num(r[10])==0 and num(r[9])==0]
    print('  cdc blast radius: %d refreshed · %d untouched (served cached) · of %d'%(len(refreshed),len(cached),len(cdc)))
    dist('cdc refresh materialize_ms', [num(r[10]) for r in refreshed])
    dist('cdc query_ms (refreshed)', [num(r[9]) for r in refreshed])
fails=[(r[0]) for r in rows if r[1]!='ok']
if fails: print('  failed qids:', ' '.join(fails))
PYEOF
echo "  per-query TSV: $OUT"
