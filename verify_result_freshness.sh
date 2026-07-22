#!/usr/bin/env bash
# verify_result_freshness.sh — proves the gateway's serve-time freshness gate:
# after a CDC append + snapshot_changed, a repeat query must RE-EXECUTE over the
# delta-merged MV (not serve the pre-append result parquet at 0 ms), and the
# following warm run must serve the refreshed result from the cache.
#
# Env (same contract as test_query.sh): GW GW_AK GW_SK CLUSTER_ID ICEBERG_URL
# WAREHOUSE [NAMESPACE=tpcds] [REGION=ap-south-1] [CDC_ROWS=50000]
#
# PASS criteria (NOT "the answer must change" — a small random append can leave
# q1's `order by c_customer_id limit 100` legitimately identical):
#   1. post-append wall-clock ≫ warm wall-clock (re-execution, not 0 ms serve)
#   2. warm hash == post-append hash (refreshed result is what the cache serves)
#   3. (definitive) gateway log shows `[query-run] done ... engine=duckdb` with a
#      new qresults parquet write between the append and the warm HIT — and/or
#      the post-append rows match an independent Spark run over the source.
set -u
NAMESPACE="${NAMESPACE:-tpcds}"; REGION="${REGION:-ap-south-1}"; CDC_ROWS="${CDC_ROWS:-50000}"
QAPI="http://$GW:9000"; TOKEN="zus-$CLUSTER_ID"
Q1=$(cat <<'SQL'
with customer_total_return as
 (select sr_customer_sk as ctr_customer_sk, sr_store_sk as ctr_store_sk,
         sum(sr_return_amt) as ctr_total_return
  from store_returns, date_dim
  where sr_returned_date_sk = d_date_sk and d_year = 2000
  group by sr_customer_sk, sr_store_sk)
select c_customer_id
from customer_total_return ctr1, store, customer
where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
        from customer_total_return ctr2 where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
  and s_store_sk = ctr1.ctr_store_sk and s_state = 'TN'
  and ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id limit 100
SQL
)
run(){ local out=$1 t0 t1; t0=$(date +%s.%N)
  curl -s -m 900 "$QAPI/admin/query/run" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d "$(python3 -c 'import json,sys;print(json.dumps({"original_sql":sys.argv[1],"source":"customer"}))' "$Q1")" > "$out"
  t1=$(date +%s.%N); awk -v a="$t0" -v z="$t1" 'BEGIN{printf "%.2f", z-a}'; }
rh(){ local sig bkt
  sig=$(python3 -c "import json;print(json.load(open('$1')).get('result_sig',''))")
  bkt=$(python3 -c "import json;print(json.load(open('$1')).get('result_bucket',''))")
  [ -z "$sig" ] && { echo NO_SIG; return; }
  duckdb -noheader -list -c "INSTALL httpfs; LOAD httpfs;
    SET s3_endpoint='$GW:9000'; SET s3_use_ssl=false; SET s3_url_style='path';
    SET s3_access_key_id='$GW_AK'; SET s3_secret_access_key='$GW_SK';
    SELECT * FROM read_parquet('s3://$bkt/qresults/$sig/*.parquet');" 2>/dev/null | sort | sha256sum | awk '{print $1}'; }

echo "== 1. pre-append q1 =="
W0=$(run /tmp/vf_pre.json); H0=$(rh /tmp/vf_pre.json); echo "wall=${W0}s hash=$H0"
echo "== 2. append $CDC_ROWS rows + snapshot_changed =="
PY3="$HOME/venv_ib/bin/python3"; [ -x "$PY3" ] || PY3=python3
"$PY3" "$(dirname "$0")/seed_tpcds.py" --catalog "$ICEBERG_URL" --warehouse "$WAREHOUSE" \
  --namespace "$NAMESPACE" --table store_returns --rows "$CDC_ROWS" --s3-region "$REGION" 2>&1 | tail -1
curl -s -m 60 "$QAPI/admin/source/snapshot_changed" -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" -d "{\"namespace\":\"$NAMESPACE\",\"table\":\"store_returns\",\"trigger\":\"freshness-verify\"}"; echo
echo "== 3. wait for delta merge (poll wave report, cap 300s) =="
for i in $(seq 1 60); do
  sleep 5
  # a merge for store_returns after our append shows as an incremental/fallback
  # wave newer than the pre-append state; poll until one lands (or cap).
  W=$(curl -s -m 15 "$QAPI/admin/mv/wave/report?limit=5" -H "Authorization: Bearer $TOKEN" 2>/dev/null       | python3 -c "import json,sys
try:
  ws=json.load(sys.stdin).get('waves',[])
  print(1 if any('store_returns' in str(w.get('old_snapshots','')) and w.get('mode') in ('incremental','fallback') for w in ws[:3]) else 0)
except Exception: print(0)" 2>/dev/null)
  [ "$W" = "1" ] && { echo "   merge landed after ~$((i*5))s"; break; }
done
echo "== 4. post-append q1 =="
W1=$(run /tmp/vf_post.json); H1=$(rh /tmp/vf_post.json); echo "wall=${W1}s hash=$H1"
echo "== 5. warm re-run =="
W2=$(run /tmp/vf_warm.json); H2=$(rh /tmp/vf_warm.json); echo "wall=${W2}s hash=$H2"
echo "== VERDICT =="
REEXEC=$(awk -v p="$W1" -v w="$W2" 'BEGIN{print (p > w*5 && p > 1.0) ? "yes" : "no"}')
FFAIL=0
[ "$REEXEC" = "yes" ] && echo "PASS: post-append re-executed (${W1}s) instead of a warm serve (${W2}s)" \
                      || { echo "FAIL: post-append looks like a cached serve (${W1}s vs warm ${W2}s) — check gateway log for [query-run][result-cache] HIT before a re-execution"; FFAIL=1; }
[ "$H1" = "$H2" ] && echo "PASS: warm run serves the refreshed result (hash match)" || { echo "FAIL: warm hash differs from post-append"; FFAIL=1; }
[ "$H0" = "$H1" ] && echo "NOTE: answer unchanged by this append (legitimate for q1 limit-100; confirm with a Spark referee run if in doubt)" \
                  || echo "NOTE: answer changed post-append (hash_pre != hash_post) — fresh data served"
exit ${FFAIL:-0}
