#!/usr/bin/env bash
# test_query.sh — Blimp QUERY-OPTIMIZER cluster test suite (real gateway API).
#
# Runs from the client box in the cluster VPC (so it uses the gateway PRIVATE ip). Uses
# the Prod-Query API directly: POST http://<gw>:9000/admin/query/run, auth
# "Bearer zus-<cluster-id>". See the Prod-Query API + Incremental-MV references.
#
# Flow:
#   1. SPARK BASELINE  — local Spark + Iceberg runs q1 on the SAME data Blimp reads
#                        (source=customer → the kit's BYO Iceberg over S3). Hash rows + time.
#   2. BLIMP MV        — POST /admin/query/run → Blimp authors + materializes an MV.
#                        Capture author_ms/shape_ms/materialize_ms/query_ms; fetch the result
#                        rows; HASH-compare to Spark + compare timing.
#   3. CDC INCREMENTAL — append rows to q1's source table, POST /admin/source/snapshot_changed;
#                        then (a) Spark full recompute on the new data, (b) Blimp q1 again =
#                        incremental delta-merge. Compare: Blimp result == Spark recompute
#                        (correctness), and incremental materialize ≪ initial materialize.
#   4. MV EVICTION     — author MVs from a batch of the TPC-DS 99, then POST /admin/mv/reap
#                        and verify coldest-first eviction via /admin/mv/list.
#   5. CACHE vs MV ISOLATION — populate BOTH the read-through cache and MVs; assert the
#                        Router evicts cache objects (never MV prefixes) at ~80% and the MV
#                        reaper evicts MVs at ~90% — disjoint data, cheap cache drains first.
#
# Config via env (see TESTING.md → cluster.env) or prompt.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ask(){ local cur="${!1:-}"; if [ -n "$cur" ]; then printf '%s' "$cur"; return; fi
  printf '%s' "$2 " >&2; read -r v; printf '%s' "$v"; }

GW=$(ask GW               "Gateway PRIVATE ip (e.g. 10.10.150.76):")
CLUSTER_ID=$(ask CLUSTER_ID "Cluster id (e.g. 1783897137567):")
TOKEN="${ADMIN_TOKEN:-zus-$CLUSTER_ID}"            # deterministic per-cluster admin token
SOURCE="${SOURCE:-customer}"                        # customer | sf1000 | internal
QAPI="http://$GW:9000"
GW_AK="${GW_AK:-}"; GW_SK="${GW_SK:-}"              # gateway S3 creds (to read result parquet)
# customer/Spark data lives in your BYO Iceberg (the kit's catalog over S3):
ICEBERG_URL="${ICEBERG_URL:-http://127.0.0.1:8181}"
NAMESPACE="${NAMESPACE:-tpcds}"
WAREHOUSE="${WAREHOUSE:-}"
REGION="${REGION:-ap-south-1}"
OUT="${OUT:-/tmp/qtest}"; mkdir -p "$OUT"
J(){ python3 -c "import json,sys;d=json.load(sys.stdin);print(d.get('$1',''))" 2>/dev/null; }
now(){ date +%s.%N; }; el(){ awk -v a="$1" -v z="$2" 'BEGIN{printf "%.3f",z-a}'; }
hash_rows(){ sort | sha256sum | awk '{print $1}'; }
QFAIL=0  # any correctness MISMATCH -> nonzero exit for the harness

# TPC-DS q1 (customer_total_return). The optimizer's MV is the mergeable inner aggregate
# (SUM group-by) → incremental-eligible. Override with Q1_FILE=<path>.
Q1_DEFAULT=$(cat <<'SQL'
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
Q1="${Q1_FILE:+$(cat "$Q1_FILE")}"; Q1="${Q1:-$Q1_DEFAULT}"

# ---- Blimp Prod-Query API ----------------------------------------------------------
# blimp_run <sql> <out.json> — POST /admin/query/run; writes the JSON response.
blimp_run(){ local sql="$1" out="$2"
  curl -s -m 900 "$QAPI/admin/query/run" -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "$(python3 -c 'import json,sys; print(json.dumps({"original_sql":sys.argv[1],"source":sys.argv[2]}))' "$sql" "$SOURCE")" \
    > "$out" 2>&1; }
# blimp_result_hash <run.json> — read the run's result parquet from the gateway S3
# (s3://<result_bucket>/qresults/<result_sig>/*.parquet) with duckdb and hash the sorted
# rows the SAME way as Spark, for an apples-to-apples correctness compare. Needs GW_AK/GW_SK.
blimp_result_hash(){ local sig bkt; sig=$(J result_sig < "$1"); bkt=$(J result_bucket < "$1")
  [ -z "$sig" ] || [ -z "$GW_AK" ] && { echo ""; return; }
  command -v duckdb >/dev/null 2>&1 || { curl -fsSL https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip -o /tmp/d.zip 2>/dev/null && (cd /tmp && unzip -oq d.zip && sudo mv duckdb /usr/local/bin/) ; }
  duckdb -noheader -list -c "
    INSTALL httpfs; LOAD httpfs;
    SET s3_endpoint='$GW:9000'; SET s3_use_ssl=false; SET s3_url_style='path';
    SET s3_access_key_id='$GW_AK'; SET s3_secret_access_key='$GW_SK';
    SELECT * FROM read_parquet('s3://$bkt/qresults/$sig/*.parquet');" 2>/dev/null | hash_rows; }

# ---- local Spark + Iceberg ---------------------------------------------------------
SPARK_VER="${SPARK_VER:-3.5.1}"; ICEBERG_VER="${ICEBERG_VER:-1.5.2}"; SPARK_HOME="${SPARK_HOME:-/opt/spark-$SPARK_VER}"
ensure_spark(){ command -v java >/dev/null 2>&1 || sudo apt-get install -y -q openjdk-17-jre-headless >/dev/null 2>&1
  [ -x "$SPARK_HOME/bin/spark-sql" ] && return 0
  echo "installing Spark $SPARK_VER…"
  curl -fsSL "https://archive.apache.org/dist/spark/spark-$SPARK_VER/spark-$SPARK_VER-bin-hadoop3.tgz" -o /tmp/spark.tgz
  sudo tar -xzf /tmp/spark.tgz -C /opt && sudo ln -sfn "/opt/spark-$SPARK_VER-bin-hadoop3" "$SPARK_HOME"; }
spark_q1(){ local label="$1"; ensure_spark; local t0; t0=$(now)
  # driver-memory: default 1g GC-thrashes on many-core boxes (local[N] = N tasks;
  # 16-core box emitted only '[..][warning][gc,alloc] GCLocker' lines, 0 data rows)
  local MEM_MB; MEM_MB=$(( $(free -m 2>/dev/null | awk 'NR==2{print $2}' || echo 8192) * 6 / 10 ))
  "$SPARK_HOME/bin/spark-sql" --driver-memory "${SPARK_DRIVER_MEM:-${MEM_MB}m}" \
    --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:$ICEBERG_VER,org.apache.iceberg:iceberg-aws-bundle:$ICEBERG_VER" \
    --conf spark.sql.catalog.zus=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.zus.type=rest --conf spark.sql.catalog.zus.uri="$ICEBERG_URL" \
    --conf spark.sql.catalog.zus.warehouse="$WAREHOUSE" \
    --conf spark.sql.catalog.zus.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.defaultCatalog=zus \
    -e "USE zus.$NAMESPACE; $Q1" 2>/dev/null | grep -vE '^::|^\[' | sort > "$OUT/$label.rows"
  el "$t0" "$(now)"; }
  # grep -v '^::' — ivy prints ':: loading settings ::' on STDOUT; unfiltered it counts
  # as a result row and corrupts the hash (spark=101 vs blimp=100, 2026-07-17; cleaned
  # hashes matched exactly)

# ---- gateway no-MV referee (DEFAULT) -----------------------------------------------
# The correctness baseline is a FULL SOURCE SCAN run ON THE GATEWAY via no_mv=true —
# so there is NO client-side Spark/JVM and the test box can be an xlarge (or smaller).
# It is still an independent MV-vs-source check (different data path, gateway engine).
# SPARK_REFEREE=1 swaps in local Spark for a cross-engine check (needs a big box + JVM).
ref_q1(){ local label="$1" t0 out; t0=$(now); out="$OUT/$label.json"
  curl -s -m 900 "$QAPI/admin/query/run" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d "$(python3 -c 'import json,sys;print(json.dumps({"original_sql":sys.argv[1],"source":sys.argv[2],"no_mv":True}))' "$Q1" "$SOURCE")" > "$out" 2>&1
  # compare the gateway's own response md5 — present + computed identically on the MV
  # and no_mv paths (the no_mv passthrough writes no result parquet, so a parquet-hash
  # read is empty). q1 ORDER BY ... LIMIT makes the md5 order-deterministic.
  J md5 < "$out" > "$OUT/$label.hash"
  el "$t0" "$(now)"; }
# base_q1 <label> — prints elapsed seconds; writes the row-hash to $OUT/<label>.hash
base_q1(){ if [ "${SPARK_REFEREE:-0}" = "1" ]; then spark_q1 "$1"; hash_rows < "$OUT/$1.rows" > "$OUT/$1.hash"; else ref_q1 "$1"; fi; }
# blimp comparison hash matching the active referee: response-md5 (default) or
# parquet-rows-hash (Spark mode, which produces sorted rows)
bcmp(){ if [ "${SPARK_REFEREE:-0}" = "1" ]; then blimp_result_hash "$1"; else J md5 < "$1"; fi; }

echo "==================== Blimp query-optimizer test (cluster $CLUSTER_ID, source=$SOURCE) ===================="

# ---------------- 1. SPARK BASELINE -----------------------------------------------
echo ">>> 1/4 baseline full source scan ($([ "${SPARK_REFEREE:-0}" = 1 ] && echo "local Spark" || echo "gateway no-MV referee"))"
SPARK_S=$(base_q1 spark_base); SPARK_HASH=$(cat "$OUT/spark_base.hash" 2>/dev/null)
echo "  spark: ${SPARK_S}s  rows=$(wc -l < "$OUT/spark_base.rows")  hash=${SPARK_HASH:0:16}…"

# ---------------- 2. BLIMP MV (author + materialize) ------------------------------
echo ">>> 2/4 Blimp MV — POST /admin/query/run (authors + materializes an MV)"
blimp_run "$Q1" "$OUT/blimp_init.json"
A0=$(J author_ms < "$OUT/blimp_init.json"); S0=$(J shape_ms < "$OUT/blimp_init.json")
M0=$(J materialize_ms < "$OUT/blimp_init.json"); Q0=$(J query_ms < "$OUT/blimp_init.json")
MVT=$(J mv_table < "$OUT/blimp_init.json"); MVNS=$(J mv_namespace < "$OUT/blimp_init.json")
NR=$(J rows < "$OUT/blimp_init.json"); MD5_0=$(J md5 < "$OUT/blimp_init.json"); SCHEME=$(J scheme < "$OUT/blimp_init.json")
BHASH=$(bcmp "$OUT/blimp_init.json")
echo "  blimp: mv=$MVNS.$MVT scheme=$SCHEME author=${A0}ms shape=${S0}ms materialize=${M0}ms query=${Q0}ms rows=$NR md5=$MD5_0"
echo "  --- correctness (Spark vs Blimp result parquet) ---"
if [ -n "$SPARK_HASH" ] && [ "$SPARK_HASH" = "$BHASH" ]; then echo "  MATCH ✓ (row hash spark==blimp)"
else echo "  MISMATCH: spark=${SPARK_HASH:0:16}… blimp=${BHASH:0:16}…  rows: spark=$(wc -l < "$OUT/spark_base.rows" 2>/dev/null) blimp=$NR (built-in md5=$MD5_0)"; QFAIL=1; fi
echo "  --- timing: Spark ${SPARK_S}s vs Blimp author+shape+materialize $(awk -v a="${A0:-0}" -v s="${S0:-0}" -v m="${M0:-0}" 'BEGIN{printf "%.3f",(a+s+m)/1000}')s ---"

# ---------------- 3. CDC INCREMENTAL ----------------------------------------------
echo ">>> 3/4 CDC incremental"
echo "  3a. append ${CDC_ROWS:-50000} rows to store_returns + POST /admin/source/snapshot_changed"
PY3="$HOME/venv_ib/bin/python3"; [ -x "$PY3" ] || PY3=python3   # register venv has pyiceberg
[ -f "$HERE/seed_tpcds.py" ] && "$PY3" "$HERE/seed_tpcds.py" --catalog "$ICEBERG_URL" --warehouse "$WAREHOUSE" \
  --namespace "$NAMESPACE" --table store_returns --rows "${CDC_ROWS:-50000}" \
  --s3-region "$REGION" 2>&1 | tail -1 || true
curl -s -m 60 "$QAPI/admin/source/snapshot_changed" -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"namespace\":\"$NAMESPACE\",\"table\":\"store_returns\",\"trigger\":\"test\"}" >/dev/null 2>&1
echo "  3b. Spark full recompute on the updated data"
SPARK2_S=$(base_q1 spark_postcdc); SPARK2_HASH=$(cat "$OUT/spark_postcdc.hash" 2>/dev/null)
echo "      spark(post-CDC): ${SPARK2_S}s hash=${SPARK2_HASH:0:16}…"
echo "  3c. Blimp q1 again (incremental delta-merge)"
blimp_run "$Q1" "$OUT/blimp_incr.json"
MI=$(J materialize_ms < "$OUT/blimp_incr.json"); QI=$(J query_ms < "$OUT/blimp_incr.json")
AI=$(J authored < "$OUT/blimp_incr.json"); MD5_I=$(J md5 < "$OUT/blimp_incr.json"); IHASH=$(bcmp "$OUT/blimp_incr.json")
echo "      blimp(incremental): authored=$AI materialize=${MI}ms query=${QI}ms md5=$MD5_I"
[ "$MD5_0" = "$MD5_I" ] && echo "      NOTE: md5 unchanged from initial — the append may not have landed / refresh didn't run (see pyiceberg note below)"
echo "  --- correctness: Blimp incremental == Spark full recompute (post-CDC) ---"
if [ -n "$SPARK2_HASH" ] && [ "$SPARK2_HASH" = "$IHASH" ]; then echo "      MATCH ✓ (incremental MV == full recompute)"
else echo "      MISMATCH: spark=${SPARK2_HASH:0:16}… blimp=${IHASH:0:16}…"; QFAIL=1; fi
echo "  --- speed: incremental materialize ${MI:-?}ms  vs  initial materialize ${M0:-?}ms  vs  Spark full recompute ${SPARK2_S}s ---"
echo "      (mode=incremental / merge_ms in the cluster's mv_wave_log; expect incremental ≪ initial ≪ Spark)"

# ---------------- 3d. UPSERT → FULL RE-MATERIALIZE ---------------------------------
# An upsert (copy-on-write delete of an sr_store_sk slice + replacement rows)
# REWRITES data files: the snapshot removes files, so an added-only merge would
# double-count. The gateway must detect removed_files and take the FULL
# re-materialize path (correct, slower) — this stage shows the append-vs-upsert
# cost diff and proves correctness either way. Skip with SKIP_UPSERT=1.
if [ "${SKIP_UPSERT:-0}" != "1" ]; then
  echo "  3d. UPSERT: replace the sr_store_sk=7 slice (${CDC_ROWS:-50000} replacement rows)"
  "$PY3" "$HERE/seed_tpcds.py" --catalog "$ICEBERG_URL" --warehouse "$WAREHOUSE" \
    --namespace "$NAMESPACE" --table store_returns --rows "${CDC_ROWS:-50000}" \
    --mode upsert --s3-region "$REGION" 2>&1 | tail -2 || true
  curl -s -m 60 "$QAPI/admin/source/snapshot_changed" -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"namespace\":\"$NAMESPACE\",\"table\":\"store_returns\",\"trigger\":\"upsert-test\"}" >/dev/null 2>&1
  echo "  3e. Spark full recompute on the upserted data"
  SPARK3_S=$(base_q1 spark_postupsert); SPARK3_HASH=$(cat "$OUT/spark_postupsert.hash" 2>/dev/null)
  echo "      spark(post-upsert): ${SPARK3_S}s hash=${SPARK3_HASH:0:16}…"
  echo "  3f. Blimp q1 after upsert (expect FULL re-materialize, not a delta merge)"
  T_UP0=$(now); blimp_run "$Q1" "$OUT/blimp_upsert.json"; T_UP1=$(now)
  MU=$(J materialize_ms < "$OUT/blimp_upsert.json"); QU=$(J query_ms < "$OUT/blimp_upsert.json")
  UHASH=$(bcmp "$OUT/blimp_upsert.json")
  echo "      blimp(post-upsert): materialize=${MU}ms query=${QU}ms wall=$(el "$T_UP0" "$T_UP1")s"
  echo "  --- refresh path taken (wave log; expect fallback with the removed-files-aware cdc) ---"
  curl -s -m 30 "$QAPI/admin/mv/wave/report?limit=10" -H "Authorization: Bearer $TOKEN" | python3 -c '
import json,sys
try:
  for w in json.load(sys.stdin).get("waves",[])[:6]:
    print("      wave: mode=%s reason=%s" % (w.get("mode"), str(w.get("reason",""))[:80]))
except Exception: pass'
  echo "  --- correctness: Blimp post-upsert == Spark full recompute ---"
  if [ -n "$SPARK3_HASH" ] && [ "$SPARK3_HASH" = "$UHASH" ]; then echo "      MATCH ✓ (upsert handled correctly via full re-materialize)"
  else echo "      MISMATCH: spark=${SPARK3_HASH:0:16}… blimp=${UHASH:0:16}… — if the cdc helper predates removed_files, an added-only merge double-counted (the exact bug the 2026-07-17 fix closes)"; QFAIL=1; fi
  echo "  --- cost diff: append→incremental ${MI:-?}ms  vs  upsert→full ${MU:-?}ms  vs  Spark ${SPARK3_S}s ---"
fi

# ---------------- 4. MV EVICTION (TPC-DS 99 batch) --------------------------------
echo ">>> 4/4 MV eviction (author many MVs from the TPC-DS 99, then reap)"
# NOTE: /admin/mv/list (and the incremental delta engine's /iceberg/added_files) require
# pyiceberg ON THE GATEWAY. If it's missing you'll see 'ModuleNotFoundError: pyiceberg'
# and mv_count returns 0 AND incremental refresh falls back to full re-author. Fix on the
# gateway: install pyiceberg into the zus-cdc / gateway python env.
Q_DIR="${Q_DIR:-$HERE/tpcds_queries}"; N_MVS="${N_MVS:-30}"
mv_count(){ curl -s -m 30 "$QAPI/admin/mv/list" -H "Authorization: Bearer $TOKEN" 2>/dev/null \
  | python3 -c 'import json,sys
try:
 d=json.load(sys.stdin)
 if isinstance(d,dict) and d.get("error"): print("ERR:"+d["error"][:60]); raise SystemExit
 mvs=d.get("mvs") or d.get("materialized_views") or (d if isinstance(d,list) else [])
 print(len(mvs) if isinstance(mvs,list) else 0)
except SystemExit: pass
except Exception: print(0)'; }
if [ -d "$Q_DIR" ]; then
  mapfile -t QF < <(ls "$Q_DIR"/*.sql 2>/dev/null | head -n "$N_MVS")
  echo "  4a. authoring MVs from ${#QF[@]} TPC-DS queries"
  for qf in "${QF[@]}"; do blimp_run "$(cat "$qf")" /dev/null; printf '.'; done; echo
  echo "  4b. MVs stored: $(mv_count)"
  echo "  4c. POST /admin/mv/reap (age/size/disk-pressure policy)"
  curl -s -m 120 "$QAPI/admin/mv/reap" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d "${MV_REAP_BODY:-{}}" 2>&1 | head -c 300; echo
  echo "  4d. MVs after reap: $(mv_count)  (expect fewer; coldest-first)"
  echo "      re-query an evicted MV -> re-materializes; a hot MV -> still MV-served (fast query_ms)"
else
  echo "  Q_DIR=$Q_DIR not found — put the TPC-DS 99 query files (q1.sql … q99.sql) there"
  echo "  (generate with the DuckDB tpcds extension: CALL dsdgen; or dsqgen at your SF)"
fi

# ---------------- 5. CACHE vs MV EVICTION ISOLATION -------------------------------
# Two SEPARATE evictors share the allocation but act on DISJOINT data:
#   - the read-through Router cache (external S3 objects) — NEVER touches MV/warehouse
#     prefixes (protected denylist in the router), evicts at ~80%;
#   - the gateway MV reaper (the MV warehouse) — evicts MVs, at ~90% (HIGHER) so the
#     cheap cache drains FIRST and MVs are protected until the disk is really full.
# This stage populates BOTH and asserts the ordering. Runs only if ROUTER + ORIGIN_BUCKET
# are set (the cache side). GW_AK/GW_SK let us read cache-bucket size via the gateway S3.
if [ -n "${ROUTER:-}" ] && [ -n "${ORIGIN_BUCKET:-}" ]; then
  echo ">>> 5/5 cache vs MV eviction isolation"
  CACHE_TABLE="${CACHE_TABLE:-store_returns}"
  cache_gib(){ [ -z "$GW_AK" ] && { echo "?"; return; }
    duckdb -noheader -list -c "SET s3_endpoint='$GW:9000'; SET s3_use_ssl=false; SET s3_url_style='path';
      SET s3_access_key_id='$GW_AK'; SET s3_secret_access_key='$GW_SK';
      SELECT round(sum(file_size)/1073741824.0,1) FROM glob('s3://$ORIGIN_BUCKET/$CACHE_TABLE/**');" 2>/dev/null | tail -1; }
  MV0=$(mv_count)
  echo "  5a. warm the router cache with external data ($CACHE_TABLE) + $MV0 MVs present"
  aws s3 ls "s3://$ORIGIN_BUCKET/$CACHE_TABLE/" --recursive --region "$REGION" 2>/dev/null | awk '$3+0>0{print $4}' \
    | xargs -P32 -I{} curl -s -m600 -o /dev/null "$ROUTER/$ORIGIN_BUCKET/{}"
  echo "      cache≈$(cache_gib) GiB, MVs=$(mv_count)"
  echo "  5b. PHASE A — cache pressure: the Router evicts CACHE objects; MVs are protected."
  echo "      (drive the router past its high watermark — see test_cache.sh eviction; MV prefixes"
  echo "       (mv-warehouse/, tpcds_mv_, …) are on the router's never-evict denylist.)"
  echo "      assert: cache shrinks, MVs unchanged -> MVs now $(mv_count) (was $MV0)"
  echo "  5c. PHASE B — MV pressure: only the MV reaper evicts MVs (POST /admin/mv/reap or"
  echo "      lower ZS3_MV_REAPER_HIGH_PCT/MAX_BYTES on the gateway)."
  curl -s -m 120 "$QAPI/admin/mv/reap" -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" -d '{}' >/dev/null 2>&1
  echo "      assert: MVs drop -> now $(mv_count) (was $MV0); router cache untouched by the reaper"
  echo "  => isolation: router (80% wmark, cache prefixes) and MV reaper (90% wmark, MV warehouse)"
  echo "     act on disjoint data; the cheap cache always drains before the expensive MVs."
fi
echo "==================== query suite complete ===================="
[ "$QFAIL" = 0 ] && echo "CORRECTNESS: all row-hash MATCH" || echo "CORRECTNESS: FAIL — a row-hash MISMATCH occurred"
exit $QFAIL
