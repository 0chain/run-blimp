#!/usr/bin/env bash
# spark_verify.sh — independent cross-engine MV correctness proof.
#
# For each qNN: compute a sorted row-hash in LOCAL SPARK of
#   (A) the ORIGINAL query over the source (Iceberg REST catalog), and
#   (B) the stored REWRITE over the MV parquet (read directly from the
#       gateway S3 endpoint),
# and compare. A==B proves the (MV, rewrite) pair reproduces the original,
# using an engine COMPLETELY INDEPENDENT of the gateway's DuckDB verifier.
#
# This complements test_query.sh's SPARK_REFEREE mode (which proves
# Spark-original == Blimp-RESULT). Here both sides run in Spark, so a match
# is independent of Blimp's engine AND its result cache — a true second
# opinion on MV correctness.
#
# Heavy TPC-DS originals (q04/q14/q23) need a big driver — run on an
# r5.4xlarge+ (128GB) or set SPARK_DRIVER_MEM. Light queries run on an xlarge.
#
# Env:
#   GW            gateway S3 endpoint hosting MV parquet (http://<ip>:9000)
#   GW_KEY/GW_SECRET   MinIO creds for the MV bucket (minioserver env)
#   MV_BUCKET     default tpcds-mv
#   ICEBERG_URL   source Iceberg REST catalog (http://<client-priv>:8181)
#   WAREHOUSE     source warehouse (s3://.../wh_e2e)
#   NAMESPACE     source namespace (default tpcds)
#   TOKEN, QAPI   gateway admin token + API base (to pull each rewrite/mv_table
#                 from the run store when a spec file isn't provided)
#   QIDS          space-separated qNN list (default: q21 q32 q37)
set -uo pipefail
SPARK_VER="${SPARK_VER:-3.5.1}"; ICEBERG_VER="${ICEBERG_VER:-1.5.2}"
SPARK_HOME="${SPARK_HOME:-/opt/spark}"
MV_BUCKET="${MV_BUCKET:-tpcds-mv}"; NAMESPACE="${NAMESPACE:-tpcds}"
QIDS="${QIDS:-q21 q32 q37}"; OUT="${OUT:-/tmp/spark_verify}"; mkdir -p "$OUT"
TOKEN="${TOKEN:-}"; QAPI="${QAPI:-}"   # only needed for the /runs fallback + INCR mode
: "${GW:?}" "${GW_KEY:?}" "${GW_SECRET:?}" "${ICEBERG_URL:?}" "${WAREHOUSE:?}"
# Specs must come from SOMEWHERE: a local spec dir (preferred) or the run store.
if [ -z "${SPEC_DIR:-}" ] && [ -z "$QAPI" ]; then
  echo "FATAL: set SPEC_DIR=<dir with qNN.spec files> (preferred) or QAPI=<gateway /runs base>"; exit 2
fi

ensure_spark(){ command -v java >/dev/null 2>&1 || sudo apt-get install -y -q openjdk-17-jre-headless >/dev/null 2>&1
  [ -x "$SPARK_HOME/bin/spark-sql" ] && return 0
  echo "installing Spark $SPARK_VER…"
  curl -fsSL "https://archive.apache.org/dist/spark/spark-$SPARK_VER/spark-$SPARK_VER-bin-hadoop3.tgz" -o /tmp/spark.tgz
  sudo tar -xzf /tmp/spark.tgz -C /opt && sudo ln -sfn "/opt/spark-$SPARK_VER-bin-hadoop3" "$SPARK_HOME"; }
# Normalize float precision so logically-equal values hash the same across the
# original (fresh full-precision compute) and the MV (parquet round-trip): round
# any decimal with 5+ fractional digits to 4 places. Same transform both sides.
norm_floats(){ python3 -c "import sys,re
for l in sys.stdin:
    sys.stdout.write(re.sub(r'-?[0-9]+\\.[0-9]{5,}', lambda m: format(float(m.group()), '.4f'), l))"; }
hash_rows(){ norm_floats | sort | sha256sum | awk '{print $1}'; }
mem_mb(){ echo $(( $(free -m 2>/dev/null | awk 'NR==2{print $2}' || echo 8192) * 7 / 10 )); }

# compare_rows <fileA> <fileB> — numeric-TOLERANT row comparison (not a byte hash).
# Non-numeric fields must match exactly; numeric fields match within a relative
# epsilon (default 1e-4) so float summation-ORDER noise on a big SUM (the MV
# pre-aggregates in a different order than the original) is NOT a false mismatch.
# Prints "MATCH" or "MISMATCH <reason>".
compare_rows(){ python3 -c '
import sys
A=sorted(open(sys.argv[1]).read().splitlines()); B=sorted(open(sys.argv[2]).read().splitlines())
if len(A)!=len(B): print("MISMATCH rows %d!=%d"%(len(A),len(B))); sys.exit()
tol=float(sys.argv[3])
for a,b in zip(A,B):
    fa=a.split("\t"); fb=b.split("\t")
    if len(fa)!=len(fb): print("MISMATCH col-count"); sys.exit()
    for x,y in zip(fa,fb):
        if x==y: continue
        try:
            fx=float(x); fy=float(y)
            if abs(fx-fy)<=tol*max(1.0,abs(fx),abs(fy)): continue
            print("MISMATCH val %s!=%s"%(x[:20],y[:20])); sys.exit()
        except ValueError:
            print("MISMATCH str %s!=%s"%(x[:20],y[:20])); sys.exit()
print("MATCH")
' "$1" "$2" "${CMP_TOL:-1e-4}"; }

# spark_errored <outfile> — true if a spark side produced NO rows AND its stderr
# log shows a real failure (so we report ERROR, never a silent "0 rows mismatch").
spark_errored(){ [ ! -s "$1" ] && grep -qiE "Exception|ERROR |AnalysisException|Table or view not found|cannot resolve" "$1.err" 2>/dev/null; }

PKGS="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:$ICEBERG_VER,org.apache.iceberg:iceberg-aws-bundle:$ICEBERG_VER,org.apache.hadoop:hadoop-aws:3.3.4"

# spark_sql <sql> <outfile> [extra hadoop confs...] — run SQL, sorted rows to file.
# Retries TRANSIENT S3/network failures (connection reset, checksum, socket, 5xx)
# up to 3x — scanning the multi-billion-row source over S3 for 12 queries makes an
# occasional reset near-certain, and it must NOT read as a verification failure.
# A real SQL error (AnalysisException etc.) is NOT retried — it breaks immediately.
spark_sql(){ local sql="$1" out="$2"; shift 2; local try=0
  while :; do
    "$SPARK_HOME/bin/spark-sql" --driver-memory "${SPARK_DRIVER_MEM:-$(mem_mb)m}" \
      --packages "$PKGS" \
      --conf spark.sql.catalog.zus=org.apache.iceberg.spark.SparkCatalog \
      --conf spark.sql.catalog.zus.type=rest --conf spark.sql.catalog.zus.uri="$ICEBERG_URL" \
      --conf spark.sql.catalog.zus.warehouse="$WAREHOUSE" \
      --conf spark.sql.catalog.zus.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
      --conf spark.sql.defaultCatalog=zus \
      --conf spark.hadoop.fs.s3a.path.style.access=true \
      --conf spark.hadoop.fs.s3a.attempts.maximum=30 \
      --conf spark.hadoop.fs.s3a.retry.limit=15 \
      "$@" \
      -e "$sql" 2>"$out.err" | grep -vE '^::|^\[' | sort > "$out"
    if [ -s "$out" ] || [ $try -ge 2 ] || \
       ! grep -qiE "Connection reset|different checksum|SocketException|SocketTimeout|Timeout|timed out|Read timed|HTTP 5[0-9][0-9]|Service.*unavailable" "$out.err" 2>/dev/null; then
      break
    fi
    try=$((try+1)); echo "    [$(basename "$out")] transient S3 error → retry $try/3" >&2; sleep 8
  done; }

# MV bucket reads use static creds + the gateway endpoint (per-bucket override)
mv_confs(){ echo \
  --conf spark.hadoop.fs.s3a.bucket.$MV_BUCKET.endpoint=$GW \
  --conf spark.hadoop.fs.s3a.bucket.$MV_BUCKET.access.key=$GW_KEY \
  --conf spark.hadoop.fs.s3a.bucket.$MV_BUCKET.secret.key=$GW_SECRET \
  --conf spark.hadoop.fs.s3a.bucket.$MV_BUCKET.path.style.access=true; }

ensure_spark

# INCR mode — prove INCREMENTAL (CDC) correctness, not just static-MV correctness.
# Append a delta to the source, notify the gateway (which delta-merges each MV),
# wait for the merge, THEN run the A/B row-hash below. A MATCH then proves the
# delta-MERGED MV equals a FRESH full recompute over the UPDATED source, in an
# engine (Spark) fully independent of the gateway — i.e. the incremental refresh
# is provably correct, not just fast.
#   INCR=1 [CDC_TABLE=store_sales] [CDC_ROWS=5000] [INCR_WAIT=90] [REGION=...]
if [ "${INCR:-0}" = "1" ]; then
  CDC_TABLE="${CDC_TABLE:-store_sales}"; CDC_ROWS="${CDC_ROWS:-5000}"; REGION="${REGION:-us-east-1}"
  PY3="$HOME/venv_ib/bin/python3"; [ -x "$PY3" ] || PY3=python3
  echo ">> INCR: append $CDC_ROWS rows to $CDC_TABLE, then snapshot_changed"
  "$PY3" "$(cd "$(dirname "$0")" && pwd)/seed_tpcds.py" --catalog "$ICEBERG_URL" --warehouse "$WAREHOUSE" \
    --namespace "$NAMESPACE" --table "$CDC_TABLE" --rows "$CDC_ROWS" --s3-region "$REGION" 2>&1 | tail -1
  curl -s -m 30 "$GW/admin/source/snapshot_changed" -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"namespace\":\"$NAMESPACE\",\"table\":\"$CDC_TABLE\",\"trigger\":\"spark-verify-incr\"}" \
    -w " [snapshot_changed http %{http_code}]\n" -o /dev/null
  echo ">> INCR: waiting ${INCR_WAIT:-90}s for the gateway to delta-merge the MVs…"
  sleep "${INCR_WAIT:-90}"
fi

PASS=0; FAIL=0; SKIP=0
declare -A RESULT ROWS_A ROWS_B
SUMMARY="$OUT/SUMMARY.md"
for q in $QIDS; do
  echo "==================== $q ===================="
  ORIG="${Q_DIR:-$HOME/tpcds_queries}/$q.sql"
  [ -f "$ORIG" ] || { echo "  SKIP: no $ORIG"; RESULT[$q]=SKIP; SKIP=$((SKIP+1)); continue; }
  # MV table + stored rewrite. FOOLPROOF: prefer a local spec file
  # (SPEC_DIR/$q.spec — line 1 = mv_table, remaining lines = rewrite_sql), which
  # is generated straight from the gateway rag DB and needs NO /runs endpoint.
  # Fall back to the run store (Bearer-authed) only if no spec file is present.
  MVT=""; REW=""
  if [ -n "${SPEC_DIR:-}" ] && [ -f "$SPEC_DIR/$q.spec" ]; then
    MVT=$(head -1 "$SPEC_DIR/$q.spec"); REW=$(tail -n +2 "$SPEC_DIR/$q.spec")
  elif [ -n "${QAPI:-}" ]; then
    SPEC=$(curl -s -m 15 -H "Authorization: Bearer $TOKEN" "$QAPI/runs?all=1" | python3 -c '
import json,sys
q=sys.argv[1]
try: runs=json.load(sys.stdin).get("runs",[])
except Exception: runs=[]
for r in runs:
    if r.get("label")==q and r.get("mv_table") and r.get("rewrite_sql"):
        print(r["mv_table"]); print(r["rewrite_sql"]); break' "$q")
    MVT=$(echo "$SPEC" | head -1); REW=$(echo "$SPEC" | tail -n +2)
  fi
  MVT=$(echo "$MVT" | tr -d '[:space:]')
  if [ -z "$MVT" ] || [ -z "$(echo "$REW" | tr -d '[:space:]')" ]; then
    echo "  SKIP: no MV+rewrite for $q (need SPEC_DIR/$q.spec or a /runs entry labeled $q)"; RESULT[$q]=SKIP; SKIP=$((SKIP+1)); continue
  fi
  echo "  mv=$MVT"

  # A) original over the source catalog
  spark_sql "USE zus.$NAMESPACE; $(cat "$ORIG")" "$OUT/$q.orig.rows"
  NA=$(wc -l < "$OUT/$q.orig.rows")

  # B) rewrite over the MV parquet — register the MV as a temp view, then run the
  #    rewrite with iceberg/schema-qualified refs repointed to that view. USE
  #    zus.$NAMESPACE so a derived-grain rewrite that JOINS the MV back to source
  #    tables (customer, customer_address — q34/q46) resolves them; temp views are
  #    session-scoped so the MV view stays visible after USE.
  REW_V=$(echo "$REW" | sed -E "s/iceberg\.tpcds_mv\.$MVT/${MVT}_view/g; s/tpcds_mv\.$MVT/${MVT}_view/g; s/\\b$MVT\\b/${MVT}_view/g")
  spark_sql "USE zus.$NAMESPACE; CREATE OR REPLACE TEMP VIEW ${MVT}_view AS SELECT * FROM parquet.\`s3a://$MV_BUCKET/$MVT/data.parquet\`; $REW_V" "$OUT/$q.mv.rows" $(mv_confs)
  NB=$(wc -l < "$OUT/$q.mv.rows")

  ROWS_A[$q]=$NA; ROWS_B[$q]=$NB
  # Surface a Spark FAILURE explicitly — never let a swallowed error read as a
  # "0-row mismatch".
  if spark_errored "$OUT/$q.orig.rows"; then
    echo "  ERROR ✗  original query failed in Spark — $(grep -ioE '(AnalysisException|cannot resolve[^\"]*|Table or view not found[^\"]*)' "$OUT/$q.orig.rows.err" | head -1)"; RESULT[$q]=ERROR-orig; FAIL=$((FAIL+1)); continue
  fi
  if spark_errored "$OUT/$q.mv.rows"; then
    echo "  ERROR ✗  MV rewrite failed in Spark — $(grep -ioE '(AnalysisException|cannot resolve[^\"]*|Table or view not found[^\"]*)' "$OUT/$q.mv.rows.err" | head -1)"; RESULT[$q]=ERROR-mv; FAIL=$((FAIL+1)); continue
  fi
  # numeric-tolerant compare (handles float summation-order noise on SUMs)
  CMP=$(compare_rows "$OUT/$q.orig.rows" "$OUT/$q.mv.rows")
  if [ "$CMP" = MATCH ]; then echo "  MATCH ✓  rows=$NA  (Spark original == Spark over MV, ±${CMP_TOL:-1e-4})"; RESULT[$q]=MATCH; PASS=$((PASS+1))
  else echo "  MISMATCH ✗  orig_rows=$NA mv_rows=$NB — $CMP"; RESULT[$q]=MISMATCH; FAIL=$((FAIL+1)); fi
done

# ---- SUMMARY (stdout + $OUT/SUMMARY.md) ----------------------------------------
{
  echo "# Spark independent MV verification — $(printf '%s ' $QIDS)"
  echo ""
  printf '| %-6s | %-8s | %10s | %10s |\n' query result orig_rows mv_rows
  printf '|%s|%s|%s|%s|\n' "--------" "----------" "------------" "------------"
  for q in $QIDS; do
    printf '| %-6s | %-8s | %10s | %10s |\n' "$q" "${RESULT[$q]:-?}" "${ROWS_A[$q]:-–}" "${ROWS_B[$q]:-–}"
  done
  echo ""
  echo "**$PASS match, $FAIL mismatch, $SKIP skip** — MATCH = Spark(original over source) row-hash == Spark(stored rewrite over MV parquet), an engine fully independent of the gateway."
} | tee "$SUMMARY"
echo ""
echo "==================== SUMMARY: $PASS match, $FAIL mismatch, $SKIP skip ===================="
echo "summary written to $SUMMARY"
[ "$FAIL" = 0 ] && [ "$PASS" -gt 0 ]
