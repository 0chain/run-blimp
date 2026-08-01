#!/usr/bin/env bash
# standup_data.sh — scenario (b): the customer has NO data yet. Generate a real
# TPC-DS SF<N> dataset (default SF1 ≈ 1 GB, all 24 tables) locally with duckdb,
# then place it in object storage the Blimp gateway can read:
#
#   • AWS client  → an S3 bucket in the CLIENT'S OWN region (co-located with this
#                   box, not a hardcoded region) + upload the parquet.
#   • non-AWS     → a MinIO server stood up ON THIS box + a bucket loaded with it,
#                   so a customer with no cloud storage still gets an S3 source.
#
# Layout matches register_tpcds_tables.py: s3://<bucket>/<table>/data.parquet.
# Prints KEY=VALUE lines on stdout for `blimp --setup` to eval:
#   ORIGIN_BUCKET, WAREHOUSE, S3_ENDPOINT, REGION, and (MinIO) S3_KEY/S3_SECRET.
# Everything human-facing goes to stderr so stdout is clean to eval.
set -uo pipefail
SF="${BLIMP_SF:-1}"
REGION="${REGION:-ap-south-1}"
CLUSTER_ID="${CLUSTER_ID:-local}"
OUT="${BLIMP_DATA_DIR:-$HOME/.blimp_sf${SF}}"
log(){ printf '\033[1m[data]\033[0m %s\n' "$*" >&2; }
die(){ printf '\033[31m[data] FATAL: %s\033[0m\n' "$*" >&2; exit 1; }
# All 24 TPC-DS tables — must match register_tpcds_tables.py's TPCDS_TABLES
# exactly. `web_site` used to appear TWICE here in place of `web_sales`, so the
# web-channel FACT table was never generated: 23 dirs, "registered 23/24", and
# every web_sales query (and its MVs / delta-merges) had no source table.
TABLES="call_center catalog_page catalog_returns catalog_sales customer \
customer_address customer_demographics date_dim household_demographics income_band \
inventory item promotion reason ship_mode store store_returns store_sales time_dim \
warehouse web_page web_returns web_sales web_site"

# --- duckdb (the generator). Installed by blimp --setup; self-install as a fallback.
if ! command -v duckdb >/dev/null; then
  log "installing duckdb CLI (needed to generate SF${SF})"
  case "$(uname -m)" in aarch64|arm64) D=aarch64;; *) D=amd64;; esac
  curl -fsSL "https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-${D}.zip" -o /tmp/duckdb.zip \
    && unzip -oq /tmp/duckdb.zip -d /tmp && sudo mv /tmp/duckdb /usr/local/bin/ 2>/dev/null || mv /tmp/duckdb "$HOME/.local/bin/" 2>/dev/null
  command -v duckdb >/dev/null || die "could not install duckdb"
fi

# --- 1. generate SF<N> once (idempotent) into <table>/data.parquet -------------
# Idempotence is keyed on the FILES, not just the .done marker: a box generated
# by an older TABLES list carries a stale marker and would silently keep its
# missing tables forever. Any absent parquet re-runs the generation (COPY TO
# overwrites, so this is safe and deletes nothing).
NEED_GEN=0
[ -f "$OUT/.done" ] || NEED_GEN=1
for t in $TABLES; do [ -s "$OUT/$t/data.parquet" ] || { NEED_GEN=1; log "missing $t/data.parquet — regenerating SF${SF}"; break; }; done
if [ "$NEED_GEN" = 1 ]; then
  log "generating TPC-DS SF${SF} (~$([ "$SF" = 1 ] && echo '1 GB' || echo "${SF}x SF1") — all 24 tables) — one time"
  mkdir -p "$OUT"; COPIES=""
  for t in $TABLES; do mkdir -p "$OUT/$t"; COPIES="$COPIES COPY $t TO '$OUT/$t/data.parquet' (FORMAT PARQUET);"; done
  # temp on-disk db so a small box (4 GiB) doesn't OOM building SF1 in memory
  rm -f /tmp/_sf${SF}.duckdb
  duckdb "/tmp/_sf${SF}.duckdb" -c "INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf=${SF}); $COPIES" >&2 \
    || die "SF${SF} generation failed (duckdb tpcds dsdgen)"
  rm -f "/tmp/_sf${SF}.duckdb"; touch "$OUT/.done"
  log "generated $(du -sh "$OUT" 2>/dev/null | awk '{print $1}') across 24 tables in $OUT"
fi

# --- 2. AWS (region-local S3 bucket) vs no writable S3 (MinIO on this box) ------
# MinIO IS the customer's S3 when there is no writable bucket: either the host is
# not on AWS at all, or it IS on AWS but its instance role cannot create/write one
# (a locked-down role with read-only S3 is the common case — the bench role grants
# only ListBucket/GetObject on the showcase bucket). The AWS leg below therefore
# FALLS BACK here instead of dying: standing the source up on this box always
# works, and it is what "blank data bucket = we generate a source for you"
# promises. Before this, a read-only role aborted `blimp --setup` outright with
# "could not create/access s3://…" and the customer had no source at all.
minio_source(){
  local_dkr(){ docker "$@" 2>/dev/null || sudo docker "$@"; }
  command -v docker >/dev/null || die "docker needed to stand up MinIO (blimp --setup installs it)"
  MK="${MINIO_ROOT_USER:-blimpadmin}"; MS="${MINIO_ROOT_PASSWORD:-blimp$(printf '%s' "$CLUSTER_ID" | tail -c 8)Pw!}"
  log "standing up MinIO :9000 + bucket blimp-sf${SF} on this box, loading SF${SF}"
  local_dkr rm -f blimp-minio >/dev/null 2>&1 || true
  local_dkr run -d --name blimp-minio --restart unless-stopped -p 9000:9000 -p 9001:9001 \
    -e MINIO_ROOT_USER="$MK" -e MINIO_ROOT_PASSWORD="$MS" \
    -v "$HOME/.blimp_minio_data:/data" minio/minio server /data --console-address ":9001" >/dev/null \
    || die "minio start failed"
  for i in $(seq 1 25); do curl -s -m2 -o /dev/null http://localhost:9000/minio/health/live && break; sleep 2; done
  curl -s -m3 -o /dev/null http://localhost:9000/minio/health/live || die "MinIO did not come up on :9000"
  ADV="${ADVERTISE_HOST:-$(hostname -I 2>/dev/null | awk '{print $1}')}"
  BKT="blimp-sf${SF}"
  AWS_ACCESS_KEY_ID="$MK" AWS_SECRET_ACCESS_KEY="$MS" AWS_DEFAULT_REGION=us-east-1 \
    aws --endpoint-url "http://localhost:9000" s3 mb "s3://$BKT" >/dev/null 2>&1 || true
  AWS_ACCESS_KEY_ID="$MK" AWS_SECRET_ACCESS_KEY="$MS" AWS_DEFAULT_REGION=us-east-1 \
    aws --endpoint-url "http://localhost:9000" s3 sync "$OUT" "s3://$BKT/" --exclude ".done" >&2 \
    || die "MinIO load failed"
  echo "ORIGIN_BUCKET=$BKT"
  echo "WAREHOUSE=s3://$BKT/wh"
  echo "S3_ENDPOINT=http://${ADV}:9000"
  echo "S3_KEY=$MK"
  echo "S3_SECRET=$MS"
  echo "REGION=us-east-1"
}

ON_AWS=0; curl -s -m 2 -o /dev/null http://169.254.169.254/latest/meta-data/ 2>/dev/null && ON_AWS=1

if [ "$ON_AWS" = 1 ] && command -v aws >/dev/null && aws sts get-caller-identity >/dev/null 2>&1; then
  # the bucket MUST be co-located with THIS box's region (a cross-region source
  # bucket 301s the gateway's single S3 endpoint). Derive it from IMDS placement.
  TOK=$(curl -s -m2 -X PUT http://169.254.169.254/latest/api/token -H "X-aws-ec2-metadata-token-ttl-seconds: 60")
  AZ=$(curl -s -m2 -H "X-aws-ec2-metadata-token: $TOK" http://169.254.169.254/latest/meta-data/placement/availability-zone 2>/dev/null)
  [ -n "$AZ" ] && REGION="${AZ%[a-z]}"
  BKT="blimp-sf${SF}-${REGION}-${CLUSTER_ID}"
  log "AWS: creating s3://$BKT in $REGION (this box's region) + uploading SF${SF}"
  # Keep the create error — swallowing it left only "could not create/access",
  # which hides the actual cause (AccessDenied vs BucketAlreadyOwnedByYou vs a
  # region mismatch) and makes the fallback below look unexplained.
  if [ "$REGION" = us-east-1 ]; then
    CB_ERR=$(aws s3api create-bucket --bucket "$BKT" --region "$REGION" 2>&1 >/dev/null) || true
  else
    CB_ERR=$(aws s3api create-bucket --bucket "$BKT" --region "$REGION" \
      --create-bucket-configuration "LocationConstraint=$REGION" 2>&1 >/dev/null) || true
  fi
  S3_OK=1
  if ! aws s3api head-bucket --bucket "$BKT" >/dev/null 2>&1; then
    S3_OK=0
    log "cannot create/access s3://$BKT — ${CB_ERR:-head-bucket denied}"
  elif ! aws s3 sync "$OUT" "s3://$BKT/" --exclude ".done" --region "$REGION" >&2; then
    S3_OK=0
    log "upload to s3://$BKT failed"
  fi
  if [ "$S3_OK" = 1 ]; then
    echo "ORIGIN_BUCKET=$BKT"
    echo "WAREHOUSE=s3://$BKT/wh"
    echo "S3_ENDPOINT=https://s3.${REGION}.amazonaws.com"
    echo "REGION=$REGION"
  else
    log "this box's AWS identity has no writable S3 bucket — falling back to MinIO ON THIS BOX"
    minio_source
  fi
else
  minio_source
fi
log "SF${SF} data source ready"
