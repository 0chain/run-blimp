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
TABLES="call_center catalog_page catalog_returns catalog_sales customer \
customer_address customer_demographics date_dim household_demographics income_band \
inventory item promotion reason ship_mode store store_returns store_sales time_dim \
warehouse web_page web_returns web_site web_site"

# --- duckdb (the generator). Installed by blimp --setup; self-install as a fallback.
if ! command -v duckdb >/dev/null; then
  log "installing duckdb CLI (needed to generate SF${SF})"
  case "$(uname -m)" in aarch64|arm64) D=aarch64;; *) D=amd64;; esac
  curl -fsSL "https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-${D}.zip" -o /tmp/duckdb.zip \
    && unzip -oq /tmp/duckdb.zip -d /tmp && sudo mv /tmp/duckdb /usr/local/bin/ 2>/dev/null || mv /tmp/duckdb "$HOME/.local/bin/" 2>/dev/null
  command -v duckdb >/dev/null || die "could not install duckdb"
fi

# --- 1. generate SF<N> once (idempotent) into <table>/data.parquet -------------
if [ ! -f "$OUT/.done" ]; then
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

# --- 2. AWS (region-local S3 bucket) vs non-AWS (MinIO on this box) -------------
ON_AWS=0; curl -s -m 2 -o /dev/null http://169.254.169.254/latest/meta-data/ 2>/dev/null && ON_AWS=1

if [ "$ON_AWS" = 1 ] && command -v aws >/dev/null && aws sts get-caller-identity >/dev/null 2>&1; then
  # the bucket MUST be co-located with THIS box's region (a cross-region source
  # bucket 301s the gateway's single S3 endpoint). Derive it from IMDS placement.
  TOK=$(curl -s -m2 -X PUT http://169.254.169.254/latest/api/token -H "X-aws-ec2-metadata-token-ttl-seconds: 60")
  AZ=$(curl -s -m2 -H "X-aws-ec2-metadata-token: $TOK" http://169.254.169.254/latest/meta-data/placement/availability-zone 2>/dev/null)
  [ -n "$AZ" ] && REGION="${AZ%[a-z]}"
  BKT="blimp-sf${SF}-${REGION}-${CLUSTER_ID}"
  log "AWS: creating s3://$BKT in $REGION (this box's region) + uploading SF${SF}"
  if [ "$REGION" = us-east-1 ]; then
    aws s3api create-bucket --bucket "$BKT" --region "$REGION" >/dev/null 2>&1 || true
  else
    aws s3api create-bucket --bucket "$BKT" --region "$REGION" \
      --create-bucket-configuration "LocationConstraint=$REGION" >/dev/null 2>&1 || true
  fi
  aws s3api head-bucket --bucket "$BKT" >/dev/null 2>&1 || die "could not create/access s3://$BKT"
  aws s3 sync "$OUT" "s3://$BKT/" --exclude ".done" --region "$REGION" >&2 || die "S3 upload failed"
  echo "ORIGIN_BUCKET=$BKT"
  echo "WAREHOUSE=s3://$BKT/wh"
  echo "S3_ENDPOINT=https://s3.${REGION}.amazonaws.com"
  echo "REGION=$REGION"
else
  # non-AWS host: MinIO IS the customer's S3. Stand it up here + load the parquet.
  local_dkr(){ docker "$@" 2>/dev/null || sudo docker "$@"; }
  command -v docker >/dev/null || die "docker needed to stand up MinIO (blimp --setup installs it)"
  MK="${MINIO_ROOT_USER:-blimpadmin}"; MS="${MINIO_ROOT_PASSWORD:-blimp$(printf '%s' "$CLUSTER_ID" | tail -c 8)Pw!}"
  log "non-AWS: standing up MinIO :9000 + bucket blimp-sf${SF}, loading SF${SF}"
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
fi
log "SF${SF} data source ready"
