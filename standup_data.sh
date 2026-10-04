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
# shellcheck source=scratch_dir.sh
. "$(dirname "${BASH_SOURCE[0]}")/scratch_dir.sh"
# duckdb's working database plus the parquet output both land locally before the
# upload. Budget ~2x the scale factor in GB (the .duckdb file and the parquet
# tree), with a 2 GB floor so SF1 does not demand a whole gigabyte for nothing.
SF_NEED_GB=$(( SF * 2 )); [ "$SF_NEED_GB" -lt 2 ] && SF_NEED_GB=2
SF_SCRATCH="$(scratch_pick "$SF_NEED_GB" "${BLIMP_SCRATCH:-}" "sf${SF}")" || exit 1
REGION="${REGION:-ap-south-1}"
CLUSTER_ID="${CLUSTER_ID:-local}"
OUT="${BLIMP_DATA_DIR:-$SF_SCRATCH/data}"
log(){ printf '\033[1m[data]\033[0m %s\n' "$*" >&2; }
# emit KEY VALUE — one shell-quoted line for the caller's eval.
emit(){ printf '%s=%q\n' "$1" "$2"; }
die(){ printf '\033[31m[data] FATAL: %s\033[0m\n' "$*" >&2; exit 1; }
# All 24 TPC-DS tables — must match register_tpcds_tables.py's TPCDS_TABLES.
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
  rm -f "$SF_SCRATCH/_sf${SF}.duckdb"
  scratch_report "$SF_SCRATCH"
  duckdb "$SF_SCRATCH/_sf${SF}.duckdb" -c "INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf=${SF}); $COPIES" >&2 \
    || die "SF${SF} generation failed (duckdb tpcds dsdgen)"
  rm -f "$SF_SCRATCH/_sf${SF}.duckdb"   # the parquet tree is the artefact; reclaim the working DB
  # Upgrade hygiene: earlier versions staged the working DB on the boot disk.
  rm -f "/tmp/_sf${SF}.duckdb"; touch "$OUT/.done"
  log "generated $(du -sh "$OUT" 2>/dev/null | awk '{print $1}') across 24 tables in $OUT"
fi

# --- 2. AWS (region-local S3 bucket) vs no writable S3 (MinIO on this box) ------
# MinIO IS the customer's S3 when there is no writable bucket: either the host is
# not on AWS at all, or its instance role cannot create/write one. The AWS leg
# below therefore FALLS BACK here instead of dying.
minio_source(){
  local_dkr(){ docker "$@" 2>/dev/null || sudo docker "$@"; }
  command -v docker >/dev/null || die "docker needed to stand up MinIO (blimp --setup installs it)"
  # Random root password, generated once and kept (mode 600) so a re-run reuses
  # the existing data dir with the same credentials. MINIO_ROOT_PASSWORD overrides.
  local pwf="$HOME/.blimp_minio_pw"
  MK="${MINIO_ROOT_USER:-blimpadmin}"
  if [ -n "${MINIO_ROOT_PASSWORD:-}" ]; then MS="$MINIO_ROOT_PASSWORD"
  elif [ -s "$pwf" ]; then MS="$(cat "$pwf")"
  else
    MS="$(LC_ALL=C tr -dc 'A-Za-z0-9' </dev/urandom 2>/dev/null | head -c 32)"
    [ "${#MS}" -ge 16 ] || die "could not generate a MinIO password from /dev/urandom"
    ( umask 077; printf '%s' "$MS" > "$pwf" )
  fi
  # Never publish on 0.0.0.0 by default: S3 binds this box's private address
  # (MINIO_BIND overrides, e.g. 127.0.0.1 or 0.0.0.0), the console loopback only.
  ADV="${ADVERTISE_HOST:-$(hostname -I 2>/dev/null | awk '{print $1}')}"
  local bind="${MINIO_BIND:-$(hostname -I 2>/dev/null | awk '{print $1}')}"
  [ -n "$bind" ] || bind=127.0.0.1
  local lep="http://$bind:9000"; [ "$bind" = 0.0.0.0 ] && lep="http://127.0.0.1:9000"
  log "standing up MinIO $bind:9000 + bucket blimp-sf${SF} on this box, loading SF${SF}"
  local_dkr rm -f blimp-minio >/dev/null 2>&1 || true
  local_dkr run -d --name blimp-minio --restart unless-stopped \
    -p "$bind:9000:9000" -p 127.0.0.1:9001:9001 \
    -e MINIO_ROOT_USER="$MK" -e MINIO_ROOT_PASSWORD="$MS" \
    -v "$HOME/.blimp_minio_data:/data" minio/minio server /data --console-address ":9001" >/dev/null \
    || die "minio start failed"
  for i in $(seq 1 25); do curl -s -m2 -o /dev/null "$lep/minio/health/live" && break; sleep 2; done
  curl -s -m3 -o /dev/null "$lep/minio/health/live" || die "MinIO did not come up on $bind:9000"
  BKT="blimp-sf${SF}"
  AWS_ACCESS_KEY_ID="$MK" AWS_SECRET_ACCESS_KEY="$MS" AWS_DEFAULT_REGION=us-east-1 \
    aws --endpoint-url "$lep" s3 mb "s3://$BKT" >/dev/null 2>&1 || true
  AWS_ACCESS_KEY_ID="$MK" AWS_SECRET_ACCESS_KEY="$MS" AWS_DEFAULT_REGION=us-east-1 \
    aws --endpoint-url "$lep" s3 sync "$OUT" "s3://$BKT/" --exclude ".done" >&2 \
    || die "MinIO load failed"
  emit ORIGIN_BUCKET "$BKT"
  emit WAREHOUSE "s3://$BKT/wh"
  emit S3_ENDPOINT "http://${ADV:-$bind}:9000"
  emit S3_KEY "$MK"
  emit S3_SECRET "$MS"
  emit REGION us-east-1
}

# --- 2a. FLEET CACHE LAYER (the default when --setup picked it) ---------------
# The dataset lands on the Blimp fleet's own S3 endpoint rather than a bucket this
# box owns. That is the "internal" path: the source is already inside the product,
# so the gateway reads it over the cache layer with no cross-account bucket, no
# IAM grant, and no MinIO container on the customer's box. Keys are the fleet keys
# from GET /admin/credentials; the caller passes them in.
fleet_source(){
  local ep="${FLEET_ENDPOINT:?FLEET_ENDPOINT required}" bkt="${FLEET_BUCKET:-blimp-sf${SF}}"
  command -v aws >/dev/null || die "awscli not installed — needed to upload to the fleet S3 endpoint"
  log "uploading SF${SF} to ${BLIMP_DATA_TARGET:-fleet} S3: $ep (bucket $bkt)"
  # The fleet endpoint is HTTPS with the cluster's own cert; --no-verify-ssl keeps
  # a private/self-signed CA from blocking the load. Path-style: the fleet address
  # is a single host, not per-bucket virtual hosts.
  local vfy=""; case "$ep" in https://*) vfy="--no-verify-ssl";; esac
  export AWS_ACCESS_KEY_ID="${FLEET_KEY:?FLEET_KEY required}"
  export AWS_SECRET_ACCESS_KEY="${FLEET_SECRET:?FLEET_SECRET required}"
  export AWS_DEFAULT_REGION="${REGION:-us-east-1}"
  # shellcheck disable=SC2086
  aws --endpoint-url "$ep" $vfy s3 mb "s3://$bkt" >/dev/null 2>&1 || true
  # shellcheck disable=SC2086
  aws --endpoint-url "$ep" $vfy s3 sync "$OUT" "s3://$bkt/" --exclude ".done" >&2 \
    || die "fleet upload failed (endpoint $ep, bucket $bkt)"
  emit ORIGIN_BUCKET "$bkt"
  emit WAREHOUSE "s3://$bkt/wh"
  emit S3_ENDPOINT "$ep"
  emit S3_KEY "$AWS_ACCESS_KEY_ID"
  emit S3_SECRET "$AWS_SECRET_ACCESS_KEY"
  emit REGION "${AWS_DEFAULT_REGION}"
}

# `gateway` is the same shape as `fleet`: an S3 endpoint plus keys that the
# caller already resolved — only the address differs (this node's own gateway
# rather than the fleet address). Routing it here is what makes "use the S3
# that is already running on this box" possible without pulling a MinIO
# container that would collide with it on :9000.
case "${BLIMP_DATA_TARGET:-}" in
  fleet|gateway)
    fleet_source
    log "SF${SF} data source ready"
    exit 0 ;;
esac

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
    emit ORIGIN_BUCKET "$BKT"
    emit WAREHOUSE "s3://$BKT/wh"
    emit S3_ENDPOINT "https://s3.${REGION}.amazonaws.com"
    emit REGION "$REGION"
  else
    log "this box's AWS identity has no writable S3 bucket — falling back to MinIO ON THIS BOX"
    minio_source
  fi
else
  minio_source
fi
log "SF${SF} data source ready"
