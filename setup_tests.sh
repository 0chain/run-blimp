#!/usr/bin/env bash
# setup_tests.sh — one-click benchmark client box for a Blimp cloud-native cluster.
#
# Spins up an EC2 instance IN THE CLUSTER'S VPC (so it reaches the gateway/router on
# private IPs), installs warp + fio + mlperf, brings up the Iceberg REST catalog
# (this kit's docker-compose) over your external S3 (where TPC-DS is registered),
# and prints the values to paste into the cluster UI + the bench targets.
#
# Prereqs (run from your Mac/laptop): awscli configured for the cluster's account,
# and the SSH .pem for KEY_NAME in ~/.ssh/.
#
# Usage:
#   CLUSTER_ID=1783897137567 REGION=ap-south-1 \
#   WAREHOUSE=s3://zus-prodtest-customer-iceberg-20260607/wh \
#   KEY_NAME=zus-prodtest-iceberg-aps1-key \
#   ./setup_tests.sh
#
# Optional: INSTANCE_TYPE (default m5.large), VOL_GB (100), SSH_CIDR (your IP/32 auto),
#           NAMESPACE (tpcds_sf1), ICEBERG_PORT (8181).

set -euo pipefail
: "${CLUSTER_ID:?set CLUSTER_ID}" "${REGION:?set REGION}" "${WAREHOUSE:?set WAREHOUSE (s3://bucket/prefix)}" "${KEY_NAME:?set KEY_NAME}"
TYPE="${INSTANCE_TYPE:-m5.large}"; VOL_GB="${VOL_GB:-100}"; ICEBERG_PORT="${ICEBERG_PORT:-8181}"
NAMESPACE="${NAMESPACE:-tpcds_sf1}"
PEM="$HOME/.ssh/${KEY_NAME}.pem"; [ -f "$PEM" ] || { echo "missing $PEM"; exit 1; }
A(){ aws --region "$REGION" "$@"; }

# --- fool-proofing preflight: catch the common customer mistakes with a CLEAR
# message BEFORE launching any infrastructure (otherwise they surface deep in
# registration as a cryptic S3 HTTP 301 or an IAM AccessDenied). ---------------
command -v aws >/dev/null || { echo "FATAL: awscli not found on PATH — install it and configure creds for the cluster's account."; exit 1; }
A sts get-caller-identity >/dev/null 2>&1 || { echo "FATAL: AWS credentials not working for region $REGION. Run 'aws configure' (or set AWS_PROFILE) for the account that owns cluster $CLUSTER_ID."; exit 1; }
case "$WAREHOUSE" in s3://*) ;; *) echo "FATAL: WAREHOUSE must be an s3:// URL (got '$WAREHOUSE'), e.g. s3://<your-bucket>/wh"; exit 1;; esac
WH_BKT=$(echo "$WAREHOUSE" | sed -E 's#^s3://([^/]+).*#\1#')
WH_REGION=$(A s3api get-bucket-location --bucket "$WH_BKT" --query 'LocationConstraint' --output text 2>/dev/null) || {
  echo "FATAL: warehouse bucket '$WH_BKT' does not exist or is not accessible in this account."
  echo "       Create it in $REGION:   aws s3 mb s3://$WH_BKT --region $REGION"; exit 1; }
[ "$WH_REGION" = None ] && WH_REGION=us-east-1
if [ "$WH_REGION" != "$REGION" ]; then
  echo "FATAL: the Iceberg warehouse bucket '$WH_BKT' is in $WH_REGION, but your cluster and data"
  echo "       are in $REGION. The warehouse MUST be in the SAME region as the cluster + data"
  echo "       (client box, cluster, S3 data, and warehouse all in one region)."
  echo "       Fix: set WAREHOUSE to an s3:// path in a $REGION bucket, e.g. s3://<a-${REGION}-bucket>/wh"
  exit 1
fi
echo "== preflight OK: warehouse $WH_BKT is in $REGION (same region as cluster + data) =="

echo "== resolving cluster $CLUSTER_ID network =="
# cluster resources are tagged; grab VPC/subnet from a running cluster instance
IID0=$(A ec2 describe-instances --filters "Name=tag:Name,Values=cluster-${CLUSTER_ID}-zs3server" "Name=instance-state-name,Values=running" --query 'Reservations[0].Instances[0].InstanceId' --output text)
[ "$IID0" != None ] || { echo "no running zs3server for cluster $CLUSTER_ID"; exit 1; }
read -r VPC SUBNET GWPRIV <<<"$(A ec2 describe-instances --instance-ids "$IID0" --query 'Reservations[0].Instances[0].[VpcId,SubnetId,PrivateIpAddress]' --output text)"
echo "  vpc=$VPC subnet=$SUBNET gateway_priv=$GWPRIV"

echo "== security group =="
SG=$(A ec2 create-security-group --group-name "zus-bench-${CLUSTER_ID}" --description "bench box for $CLUSTER_ID" --vpc-id "$VPC" --query GroupId --output text 2>/dev/null \
     || A ec2 describe-security-groups --filters "Name=group-name,Values=zus-bench-${CLUSTER_ID}" "Name=vpc-id,Values=$VPC" --query 'SecurityGroups[0].GroupId' --output text)
MYIP="${SSH_CIDR:-$(curl -s https://checkip.amazonaws.com)/32}"
A ec2 authorize-security-group-ingress --group-id "$SG" --protocol tcp --port 22 --cidr "$MYIP" 2>/dev/null || true
VPC_CIDR=$(A ec2 describe-vpcs --vpc-ids "$VPC" --query 'Vpcs[0].CidrBlock' --output text)
A ec2 authorize-security-group-ingress --group-id "$SG" --protocol tcp --port "$ICEBERG_PORT" --cidr "$VPC_CIDR" 2>/dev/null || true
echo "  sg=$SG (ssh from $MYIP, iceberg :$ICEBERG_PORT from $VPC_CIDR)"

echo "== launch $TYPE =="
AMI=$(A ec2 describe-images --owners 099720109477 --filters 'Name=name,Values=ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*' 'Name=state,Values=available' --query 'reverse(sort_by(Images,&CreationDate))[0].ImageId' --output text)
UD=$(mktemp); cat > "$UD" <<'CLOUD'
#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
# EXACT versions to match zs3-init (so the box simulates the in-cluster run_bench):
#   fio     = apt (3.28 on Ubuntu 22.04)   warp = v1.1.4   dlio_benchmark = 2.0   torch = CPU wheel
apt-get install -y docker.io docker-compose-v2 fio nfs-common git unzip \
  python3-venv python3-pip python3-dev build-essential libopenmpi-dev openmpi-bin
systemctl enable --now docker
# warp v1.1.4 — exact match to zs3-init installWarpBinary (NOT :latest, whose API differs)
curl -fsSL https://github.com/minio/warp/releases/download/v1.1.4/warp_Linux_x86_64.tar.gz -o /tmp/warp.tgz \
  && tar -xzf /tmp/warp.tgz -C /usr/local/bin warp
# mlperf = torch (CPU-only) + dlio-benchmark==2.0 in the SAME venv path zs3 uses
mkdir -p /opt/dlio && python3 -m venv /opt/dlio/venv_mlperf
/opt/dlio/venv_mlperf/bin/pip install -q --upgrade pip
/opt/dlio/venv_mlperf/bin/pip install -q --index-url https://download.pytorch.org/whl/cpu torch
/opt/dlio/venv_mlperf/bin/pip install -q dlio-benchmark==2.0 mpi4py s3torchconnector || true
# mountpoint-s3 (mount-s3) — the mlperf reads via FUSE-over-S3 (mlperf-mp-s3 path),
# same as zs3-init installMountpointS3. awscli for the bench bucket create.
apt-get install -y --no-install-recommends fuse awscli || true
curl -fsSL -o /tmp/mount-s3.deb https://s3.amazonaws.com/mountpoint-s3-release/latest/x86_64/mount-s3.deb \
  && apt-get install -y /tmp/mount-s3.deb || true
# tfrecord2idx stub — dlio's TFRecord reader probes for nvidia-dali's tfrecord2idx
# (CUDA-only); zs3 ships a no-op stub for the CPU env. Match it or mlperf gen fails.
printf '#!/bin/sh\n# nvidia-dali tfrecord2idx workaround stub (CPU-only env)\nexit 0\n' > /usr/local/bin/tfrecord2idx && chmod 755 /usr/local/bin/tfrecord2idx
echo done > /var/log/benchbox-setup.done
CLOUD
IID=$(A ec2 run-instances --image-id "$AMI" --instance-type "$TYPE" --key-name "$KEY_NAME" \
  --subnet-id "$SUBNET" --security-group-ids "$SG" --associate-public-ip-address \
  --user-data "file://$UD" --block-device-mappings "DeviceName=/dev/sda1,Ebs={VolumeSize=${VOL_GB},VolumeType=gp3}" \
  --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=zus-bench-${CLUSTER_ID}}]" \
  --query 'Instances[0].InstanceId' --output text)
A ec2 wait instance-running --instance-ids "$IID"
read -r PUB PRIV <<<"$(A ec2 describe-instances --instance-ids "$IID" --query 'Reservations[0].Instances[0].[PublicIpAddress,PrivateIpAddress]' --output text)"
echo "  instance=$IID pub=$PUB priv=$PRIV"

# --- S3 gateway VPC endpoint + IAM (all idempotent; discovered 2026-07-17: these
# were manual steps — without them S3 reads egress via NAT and the register
# script has no credentials) -------------------------------------------------
echo "== S3 gateway VPC endpoint + IAM wiring =="
VPCE=$(A ec2 describe-vpc-endpoints --filters "Name=vpc-id,Values=$VPC" "Name=service-name,Values=com.amazonaws.${REGION}.s3" --query 'VpcEndpoints[0].VpcEndpointId' --output text)
if [ "$VPCE" = "None" ] || [ -z "$VPCE" ]; then
  RTBS=$(A ec2 describe-route-tables --filters "Name=vpc-id,Values=$VPC" --query 'RouteTables[].RouteTableId' --output text)
  # shellcheck disable=SC2086 — route-table ids are separate args
  VPCE=$(A ec2 create-vpc-endpoint --vpc-id "$VPC" --service-name "com.amazonaws.${REGION}.s3" \
    --vpc-endpoint-type Gateway --route-table-ids $RTBS --query 'VpcEndpoint.VpcEndpointId' --output text)
fi
echo "  s3 endpoint: $VPCE"
# the cluster's own SSM profile carries sf1000/warehouse read — reuse it on the box
PROFILE="ec2-ssm-profile-${CLUSTER_ID}"
A ec2 associate-iam-instance-profile --instance-id "$IID" --iam-instance-profile "Name=$PROFILE" >/dev/null 2>&1 \
  && echo "  attached instance profile $PROFILE" \
  || echo "  WARN: could not attach $PROFILE (already associated, or missing) — registration needs S3 read+write creds"
# scoped WRITE on the warehouse prefix (Iceberg metadata + CDC appends land there)
ROLE=$(A iam get-instance-profile --instance-profile-name "$PROFILE" --query 'InstanceProfile.Roles[0].RoleName' --output text 2>/dev/null)
if [ -n "$ROLE" ] && [ "$ROLE" != "None" ]; then
  WH_BKT=$(echo "$WAREHOUSE" | sed -E 's#^s3://([^/]+).*#\1#'); WH_PFX=$(echo "$WAREHOUSE" | sed -E 's#^s3://[^/]+/?##')
  A iam put-role-policy --role-name "$ROLE" --policy-name "wh-write-${CLUSTER_ID}" --policy-document "{
    \"Version\":\"2012-10-17\",\"Statement\":[
      {\"Effect\":\"Allow\",\"Action\":[\"s3:PutObject\",\"s3:DeleteObject\",\"s3:AbortMultipartUpload\"],
       \"Resource\":\"arn:aws:s3:::${WH_BKT}/${WH_PFX}${WH_PFX:+/}*\"},
      {\"Effect\":\"Allow\",\"Action\":[\"s3:ListBucket\",\"s3:GetObject\"],
       \"Resource\":[\"arn:aws:s3:::${WH_BKT}\",\"arn:aws:s3:::${WH_BKT}/*\"]}]}" \
    && echo "  warehouse write policy wh-write-${CLUSTER_ID} on $ROLE"
fi

echo "== wait for cloud-init (docker/warp/fio/mlperf) =="
for i in $(seq 1 40); do
  ssh -o StrictHostKeyChecking=no -o ConnectTimeout=8 -i "$PEM" ubuntu@"$PUB" 'test -f /var/log/benchbox-setup.done' 2>/dev/null && break; sleep 15
done

echo "== bring up Iceberg REST catalog over $WAREHOUSE =="
WH_BUCKET=$(echo "$WAREHOUSE" | sed -E 's#^s3://([^/]+).*#\1#')
# EXPLICIT catalog S3 credentials (required for CDC, see CDC_INCREMENTAL.md): with
# only the instance role, the catalog's per-commit credential resolution holds the
# SQLite commit transaction open and Iceberg appends fail with
# "SQLITE_BUSY — database is locked", so incremental refresh never gets a delta.
# A scoped IAM-user key fixes it. Same-account reads still work either way.
echo "   provisioning scoped catalog IAM user + key on $WH_BUCKET"
CAT_USER="blimp-catalog-${CLUSTER_ID}"
aws iam get-user --user-name "$CAT_USER" >/dev/null 2>&1 || aws iam create-user --user-name "$CAT_USER" >/dev/null
aws iam put-user-policy --user-name "$CAT_USER" --policy-name warehouse-rw \
  --policy-document "$(printf '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:ListBucket","s3:GetBucketLocation"],"Resource":["arn:aws:s3:::%s","arn:aws:s3:::%s/*"]}]}' "$WH_BUCKET" "$WH_BUCKET")"
for k in $(aws iam list-access-keys --user-name "$CAT_USER" --query 'AccessKeyMetadata[].AccessKeyId' --output text 2>/dev/null); do aws iam delete-access-key --user-name "$CAT_USER" --access-key-id "$k" 2>/dev/null; done
read CAT_AK CAT_SK < <(aws iam create-access-key --user-name "$CAT_USER" --query 'AccessKey.[AccessKeyId,SecretAccessKey]' --output text)
sleep 8  # IAM propagation
scp -o StrictHostKeyChecking=no -i "$PEM" -r "$(dirname "$0")"/{docker-compose.yml,.env.example,seed_tpcds.py} ubuntu@"$PUB":/home/ubuntu/ 2>/dev/null || true
ssh -o StrictHostKeyChecking=no -i "$PEM" ubuntu@"$PUB" bash -s <<REMOTE
  set -e; cd /home/ubuntu; mkdir -p catalog
  cat > .env <<E
WAREHOUSE=$WAREHOUSE
AWS_REGION=$REGION
AWS_ACCESS_KEY_ID=$CAT_AK
AWS_SECRET_ACCESS_KEY=$CAT_SK
E
  sudo docker compose up -d
  sleep 4; curl -s localhost:${ICEBERG_PORT}/v1/config >/dev/null && echo "  <- iceberg catalog up"
  # Confirm WAL is active — concurrent CDC appends vs the gateway drift-poller's
  # catalog reads deadlock on SQLite's single write lock without it (SQLITE_BUSY /
  # CommitStateUnknownException). journal_mode=WAL + busy_timeout come from the
  # CATALOG_URI in docker-compose.yml; this just proves they took effect.
  python3 -c 'import sqlite3;print("  <- catalog journal_mode =",sqlite3.connect("/home/ubuntu/catalog/cat.db").execute("PRAGMA journal_mode").fetchone()[0])' 2>/dev/null || echo "  <- (WAL applies on first catalog write)"
REMOTE

cat <<OUT

=================== BENCH BOX READY ===================
instance         : $IID   ssh -i $PEM ubuntu@$PUB
in VPC           : $VPC / $SUBNET   (private $PRIV)
tools            : warp (docker minio/warp), fio, mlperf-storage
iceberg REST     : http://$PRIV:${ICEBERG_PORT}   (warehouse $WAREHOUSE, namespace $NAMESPACE)

--- Cluster UI: Query Optimizer -> Production (BYO iceberg source) ---
  Iceberg REST URL : http://$PRIV:${ICEBERG_PORT}
  Warehouse        : $WAREHOUSE
  S3 endpoint      : https://s3.${REGION}.amazonaws.com
  S3 access/secret : (blank — same account, cluster instance role)
  Namespace        : $NAMESPACE

--- Cluster UI: Storage Cache -> Production (direct S3 file pull) ---
  S3 endpoint      : https://s3.${REGION}.amazonaws.com
  Bucket           : $WH_BUCKET
  File/object key  : <a large TPC-DS parquet key in the warehouse>
  S3 access/secret : (blank — same account)

--- benchmarks (run on the box) ---
  warp (raw S3 to the gateway minioserver :9000):
    ssh -i $PEM ubuntu@$PUB "sudo docker run --rm --network host minio/warp mixed \\
      --host=$GWPRIV:9000 --access-key=<gw-key> --secret-key=<gw-secret> --tls=false \\
      --bucket=warp-bench --obj.size=16MiB --duration=1m --concurrent=32"
    (gateway keys: SSM the zs3server -> docker inspect minioserver env; the zus-router
     on :8088 is an iceberg cache, NOT a warp PUT target.)
  fio / mlperf: mount the cluster NFS export, then run fio/mlperf against the mount
    (NFS endpoint = the gateway; see the storage-bench runbook).
=======================================================
OUT
