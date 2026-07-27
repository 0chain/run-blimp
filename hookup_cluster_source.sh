#!/usr/bin/env bash
# hookup_cluster_source.sh — point a RUNNING Blimp cluster's query optimizer +
# read-through cache (zus-router) at YOUR Iceberg catalog + S3 bucket.
# Run ON the cluster GATEWAY (ssh/SSM), as root:  sudo bash hookup_cluster_source.sh
#
# VALIDATED 2026-07-17 against a live cluster. Gotchas this script encodes:
#   * The gateway env is BAKED into the docker-compose file (mapping-style
#     `KEY: "value"` under the minioserver service) — there is NO .env file on
#     current clusters. We VALUE-REPLACE the existing ZS3_SOURCE_* keys.
#   * compose v1 `--force-recreate` is broken (KeyError: ContainerConfig) —
#     recreate is stop/rm then `up -d`.
#   * zus-router runs with empty-string args (-s3-access-key "" = use the IAM
#     instance role). Naive shell re-quoting turns "" into a literal '' and the
#     router then signs with a bogus static key (403 InvalidAccessKeyId) —
#     args must be passed as an exec list, never through word splitting.
#
# Config (env):
#   ICEBERG_URL   e.g. http://10.10.37.179:8181     (your Iceberg REST catalog)
#   WAREHOUSE     e.g. s3://my-bucket/wh            (your catalog's warehouse)
#   NAMESPACE     e.g. tpcds
#   ORIGIN_BUCKET e.g. my-bucket                    (bucket the cache fronts)
#   S3_REGION     e.g. ap-south-1                   (the BUCKET's region)
#   S3_ENDPOINT   default https://s3.<S3_REGION>.amazonaws.com
#   S3_KEY/S3_SECRET  blank = the gateway's IAM instance role
set -euo pipefail
: "${ICEBERG_URL:?set ICEBERG_URL}" "${WAREHOUSE:?set WAREHOUSE}" "${NAMESPACE:?set NAMESPACE}"
: "${ORIGIN_BUCKET:?set ORIGIN_BUCKET}" "${S3_REGION:?set S3_REGION}"
S3_ENDPOINT="${S3_ENDPOINT:-https://s3.${S3_REGION}.amazonaws.com}"
S3_KEY="${S3_KEY:-}"; S3_SECRET="${S3_SECRET:-}"
log(){ printf '[hookup] %s\n' "$*"; }

# --- 0. locate the compose file from the running gateway container ------------
# The config_files label is RELATIVE when the container was last recreated with
# a relative -f (observed 2026-07-17: silent exit 2) — resolve against the
# compose working_dir label. Fallbacks must cover the /environment subdir and
# must not die under set -e/pipefail when the glob matches nothing.
COMPOSE=$(docker inspect minioserver --format '{{index .Config.Labels "com.docker.compose.project.config_files"}}' 2>/dev/null || true)
case "$COMPOSE" in
  /*|"") ;;
  *) WD=$(docker inspect minioserver --format '{{index .Config.Labels "com.docker.compose.project.working_dir"}}' 2>/dev/null || true)
     [ -n "$WD" ] && COMPOSE="$WD/$COMPOSE" ;;
esac
[ -n "$COMPOSE" ] && [ -f "$COMPOSE" ] || COMPOSE=$(ls /opt/0chain/zs3server/environment/docker-compose*.y*ml /opt/0chain/zs3server/docker-compose*.y*ml /var/0chain/zs3server/docker-compose*.y*ml 2>/dev/null | head -1 || true)
[ -n "$COMPOSE" ] && [ -f "$COMPOSE" ] || { log "ERROR: cannot locate the gateway compose file"; exit 1; }
WORKDIR=$(dirname "$COMPOSE")
log "compose: $COMPOSE"
cp -a "$COMPOSE" "$COMPOSE.bak-$(date +%s)"

# --- 1. value-replace the ZS3_SOURCE_* keys (mapping style: KEY: "value") -----
GWPRIV=$(hostname -I | awk '{print $1}')
export HK_ICEBERG_URL="$ICEBERG_URL" HK_WAREHOUSE="$WAREHOUSE" HK_NAMESPACE="$NAMESPACE" \
       HK_BUCKET="$ORIGIN_BUCKET" HK_EP="$S3_ENDPOINT" HK_REGION="$S3_REGION" \
       HK_KEY="$S3_KEY" HK_SECRET="$S3_SECRET" HK_COMPOSE="$COMPOSE" HK_GWPRIV="$GWPRIV"
python3 - <<'PY'
import os, re
path = os.environ["HK_COMPOSE"]
src = open(path).read()
vals = {
    "ZS3_SOURCE_IRC_URL":        os.environ["HK_ICEBERG_URL"] + "|" + os.environ["HK_WAREHOUSE"],
    "ZS3_SOURCE_IRC_NAMESPACE":  os.environ["HK_NAMESPACE"],
    "ZS3_SOURCE_BUCKET":         os.environ["HK_BUCKET"],
    "ZS3_SOURCE_S3_ENDPOINT":    os.environ["HK_EP"],
    "ZS3_SOURCE_S3_REGION":      os.environ["HK_REGION"],
    "ZS3_SOURCE_S3_KEY_ID":      os.environ["HK_KEY"],
    "ZS3_SOURCE_S3_SECRET":      os.environ["HK_SECRET"],
    "ZS3_SOURCE_APPEND_ENABLED": "true",
}
vals["ZS3_SOURCE_S3_REGION"] = os.environ["HK_REGION"]
# route source reads through the read-through cache (:8088) — without these a
# fresh cluster reads S3 DIRECT and the cache/panel never sees query traffic
vals["ZS3_SOURCE_VIA_ROUTER"] = "1"
vals["ZS3_SOURCE_ROUTER_BASE"] = "http://" + os.environ["HK_GWPRIV"] + ":8088"
vals["ZS3_SOURCE_CATALOG"] = "source"
# tab-source registry: the query API's source="customer" resolves the
# ZS3_SRC_CUSTOMER_* set (resolveSourceEnv), NOT the legacy ZS3_SOURCE_* keys —
# without these every "Run on Prod" errors "no external Prod source is wired".
vals["ZS3_SRC_CUSTOMER_SCHEMA"]        = os.environ["HK_NAMESPACE"]
vals["ZS3_SRC_CUSTOMER_CATALOG"]       = "source"
vals["ZS3_SRC_CUSTOMER_IRC_URL"]       = os.environ["HK_ICEBERG_URL"] + "|" + os.environ["HK_WAREHOUSE"]
vals["ZS3_SRC_CUSTOMER_IRC_NAMESPACE"] = os.environ["HK_NAMESPACE"]
vals["ZS3_SRC_CUSTOMER_S3_ENDPOINT"]   = os.environ["HK_EP"]
vals["ZS3_SRC_CUSTOMER_S3_KEY_ID"]     = os.environ["HK_KEY"]
vals["ZS3_SRC_CUSTOMER_S3_SECRET"]     = os.environ["HK_SECRET"]
vals["ZS3_SRC_CUSTOMER_S3_REGION"]     = os.environ["HK_REGION"]
vals["ZS3_SRC_CUSTOMER_BUCKET"]        = os.environ["HK_BUCKET"]
vals["ZS3_SRC_CUSTOMER_APPEND_ENABLED"]= "true"
missing = []
for k, v in vals.items():
    pat = re.compile(r'^(\s+%s:\s*).*$' % re.escape(k), re.M)
    if pat.search(src):
        src = pat.sub(lambda m: m.group(1) + '"' + v + '"', src)
    else:
        missing.append(k)
# APPEND keys the compose doesn't carry (a fresh provision ships a subset —
# 2026-07-17: BUCKET + S3_REGION were absent and the sidecar fell back to
# internal defaults, breaking the source attach). Insert after an existing
# ZS3_SOURCE_ line so they land in the same service's env block, same indent.
if missing:
    anchor = re.search(r'^(\s+)ZS3_SOURCE_[A-Z_]+:.*$', src, re.M)
    if not anchor:
        raise SystemExit("[hookup] ERROR: no ZS3_SOURCE_ block in compose to anchor on")
    indent = anchor.group(1)
    add = "".join("\n%s%s: \"%s\"" % (indent, k, vals[k]) for k in missing)
    src = src[:anchor.end()] + add + src[anchor.end():]
    print("[hookup] appended missing keys:", ",".join(missing))
open(path, "w").write(src)
PY
python3 -c "import yaml,sys; yaml.safe_load(open('$COMPOSE'))" && log "compose YAML valid"

# --- 2. recreate the gateway (compose v1: stop/rm then up, NEVER --force-recreate) --
( cd "$WORKDIR"
  docker-compose -f "$COMPOSE" stop minioserver >/dev/null 2>&1 || docker stop minioserver
  docker-compose -f "$COMPOSE" rm -f minioserver >/dev/null 2>&1 || docker rm -f minioserver
  docker-compose -f "$COMPOSE" up -d minioserver )
sleep 4
docker exec minioserver env | grep -E '^ZS3_SOURCE_(IRC_URL|BUCKET|S3_REGION)=' | sed 's/^/[hookup] gw env: /'

# --- 3. recreate zus-router pointed at the origin bucket ----------------------
# Preserve image + flags, swap only the s3/bucket flags; pass args as an exec
# list so empty strings survive (see header).
docker inspect zus-router --format '{{json .Config.Cmd}}' > /tmp/.hookup_router_cmd.json 2>/dev/null || echo '[]' > /tmp/.hookup_router_cmd.json
RIMG=$(docker inspect zus-router --format '{{.Config.Image}}' 2>/dev/null || echo 0chaindev/router:latest)
docker rm -f zus-router >/dev/null 2>&1 || true
HK_RIMG="$RIMG" python3 - <<'PY'
import json, os, subprocess
args = ["" if a == "''" else a for a in json.load(open("/tmp/.hookup_router_cmd.json"))]
def setf(flag, val):
    if flag in args:
        args[args.index(flag) + 1] = val
    else:
        args.extend([flag, val])
setf("-bucket",         os.environ["HK_BUCKET"])
setf("-cache-buckets",  os.environ["HK_BUCKET"])
setf("-s3-endpoint",    os.environ["HK_EP"])
setf("-s3-region",      os.environ["HK_REGION"])
setf("-s3-access-key",  os.environ["HK_KEY"])
setf("-s3-secret-key",  os.environ["HK_SECRET"])
cmd = ["docker", "run", "-d", "--name", "zus-router", "--restart", "unless-stopped",
       "--network", "host", os.environ["HK_RIMG"]] + args
subprocess.run(cmd, check=True)
PY
sleep 3
docker logs zus-router 2>&1 | grep -i credential | tail -1 | sed 's/^/[hookup] router: /'
curl -sf http://localhost:8088/iostats >/dev/null && log "router :8088 up"
curl -sf -o /dev/null http://localhost:9000/minio/health/live && log "gateway :9000 up"
log "DONE — source=${ICEBERG_URL}|${WAREHOUSE} ns=${NAMESPACE}, cache fronts s3://${ORIGIN_BUCKET} (${S3_REGION})"
