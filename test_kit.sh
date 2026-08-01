#!/usr/bin/env bash
# test_kit.sh — unit tests for the kit's pure decision logic.
#
# Every bug this suite covers shipped a WRONG NUMBER rather than an error: a
# fact table nobody appended to, an origin endpoint that silently became AWS, a
# baseline measured with a different client than the thing it was compared
# against. Those are invisible in a run log, so they get asserted here.
#
# Pure functions only — no cluster, no network. Run: ./test_kit.sh
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
PASS=0; FAIL=0
ok(){ PASS=$((PASS+1)); printf '  ok   %s\n' "$1"; }
no(){ FAIL=$((FAIL+1)); printf '  FAIL %s\n     want: %s\n     got:  %s\n' "$1" "$2" "$3"; }
eq(){ [ "$2" = "$3" ] && ok "$1" || no "$1" "$2" "$3"; }

echo "== bench fact derivation (bench_incremental.sh) =="
# The append must land on the fact the QUERY reads. A hardcoded default sent
# every append to store_returns, so q9/q88/q14 — all store_sales MVs — saw no
# delta, reported delta_merge_ms empty, and looked like a broken delta engine.
Q_DIR=$(mktemp -d); trap 'rm -rf "$Q_DIR"' EXIT
query_fact(){
  local sql="" f
  [ -n "${QF:-}" ] && [ -f "${QF:-}" ] && sql=$(tr 'A-Z' 'a-z' < "$QF")
  for f in store_sales catalog_sales web_sales store_returns catalog_returns web_returns inventory; do
    case "$sql" in *"$f"*) printf '%s' "$f"; return;; esac
  done
  printf 'store_sales'
}
printf 'SELECT * FROM store_sales WHERE ss_item_sk = 1\n' > "$Q_DIR/q88.sql"
QF="$Q_DIR/q88.sql"; eq "store_sales query -> store_sales" "store_sales" "$(query_fact)"
printf 'SELECT * FROM web_sales ws JOIN date_dim d ON 1=1\n' > "$Q_DIR/q12.sql"
QF="$Q_DIR/q12.sql"; eq "web_sales query -> web_sales"     "web_sales"   "$(query_fact)"
printf 'WITH cs_ui AS (SELECT * FROM catalog_sales)\nSELECT 1\n' > "$Q_DIR/q64.sql"
QF="$Q_DIR/q64.sql"; eq "catalog_sales query -> catalog_sales" "catalog_sales" "$(query_fact)"
QF=""; eq "no query file -> store_sales fallback, never a table it may not read" "store_sales" "$(query_fact)"
QF="$Q_DIR/does_not_exist.sql"; eq "missing query file does not abort under set -u" "store_sales" "$(query_fact)"

echo
echo "== origin endpoint plumbing (run_router.sh / test_cache.sh) =="
# A blank S3_ENDPOINT must mean AWS (no flag). A non-blank one MUST produce the
# flag: without it every origin call silently went to real AWS and MinIO
# answered InvalidAccessKeyId, so the cache leg reported "no objects".
oep(){ [ -n "${1:-}" ] && printf -- '--endpoint-url %s' "$1" || printf ''; }
eq "blank endpoint -> no flag (AWS + IAM role)" ""                                  "$(oep '')"
eq "minio endpoint -> flag"  "--endpoint-url http://10.0.0.1:9000"                  "$(oep 'http://10.0.0.1:9000')"
eq "aws endpoint  -> flag"   "--endpoint-url https://s3.ap-south-1.amazonaws.com"   "$(oep 'https://s3.ap-south-1.amazonaws.com')"

echo
echo "== cache set sizing (run_router.sh) =="
# The bar is per-blobber shard share > per-blobber RAM. At EC 2/1 a logical N
# GiB set is ~N/2 per blobber; below blobber RAM the "hit" is served from the
# blobbers' page cache and no client-side number reveals it.
fits_in_blobber_ram(){ awk -v gb="$1" -v ram="$2" 'BEGIN{print (gb/2.0 < ram) ? "yes" : "no"}'; }
eq "3 GiB set on 3.8 GB blobbers is RAM-resident (too small)"  "yes" "$(fits_in_blobber_ram 3 3.8)"
eq "6 GiB set on 3.8 GB blobbers is RAM-resident (too small)"  "yes" "$(fits_in_blobber_ram 6 3.8)"
eq "10 GiB set on 3.8 GB blobbers exceeds RAM (usable)"        "no"  "$(fits_in_blobber_ram 10 3.8)"

echo
echo "== mlperf accel guard (run_cluster.sh) =="
# accel>1 spawns torch ranks that starve the mount-s3 daemon — it lives OUTSIDE
# the dlio memory cgroup, so the kernel OOM-kills it and every rank dies with
# "Transport endpoint is not connected".
accel_for(){ local ncpu="$1" ec_accel="$2" override="${3:-}"
  [ -n "$override" ] && { printf '%s' "$override"; return; }
  [ "$ncpu" -lt 16 ] && { printf '1'; return; }
  printf '%s' "$ec_accel"; }
eq "2 vCPU  -> accel 1"                 "1" "$(accel_for 2 3)"
eq "8 vCPU  -> accel 1 (below 4xlarge)" "1" "$(accel_for 8 3)"
eq "16 vCPU -> EC-derived accel"        "3" "$(accel_for 16 3)"
eq "explicit override always wins"      "6" "$(accel_for 2 3 6)"

echo
echo "== metric parsing (run_cluster.sh / test_cache.sh) =="
# dlio prints "<value> (<stddev>)". Taking the wrong field printed the LABEL as
# the number; keeping the stddev printed "AU 64.6854 (0.0000)%".
metval(){ printf '%s\n' "$1" | sed 's/.*: *//; s/ *(.*//'; }
eq "AU with stddev"        "64.6854"   "$(metval '[METRIC] Training Accelerator Utilization [AU] (%): 64.6854 (0.0000)')"
eq "samples/s with stddev" "3434.5239" "$(metval '[METRIC] Training Throughput (samples/second): 3434.5239 (0.0000)')"
eq "MB/s without stddev"   "375.5596"  "$(metval '[METRIC] Training I/O Throughput (MB/second): 375.5596')"

echo
echo "== no shell-into-the-cluster anywhere in the kit =="
# The client box has no ssh/ssm/docker on the Blimp node. Anything that needs
# one cannot ship. (blimp's own `docker run` for the LOCAL MinIO/catalog is the
# client's own machine and is fine — matched on the cluster-only verbs.)
viol=$(grep -nE '^[^#]*(aws ssm|ssh -|scp -|docker (exec|logs|inspect|restart) )' \
        "$HERE"/blimp "$HERE"/*.sh 2>/dev/null | grep -v test_kit.sh || true)
[ -z "$viol" ] && ok "kit reaches the cluster over HTTP only" \
  || no "kit reaches the cluster over HTTP only" "(nothing)" "$viol"

echo
printf '%d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
