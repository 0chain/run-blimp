#!/usr/bin/env bash
# test_setup_options.sh — unit tests for the `blimp --setup` choices:
#
#   A. Iceberg catalog   — gateway Nessie (default) | stand up here | existing URL
#   B. Dataset location  — fleet cache layer (default) | another S3 endpoint
#   C. Test dataset      — TPC-DS SF1 (default) / SF10 / SF100 / SF1000 | none
#
# OFFLINE BY DESIGN. Every network call is stubbed, so this runs in CI with no
# cluster, no docker and no AWS. The stubs reproduce REAL responses captured from
# a live gateway (node 37, 2026-09-13) — in particular Nessie's 500 for an unknown
# warehouse, which is what makes the warehouse a NAME and not an s3:// path.
#
#   ./test_setup_options.sh          run all
#   ./test_setup_options.sh -v       show each assertion
#
# The live end-to-end (generate -> register -> --storage -> --query) is a
# different thing and lives in test_setup_e2e.sh.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
VERBOSE=0; [ "${1:-}" = -v ] && VERBOSE=1

PASS=0; FAIL=0
ok(){   PASS=$((PASS+1)); [ "$VERBOSE" = 1 ] && printf '  \033[32m✓\033[0m %s\n' "$*"; return 0; }
bad(){  FAIL=$((FAIL+1)); printf '  \033[31m✗ %s\033[0m\n' "$*"; return 0; }
eq(){ # <actual> <expected> <label>
  if [ "$1" = "$2" ]; then ok "$3"; else bad "$3 — got '$1', want '$2'"; fi; }
case_(){ printf '\n\033[1m%s\033[0m\n' "$*"; }

# ---------------------------------------------------------------- stubs ----
# Nessie's real shapes. /v1/config answers for a KNOWN warehouse name and 500s
# for anything else; /admin/credentials carries the fleet address + keys.
FLEET_URL_STUB="https://fleet-8429413131.blimp.software:9443"
NESSIE_OK=1        # flip to 0 to simulate an unreachable :19122
FLEET_OK=1         # flip to 0 to simulate fleet mode off / old gateway
KNOWN_WAREHOUSE=mv

curl(){
  # Only the URL matters; every flag the callers use (-s/-sf/-m/-H) is ignored.
  # `curl -sf` returns 22 on an HTTP error, which is how choose_catalog detects
  # an unknown warehouse — so the unknown branch returns 22, not a JSON body.
  local url="" a
  for a in "$@"; do case "$a" in http*) url="$a" ;; esac; done
  case "$url" in
    *"/admin/credentials"*)
      [ "$FLEET_OK" = 1 ] || { printf '{"fleet_enabled":false}'; return 0; }
      printf '{"fleet_bearer_token":"tok","fleet_enabled":true,"fleet_s3_access_key":"AKIAFLEET","fleet_s3_secret_key":"SECRETFLEET","fleet_url":"%s"}' "$FLEET_URL_STUB"
      return 0 ;;
    *"/v1/config?warehouse=$KNOWN_WAREHOUSE")
      [ "$NESSIE_OK" = 1 ] || return 7
      printf '{"defaults":{"warehouse":"s3://tpcds-mv","prefix":"main|mv"}}'; return 0 ;;
    *"/v1/config?warehouse="*)
      [ "$NESSIE_OK" = 1 ] || return 7
      return 22 ;;                      # Nessie 500 "Warehouse ... is not known"
    *"/v1/config"*)
      [ "$NESSIE_OK" = 1 ] || return 7
      printf '{"defaults":{"warehouse":"s3://tpcds-mv","prefix":"main"}}'; return 0 ;;
  esac
  return 0
}

# Load the CLI's functions only.
# shellcheck disable=SC1090
BLIMP_LIB_ONLY=1 . "$HERE/blimp"

# Each case starts from a clean slate: these are the vars the choices read AND
# write, and a leftover value would silently satisfy the next case's prompt.
reset(){
  unset CATALOG_CHOICE ICEBERG_URL ICEBERG_PREFIX ICEBERG_URL_LOCAL ICEBERG_WAREHOUSE \
        NESSIE_WAREHOUSE SOURCE_CHOICE SOURCE_TARGET ORIGIN_BUCKET S3_ENDPOINT S3_KEY \
        S3_SECRET FLEET_URL FLEET_AK FLEET_SK BUILD_DATASET BLIMP_SF SF_CHOICE \
        WAREHOUSE reg_wh pfx_path 2>/dev/null
  GW=gw.example CLUSTER_ID=123 CLUSTER_TOKEN=blimp-123 REGION=ap-south-1
  NESSIE_OK=1; FLEET_OK=1; KNOWN_WAREHOUSE=mv
}

# =========================================================== pick() =========
case_ "pick() — defaults, overrides, bad input"
reset
eq "$(pick UNSET_A 'p' 2 a b c 2>/dev/null)" "2" "no tty -> default"
eq "$(PRESET=3 pick PRESET 'p' 1 a b c 2>/dev/null)" "3" "env preset wins"
eq "$(BAD=notanumber pick BAD 'p' 1 a b c 2>/dev/null)" "notanumber" "preset passes through verbatim"

# ================================================= A. catalog choices =======
case_ "A. Iceberg catalog"

reset; CATALOG_CHOICE=1; choose_catalog >/dev/null 2>&1
eq "$ICEBERG_URL"       "http://gw.example:19122/iceberg" "A1 default -> gateway Nessie URL"
eq "$ICEBERG_PREFIX"    "main"                            "A1 sets the branch prefix"
eq "$ICEBERG_WAREHOUSE" "mv"                              "A1 uses the warehouse NAME"
eq "$NESSIE_WAREHOUSE"  "s3://tpcds-mv"                   "A1 resolves the name to its bucket"

# An unknown warehouse name is a 500 from Nessie, not a silent default: the
# choice must degrade to standing a catalog up here rather than register into
# a warehouse that does not exist.
reset; CATALOG_CHOICE=1; ICEBERG_WAREHOUSE=nope; choose_catalog >/dev/null 2>&1
eq "${ICEBERG_URL:-}" "" "A1 unknown warehouse -> falls back to a local catalog"

# Same for a gateway whose :19122 is not reachable from this box.
reset; NESSIE_OK=0; CATALOG_CHOICE=1; choose_catalog >/dev/null 2>&1
eq "${ICEBERG_URL:-}" "" "A1 unreachable Nessie -> falls back to a local catalog"

reset; CATALOG_CHOICE=2; choose_catalog >/dev/null 2>&1
eq "${ICEBERG_URL:-}"    "" "A2 leaves the URL blank so standup_iceberg runs"
# A2 leaves prefix/warehouse blank HERE; standup_iceberg fills them in once the
# bucket is known, because the local catalog is now Nessie too and Nessie needs
# a warehouse LOCATION to be configured before it can name one.
eq "${ICEBERG_PREFIX:-}" "" "A2 defers the prefix to standup_iceberg"

reset; CATALOG_CHOICE=3; ICEBERG_URL=http://cat.example:8181; ICEBERG_PREFIX=""; choose_catalog >/dev/null 2>&1
eq "$ICEBERG_URL" "http://cat.example:8181" "A3 keeps the URL I supplied"

# =============================================== B. source location =========
case_ "B. Dataset (source) location"

reset; SOURCE_CHOICE=1; ORIGIN_BUCKET=blimp-src; choose_source_location >/dev/null 2>&1
eq "$SOURCE_TARGET" "fleet"           "B1 default -> fleet cache layer"
eq "$S3_ENDPOINT"   "$FLEET_URL_STUB" "B1 endpoint is the fleet address"
eq "$S3_KEY"        "AKIAFLEET"       "B1 key auto-fetched from the gateway"
eq "$S3_SECRET"     "SECRETFLEET"     "B1 secret auto-fetched from the gateway"

# No fleet credentials (old gateway / fleet off) must not offer option 1 at all.
reset; FLEET_OK=0; ORIGIN_BUCKET=mine; S3_ENDPOINT=https://s3.amazonaws.com; choose_source_location >/dev/null 2>&1
eq "$SOURCE_TARGET" "external" "B falls to external when the gateway has no fleet creds"

reset; SOURCE_CHOICE=2; ORIGIN_BUCKET=mine; S3_ENDPOINT=https://s3.us-east-1.amazonaws.com; choose_source_location >/dev/null 2>&1
eq "$SOURCE_TARGET" "external" "B2 -> external S3"
eq "$ORIGIN_BUCKET" "mine"     "B2 keeps my bucket"

# --------- the pairing guard ---------
# Nessie can only write metadata into a warehouse IT owns, which lives on the
# FLEET endpoint. With the data on another S3 the gateway would need two
# endpoints at once, so A1 must demote to A2 rather than produce a broken wiring.
case_ "A+B pairing guard"
reset
CATALOG_CHOICE=1; choose_catalog >/dev/null 2>&1
eq "$CATALOG_CHOICE" "1" "starts on the Nessie catalog"
SOURCE_CHOICE=2; ORIGIN_BUCKET=mine; S3_ENDPOINT=https://s3.us-east-1.amazonaws.com; choose_source_location >/dev/null 2>&1
eq "$CATALOG_CHOICE"  "2" "Nessie + external S3 demotes the catalog to local"
eq "${ICEBERG_URL:-}" ""  "and clears the URL so a local catalog is stood up"

# The compatible pair must NOT be demoted.
reset
CATALOG_CHOICE=1; choose_catalog >/dev/null 2>&1
SOURCE_CHOICE=1; ORIGIN_BUCKET=blimp-src; choose_source_location >/dev/null 2>&1
eq "$CATALOG_CHOICE" "1"                               "Nessie + fleet is kept"
eq "$ICEBERG_URL" "http://gw.example:19122/iceberg"    "and keeps the Nessie URL"

# ==================================================== C. test dataset =======
case_ "C. Test dataset + scale factor"
reset; BUILD_DATASET=1; SF_CHOICE=1; choose_dataset >/dev/null 2>&1
eq "$BUILD_DATASET" "1" "C default -> build a dataset"
eq "$BLIMP_SF"      "1" "C default scale factor is SF1"

for pair in "1 1" "2 10" "3 100" "4 1000"; do
  set -- $pair
  reset; BUILD_DATASET=1; SF_CHOICE=$1; choose_dataset >/dev/null 2>&1
  eq "$BLIMP_SF" "$2" "C choice $1 -> SF$2"
done

reset; BUILD_DATASET=2; choose_dataset >/dev/null 2>&1
eq "$BUILD_DATASET" "0" "C 'no' -> bring your own data"

# ================================================ registration wiring =======
# The registrar takes the warehouse NAME for Nessie and the s3:// path for a
# plain REST catalog. Sending a path to Nessie is a hard 500, so this mapping is
# the difference between a working registration and a broken one.
case_ "registration warehouse mapping"
reset; CATALOG_CHOICE=1; choose_catalog >/dev/null 2>&1
WAREHOUSE="s3://blimp-src/wh"
reg_wh="$WAREHOUSE"; [ -n "${ICEBERG_WAREHOUSE:-}" ] && reg_wh="$ICEBERG_WAREHOUSE"
eq "$reg_wh" "mv" "Nessie gets the warehouse NAME"

reset; CATALOG_CHOICE=2; choose_catalog >/dev/null 2>&1
WAREHOUSE="s3://blimp-src/wh"
reg_wh="$WAREHOUSE"; [ -n "${ICEBERG_WAREHOUSE:-}" ] && reg_wh="$ICEBERG_WAREHOUSE"
eq "$reg_wh" "s3://blimp-src/wh" "before standup, a local catalog still carries the s3:// path"

# The namespace probe path differs too: Nessie nests every route under its
# branch prefix, a plain catalog does not.
reset; CATALOG_CHOICE=1; choose_catalog >/dev/null 2>&1
pfx_path=""; [ -n "${ICEBERG_PREFIX:-}" ] && pfx_path="/$ICEBERG_PREFIX"
eq "$pfx_path" "/main" "Nessie probe path carries the branch prefix"
reset; CATALOG_CHOICE=2; choose_catalog >/dev/null 2>&1
pfx_path=""; [ -n "${ICEBERG_PREFIX:-}" ] && pfx_path="/$ICEBERG_PREFIX"
eq "$pfx_path" "" "before standup, the local catalog has no prefix yet"

# The WAREHOUSE that reaches the gateway is the same value the registrar uses,
# because /admin/source/configure sends it as IRC_URL "<url>|<warehouse>". A
# first E2E on node 37 wired the Nessie URL with "s3://blimp-e2e/wh" — a
# warehouse Nessie does not know — which would 500 every source read. The
# default must therefore be the NAME whenever the Nessie catalog is in play.
case_ "warehouse sent to /admin/source/configure"
reset; CATALOG_CHOICE=1; choose_catalog >/dev/null 2>&1
ORIGIN_BUCKET=blimp-e2e
WAREHOUSE=$(ask WAREHOUSE "  w" "${ICEBERG_WAREHOUSE:-${ORIGIN_BUCKET:+s3://$ORIGIN_BUCKET/wh}}" 2>/dev/null)
eq "$WAREHOUSE" "mv" "Nessie chosen -> warehouse defaults to the NAME, not an s3:// path"

reset; CATALOG_CHOICE=2; choose_catalog >/dev/null 2>&1
ORIGIN_BUCKET=blimp-e2e
WAREHOUSE=$(ask WAREHOUSE "  w" "${ICEBERG_WAREHOUSE:-${ORIGIN_BUCKET:+s3://$ORIGIN_BUCKET/wh}}" 2>/dev/null)
eq "$WAREHOUSE" "s3://blimp-e2e/wh" "local catalog -> warehouse is the s3:// path"

# ===================================================== standup_data leg =====
# The fleet leg must be selected by BLIMP_DATA_TARGET and must refuse to run
# without an endpoint — silently falling through to the MinIO leg would put the
# dataset on the wrong box.
case_ "standup_data.sh fleet leg"
if grep -q 'BLIMP_DATA_TARGET:-' "$HERE/standup_data.sh"; then ok "fleet leg is gated on BLIMP_DATA_TARGET"; else bad "fleet leg gate missing"; fi
if grep -q 'FLEET_ENDPOINT:?' "$HERE/standup_data.sh"; then ok "fleet leg requires FLEET_ENDPOINT"; else bad "fleet leg does not require an endpoint"; fi
out=$(BLIMP_DATA_TARGET=fleet BLIMP_SF=1 bash "$HERE/standup_data.sh" 2>&1 >/dev/null); rc=$?
if [ "$rc" != 0 ]; then ok "fleet leg fails loudly with no endpoint"; else bad "fleet leg silently succeeded with no endpoint"; fi

# ONE CATALOG TYPE. The local catalog used to be tabulario/iceberg-rest, a
# different implementation from the Nessie the gateway runs — so every consumer
# had to branch on prefix-vs-root and name-vs-path. Standing up Nessie here too
# removes that split; these assertions keep it from creeping back.
case_ "local catalog is Nessie (same as internal)"
if grep -q 'ghcr.io/projectnessie/nessie' "$HERE/blimp"; then ok "standup uses the Nessie image"; else bad "standup is not Nessie"; fi
# Ignore comment lines: the history of the switch is documented in place, and
# grepping the whole file would flag that explanation forever.
if grep -v '^[[:space:]]*#' "$HERE/blimp" | grep -q 'tabulario/iceberg-rest'; then
  bad "tabulario/iceberg-rest is still used"; else ok "no second catalog implementation left"; fi
for e in NESSIE_CATALOG_DEFAULT_WAREHOUSE NESSIE_VERSION_STORE_TYPE QUARKUS_MANAGEMENT_ENABLED; do
  if grep -q "$e" "$HERE/blimp"; then ok "standup sets $e"; else bad "standup missing $e"; fi
done
if grep -q 'ICEBERG_PREFIX=main; ICEBERG_WAREHOUSE="$wname"' "$HERE/blimp"; then
  ok "standup yields the same prefix+warehouse-name shape as the gateway Nessie"
else bad "standup does not set the unified prefix/warehouse shape"; fi
# The gateway is wired with WAREHOUSE; for Nessie that must be the NAME, and
# standup_data.sh's WAREHOUSE=s3://... must not clobber it.
if grep -q 'ICEBERG_WAREHOUSE:-}" \] && WAREHOUSE="$ICEBERG_WAREHOUSE"' "$HERE/blimp"; then
  ok "generated-data WAREHOUSE cannot clobber the Nessie warehouse name"
else bad "WAREHOUSE clobber guard missing"; fi

# heal_wiring must read the LIVE token file, not the container env. Rotating the
# fleet token rewrites the file but cannot rewrite a running container's env, so
# healing from the env pins a stale token and every admin call 401s.
case_ "heal_wiring token source"
if grep -q "cat /opt/0chain/zs3server/environment/admin_token" "$HERE/blimp"; then
  ok "heal_wiring reads the live admin_token file"
else bad "heal_wiring does not read the live token file"; fi
if sed -n '/^heal_wiring(){/,/^}/p' "$HERE/blimp" | grep -q 'tok=$(cat /opt/0chain'; then
  ok "the file is tried BEFORE the container env"
else bad "the container env still wins over the file"; fi

# ========================================================= registrar ========
case_ "register_tpcds_tables.py flags"
for f in --prefix --s3-key --s3-secret; do
  if grep -q -- "\"$f\"" "$HERE/register_tpcds_tables.py"; then ok "registrar accepts $f"; else bad "registrar missing $f"; fi
done
if "${BLIMP_PY:-python3}" -c "import ast,sys; ast.parse(open('$HERE/register_tpcds_tables.py').read())" 2>/dev/null; then
  ok "registrar parses"; else bad "registrar has a syntax error"; fi

# ================================================== query_tables.py =========
# --sql: the queries' tables come from the SQL text (every FROM/JOIN item, comma
# lists and nested subqueries included, CTE names excluded). Offline via --list-refs.
case_ "query_tables.py table references"
QT_TMP=$(mktemp -d)
cat > "$QT_TMP/a.sql" <<'SQL'
with ctr as (select sr_customer_sk c, sum(sr_return_amt) t from store_returns, date_dim where sr_returned_date_sk = d_date_sk group by 1)
select c_customer_id from ctr, store, customer -- from nothing
where exists (select 1 from (select x from t1 join t2 using (k)) s left join t3 on s.x = t3.y) limit 10
SQL
refs=$("${BLIMP_PY:-python3}" "$HERE/query_tables.py" --sql-file "$QT_TMP/a.sql" --list-refs | "${BLIMP_PY:-python3}" -c 'import json,sys;print(" ".join(json.load(sys.stdin)["refs"]))')
if [ "$refs" = "store_returns date_dim store customer t1 t2 t3" ]; then ok "refs: $refs"; else bad "refs wrong: '$refs'"; fi
rm -rf "$QT_TMP"

# ================================================ public gw default =========
# The external-mode gateway default must be the CURRENT public pattern. The
# legacy zus-<id>-0.zus.network was hardcoded here and no longer resolves at
# all, so a client that could not discover the gateway was handed a dead name.
case_ "public gateway default"
g=$(pub_gw_default 1789320141035)
case "$g" in
  blimp-1789320141035-0.blimp.software) ok "prefers blimp-<id>-0.blimp.software ($g)" ;;
  *) bad "public default is not the current pattern: $g" ;;
esac
g2=$(pub_gw_default 9999999999999)   # resolves nowhere → still the current pattern
case "$g2" in
  blimp-*) ok "unresolvable id keeps the current pattern ($g2)" ;;
  *) bad "unresolvable id fell back to a dead legacy name: $g2" ;;
esac

[ "$FAIL" = 0 ]

# ---------------------------------------------------- scale factors ----
# A fresh cloud node reported (2026-09-15) that the SF picker stopped at SF1000.
# The generator is duckdb's dsdgen(sf=N), which takes any N; the only real limit
# is local scratch, which scratch_pick() now checks up front.
case_ "scale factors offered"

SF_BLOCK=$(sed -n '/pick SF_CHOICE/,/esac/p' "$HERE/blimp")
for sf in 1 10 100 1000 10000 100000; do
  if printf '%s' "$SF_BLOCK" | grep -q "BLIMP_SF=$sf;"; then ok "SF$sf selectable"
  else bad "SF$sf missing from the picker"; fi
done
# The mapping must be 1:1 — a duplicated case arm silently generates the wrong
# size, which at these scales is hours of wasted generation.
DUPES=$(printf '%s' "$SF_BLOCK" | grep -o 'BLIMP_SF=[0-9]*' | sort | uniq -d)
eq "$DUPES" "" "each menu entry maps to a distinct scale factor"

# ------------------------------------------- gateway-S3 source option ----
# Reported: with the fleet endpoint unreachable, the only other route stood up a
# MinIO container on :9000 — a Docker Hub pull a locked-down box refuses, and a
# port collision with the gateway's own minioserver. The node's own gateway S3
# is right there.
case_ "dataset source options"

SRC_BLOCK=$(sed -n '/choose_source_location(){/,/^}/p' "$HERE/blimp")
printf '%s' "$SRC_BLOCK" | grep -q "SOURCE_TARGET=gateway" \
  && ok "the node's own gateway S3 is offered as a source" \
  || bad "no gateway-S3 source option"
printf '%s' "$SRC_BLOCK" | grep -q 'GW_AK' \
  && ok "gateway source uses the node's own MinIO root keys" \
  || bad "gateway source does not use GW_AK/GW_SK"
# It must degrade, not dead-end: no fleet AND no gateway keys still has to reach
# the external branch rather than leaving SOURCE_TARGET unset.
printf '%s' "$SRC_BLOCK" | grep -q 'c=3' \
  && ok "falls through to an external endpoint when neither is available" \
  || bad "no fallback when fleet and gateway are both unavailable"

# standup_data.sh must route `gateway` the same way it routes `fleet` — both are
# "an S3 endpoint plus keys the caller already resolved".
grep -q 'fleet|gateway' "$HERE/standup_data.sh" \
  && ok "standup_data.sh uploads a gateway source like a fleet source" \
  || bad "standup_data.sh does not handle BLIMP_DATA_TARGET=gateway"

# ------------------------------------------------ unreachable endpoints ----
# The fleet hostname resolves to the node's OWN public IP, which a cloud VM
# cannot hairpin to; the gateway's minioserver:9000 resolves only inside docker.
# endpoint_local() probes and rewrites to loopback ONLY when loopback serves it.
case_ "endpoints that point back at this node"

EL=$(sed -n '/^endpoint_local(){/,/^}/p' "$HERE/blimp")
[ -n "$EL" ] && ok "endpoint_local exists" || bad "endpoint_local missing"
printf '%s' "$EL" | grep -q '127.0.0.1' \
  && ok "rewrites to loopback" || bad "no loopback rewrite"
# The safety property: never rewrite an endpoint that is genuinely reachable.
printf '%s' "$EL" | grep -q 'opens(host, port)' \
  && ok "leaves a reachable endpoint alone" \
  || bad "does not check the published address first"

for site in 'FLEET_ENDPOINT="$(endpoint_local' 'reg_ep="--s3-endpoint $(endpoint_local'; do
  grep -qF "$site" "$HERE/blimp" \
    && ok "applied at: ${site%%=*}" || bad "not applied at ${site%%=*}"
done

# ------------------------------------------------------- scratch space ----
# The mlperf leg wrote ~33 GiB to /var/tmp (boot disk) and aborted the whole
# suite with ENOSPC after earlier legs had already produced numbers.
case_ "local scratch is sized before it is used"

[ -f "$HERE/scratch_dir.sh" ] && ok "scratch_dir.sh present" || bad "scratch_dir.sh missing"
grep -q 'scratch_pick' "$HERE/standup_data.sh" \
  && ok "dataset generation sizes its scratch" || bad "generation still assumes the boot disk"
grep -q 'scratch_pick' "$HERE/run_cluster.sh" \
  && ok "mlperf gen sizes its scratch" || bad "mlperf gen still defaults to /var/tmp"
# Check the duckdb INVOCATION, not any mention of /tmp: the script also cleans
# up the old boot-disk path for anyone upgrading, which is not a regression.
if grep -E '^\s*duckdb "' "$HERE/standup_data.sh" | grep -q 'SF_SCRATCH'; then
  ok "duckdb working DB is staged off the boot disk"
else
  bad "duckdb still opens its working DB on the boot disk"
fi

# ------------------------------------------------------ cloud portability ----
# IMDS is AWS-only; other clouds answer the same URL with an HTML error page,
# which used to be stored as an IP/region and printed mid-run.
case_ "non-AWS clouds"

grep -q 'imds_ipv4' "$HERE/blimp" \
  && ok "IMDS responses are shape-checked before use" \
  || bad "IMDS responses used unvalidated"
sed -n '/placement\/region/,+2p' "$HERE/run_cluster.sh" | grep -q 'a-z\]\[a-z\]-' \
  && ok "region is shape-checked before use" || bad "region used unvalidated"


# ------------------------------------------------- CLI vs UI comparability ----
# The panel runs run_bench.sh (embedded in zs3-init.go); the CLI runs
# run_cluster.sh. They measure the same box, so their knobs must agree or the two
# numbers are not comparable — which is exactly what was reported. warp
# concurrency was the outlier: a flat 64 (>=8 shards) / 16 in the CLI against the
# panel's "gateway vCPUs", i.e. 8x apart on an 8-vCPU gateway, on the single knob
# that most moves the result.
case_ "CLI benchmark knobs match the panel's"

RC="$HERE/run_cluster.sh"

# warp concurrency: vCPU-derived, not a flat tier.
if grep -qE '^_gwcpu=\$\(nproc' "$RC" && grep -qF '_warpconc=$(( _gwcpu * 2 ))' "$RC" && grep -qF 'EC_CONC=$_warpconc' "$RC"; then
  ok "warp concurrency = 2x gateway vCPUs (measured PUT peak)"
else
  bad "warp concurrency is not 2x-vCPU-derived — CLI and panel numbers diverge"
fi
grep -qE 'EC_CONC=(64|16)$' "$RC" \
  && bad "a flat warp-concurrency tier is still present" \
  || ok "no flat 64/16 warp tier remains"

# The reader concurrency alignment that was already done — keep it.
grep -qF 'EC_RT="${MLPERF_RT:-$_gwcpu}"' "$RC" \
  && ok "mlperf read threads = gateway vCPUs (matches the panel)" \
  || bad "mlperf rt no longer matches the panel"
grep -qF 'EC_PF="${MLPERF_PF:-$(( EC_RT * 2 ))}"' "$RC" \
  && ok "mlperf prefetch = rt*2 (matches the panel)" \
  || bad "mlperf pf no longer matches the panel"

# The override has to survive: it is how you deliberately push past vCPU count.
grep -qF 'EC_CONC="${WARP_CONC:-$EC_CONC}"' "$RC" \
  && ok "WARP_CONC still overrides" || bad "WARP_CONC override lost"

# Object size must match too, or the throughput figures are not comparable.
grep -qF 'OSZ="${WARP_OBJ_SIZE:-96MiB}"' "$RC" \
  && ok "warp object size 96MiB (matches the panel)" || bad "warp object size drifted"

printf '\n\033[1m%d passed, %d failed\033[0m\n' "$PASS" "$FAIL"
# Exit non-zero on failure so CI and `blimp --selftest` actually gate on this.
[ "$FAIL" -eq 0 ]