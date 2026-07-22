#!/usr/bin/env bash
# test_archetype.sh — observe the MV author LOGIC without materialising anything.
# For each query, POST /admin/mv/resolve {rank:true}: the gateway enumerates
# EVERY applicable archetype (hand-crafted deterministic schemes + the 105-entry
# federated library), sample-estimates each candidate MV's row count IN PARALLEL,
# ranks by dimension, and returns:
#   path            library_exact | matcher | miss
#   scheme          the picked archetype
#   archetype_candidates  ranked top-3: {scheme, est_rows, ratio}
#   weak_compression      true = even the best barely compresses (→ defers to LLM)
# Nothing is materialised — this is the pure "how does the optimizer choose"
# probe. Fast + cheap + deterministic; safe to run against any cluster.
#
#   GATEWAY=<gw-ip> CLUSTER_ID=<id> [Q_DIR=~/tpcds_queries] [N=20] ./test_archetype.sh
#
# Requires the gateway to have dimension-ranking on (default 2026-07-17; or
# ZS3_MV_DIMENSION_RANKING=1). Without it, archetype_candidates is empty and
# you only see the first-match scheme.
set -u
: "${GATEWAY:?}" "${CLUSTER_ID:?}"
N="${N:-20}"; Q_DIR="${Q_DIR:-$HOME/tpcds_queries}"
GW="http://$GATEWAY:9000"; TOKEN="zus-$CLUSTER_ID"; AUTH="Authorization: Bearer $TOKEN"

# a small built-in set (q1 archetype + structurally distinct shapes) so the test
# runs even without the TPC-DS 99 files on the box.
declare -a QUERIES LABELS
QUERIES+=("with ctr as (select sr_customer_sk, sr_store_sk, sum(sr_return_amt) t from store_returns, date_dim where sr_returned_date_sk=d_date_sk and d_year=2000 group by sr_customer_sk, sr_store_sk) select c_customer_id from ctr, store, customer where t > (select avg(t)*1.2 from ctr c2 where ctr.sr_store_sk=c2.sr_store_sk) and s_store_sk=ctr.sr_store_sk and s_state='TN' and ctr.sr_customer_sk=c_customer_sk order by c_customer_id limit 100")
LABELS+=("q1 customer_total_return (cte-grain)")
QUERIES+=("select sr_store_sk, count(*) c, sum(sr_return_amt) t from store_returns group by sr_store_sk order by t desc limit 20")
LABELS+=("store grain (low-card groupby)")
QUERIES+=("select sr_customer_sk, sr_item_sk, sum(sr_return_amt) t from store_returns group by sr_customer_sk, sr_item_sk order by t desc limit 100")
LABELS+=("customer×item grain (HIGH-card — should be weak)")
QUERIES+=("select d_year, sr_store_sk, sum(sr_return_amt) t from store_returns, date_dim where sr_returned_date_sk=d_date_sk group by d_year, sr_store_sk order by d_year limit 50")
LABELS+=("year×store grain (join + low-card)")

# add TPC-DS 99 files if present
if [ -d "$Q_DIR" ]; then
  i=0
  for qf in $(ls "$Q_DIR"/q*.sql 2>/dev/null | sort -V); do
    [ "$i" -ge "$N" ] && break
    QUERIES+=("$(tr '\n' ' ' < "$qf")"); LABELS+=("$(basename "$qf" .sql)"); i=$((i+1))
  done
fi

echo "== archetype author-logic probe: cluster $CLUSTER_ID (resolve rank=true, no materialise) =="
printf '%-38s %-24s %-24s %s\n' QUERY PATH/SCHEME "TOP-3 (scheme:estRows:ratio)" WEAK
ranked=0; total=0
for idx in "${!QUERIES[@]}"; do
  total=$((total+1))
  R=$(curl -s -m 60 -X POST -H "$AUTH" -H "Content-Type: application/json" \
      -d "$(python3 -c 'import json,sys;print(json.dumps({"original_sql":sys.argv[1],"mode":"match","rank":True}))' "${QUERIES[$idx]}")" \
      "$GW/admin/mv/resolve")
  python3 - "$R" "${LABELS[$idx]}" <<'PYEOF'
import json,sys
try: d=json.loads(sys.argv[1])
except Exception: d={}
label=sys.argv[2][:38]
path=d.get("path","?"); scheme=d.get("scheme","-")
cands=d.get("archetype_candidates") or []
top3=" | ".join("%s:%s:%.3f"%(c.get("scheme","?"),c.get("est_rows","?"),c.get("ratio",0)) for c in cands[:3]) or "(ranking off / none)"
weak="WEAK→LLM" if d.get("weak_compression") else ""
print("%-38s %-24s %-24s %s"%(label, (path+"/"+scheme)[:24], top3[:60], weak))
PYEOF
  echo "$R" | grep -q '"archetype_candidates"' && ranked=$((ranked+1))
done
echo
echo "ranked $ranked/$total queries had a candidate set (0 ⇒ dimension-ranking is OFF on the gateway)"
echo "reading: est_rows = sampled+extrapolated MV row count · ratio = mv/source (1.0 ≈ no compression = weak)"
echo "the pick (scheme) is the lowest-est_rows candidate; weak ⇒ it defers to the LLM ladder."
