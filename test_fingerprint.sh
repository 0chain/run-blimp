#!/usr/bin/env bash
# test_fingerprint.sh — fingerprint/scheme-match robustness: generate N UNIQUE,
# structurally varied queries around the q1 archetype (customer_total_return
# CTE → per-store average filter) and measure the optimizer's hit ratio via the
# READ-ONLY resolver (POST /admin/mv/resolve — no authoring, no execution).
#
#   GATEWAY=<gw-ip> CLUSTER_ID=<id> [N=30] ./test_fingerprint.sh
#
# Variant classes (each query is unique — different constants AND structure):
#   A cosmetic   — whitespace/case/alias renames of the SAME semantics
#                  (must hit: exact-library or scheme)
#   B constants  — different year / threshold factor / state / agg column
#                  (same archetype grain → scheme-match expected)
#   C structural — extra predicates, different agg fn (mergeable), reordered
#                  joins, HAVING form (scheme-match if the grain still covers)
#   D novel      — different group-by grain / different fact table / window fn
#                  (MISS expected — these need fresh authoring)
# Report: hit ratio per class + overall, and the resolve path distribution
# (library | scheme | miss). A healthy optimizer: A≈100%, B high, C mixed
# (documented), D low — D hits would be FALSE POSITIVES (wrong results risk),
# so D "misses" are correct behavior, not failures.
set -u
: "${GATEWAY:?}" "${CLUSTER_ID:?}"
N="${N:-30}"
GW="http://$GATEWAY:9000"; TOKEN="zus-$CLUSTER_ID"; AUTH="Authorization: Bearer $TOKEN"

python3 - "$GW" "$TOKEN" "$N" <<'PYEOF'
import json, sys, urllib.request, random, itertools, collections
GW, TOKEN, N = sys.argv[1], sys.argv[2], int(sys.argv[3])
random.seed(42)  # reproducible query set

YEARS=[1998,1999,2000,2001,2002]; STATES=['TN','GA','SC','AL','KY','MO','NE','VA']
AGGS=['sr_return_amt','sr_fee','sr_return_tax','sr_refunded_cash']
FACTS=[1.1,1.15,1.2,1.25,1.3,1.4]

def q1(year=2000,fact=1.2,state='TN',agg='sr_return_amt',cte='customer_total_return',
       upper=False,extra='',having=False,grain=None,table='store_returns'):
    grain = grain or ['sr_customer_sk','sr_store_sk']
    gcols=', '.join(grain)
    sel_grain=', '.join('%s as g%d'%(c,i) for i,c in enumerate(grain))
    inner=(f"select {gcols.replace('sr_customer_sk','sr_customer_sk as ctr_customer_sk').replace('sr_store_sk','sr_store_sk as ctr_store_sk')}, "
           f"sum({agg}) as ctr_total from {table}, date_dim "
           f"where sr_returned_date_sk = d_date_sk and d_year = {year}{extra} "
           f"group by {gcols}")
    outer=(f"with {cte} as ({inner}) "
           f"select c_customer_id from {cte} ctr1, store, customer "
           f"where ctr1.ctr_total > (select avg(ctr_total)*{fact} from {cte} ctr2 "
           f"where ctr1.ctr_store_sk = ctr2.ctr_store_sk) "
           f"and s_store_sk = ctr1.ctr_store_sk and s_state = '{state}' "
           f"and ctr1.ctr_customer_sk = c_customer_sk order by c_customer_id limit 100")
    return outer.upper() if upper else outer

cases=[]  # (class, label, sql)
# A: cosmetic — same semantics, different text
base=q1()
cases.append(('A','exact text', base))
cases.append(('A','upper-cased', q1(upper=True)))
cases.append(('A','cte renamed', q1(cte='ctr_x')))
cases.append(('A','extra whitespace', base.replace(' ', '  ')))
# B: constants — same archetype, different literals/agg column
for year,fact,state,agg in itertools.islice(
        ((y,f,s,a) for y in YEARS for f in FACTS for s in STATES for a in AGGS), 0, 200):
    cases.append(('B', f'y{year} f{fact} {state} {agg}', q1(year=year,fact=fact,state=state,agg=agg)))
# C: structural — extra predicate / HAVING-ish variants (same grain)
cases.append(('C','extra store filter', q1(extra=' and sr_store_sk between 1 and 40')))
cases.append(('C','amount floor', q1(extra=' and sr_return_amt > 5')))
cases.append(('C','two-year range', q1().replace('d_year = 2000','d_year in (1999, 2000)')))
cases.append(('C','customer filter', q1(extra=' and sr_customer_sk < 5000000')))
# D: novel — different grain / table / window (must MISS: false-positive check)
cases.append(('D','item grain', q1(grain=['sr_item_sk','sr_store_sk'])))
cases.append(('D','web_returns fact', q1(table='web_returns').replace('sr_','wr_').replace('wr_store_sk','wr_web_page_sk')))
cases.append(('D','window fn', "select sr_store_sk, rank() over (order by sum(sr_return_amt) desc) rk from store_returns group by sr_store_sk limit 10"))
cases.append(('D','distinct count', "select sr_store_sk, count(distinct sr_customer_sk) c from store_returns group by sr_store_sk order by c desc limit 10"))

# subsample to N keeping all of A/C/D and filling with B
fixed=[c for c in cases if c[0]!='B']; bs=[c for c in cases if c[0]=='B']
random.shuffle(bs)
sel=fixed+bs[:max(0,N-len(fixed))]

def resolve(sql):
    body=json.dumps({"original_sql":sql,"mode":"match"}).encode()
    req=urllib.request.Request(GW+"/admin/mv/resolve", data=body, method='POST',
        headers={"Authorization":"Bearer "+TOKEN,"Content-Type":"application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.load(r)
    except Exception as e:
        return {"path":"error","err":str(e)[:60]}

# resolve path semantics (mv_resolve.go):
#   library_exact — REUSES an existing materialized MV (same fingerprint)
#   matcher       — archetype recognized; instantiates a NEW query-specific MV
#                   deterministically (no LLM call, correct-by-construction body)
#   llm/miss      — needs LLM authoring (or nothing matched)
stats=collections.defaultdict(collections.Counter)
paths=collections.Counter()
for cls,label,sql in sel:
    r=resolve(sql)
    p=r.get("path","error")
    kind={'library_exact':'reuse','matcher':'det-author','llm':'llm','miss':'miss'}.get(p,p)
    stats[cls][kind]+=1
    paths[p]+=1
    print("  [%s] %-28s -> %s"%(cls,label[:28],kind))

print()
print("== FINGERPRINT/SCHEME COVERAGE SUMMARY (n=%d unique queries) =="%len(sel))
for cls,desc in (('A','cosmetic variants'),('B','constant variants'),
                 ('C','structural variants'),('D','novel shapes')):
    c=stats[cls]; tot=sum(c.values())
    if tot:
        cov=c['reuse']+c['det-author']
        print("  %s %-22s no-LLM coverage %d/%d (%.0f%%)  [%s]"%(
            cls,desc,cov,tot,100*cov/tot,
            ' '.join('%s=%d'%(k,v) for k,v in sorted(c.items()))))
tot=sum(sum(c.values()) for c in stats.values())
cov=sum(c['reuse']+c['det-author'] for c in stats.values())
print("  overall no-LLM coverage: %d/%d (%.0f%%) · raw paths: %s"%(cov,tot,100*cov/tot,dict(paths)))
print("  reading: reuse = serves an EXISTING MV · det-author = archetype instantiates a")
print("  NEW correct MV without the LLM · miss/llm = would spend an LLM call (quota-gated).")
print("  A-class misses = fingerprint too text-literal (robustness bug).")
# gate: cosmetic variants MUST all resolve without an LLM; novel shapes MUST NOT
# reuse an existing MV (that would serve a wrong-grain result).
a=stats["A"]; acov=a["reuse"]+a["det-author"]; atot=sum(a.values())
d=stats["D"]; dreuse=d["reuse"]
fail=0
if atot and acov<atot:
    print("  GATE FAIL: A-class (cosmetic) coverage %d/%d < 100%% — fingerprint too literal"%(acov,atot)); fail=1
if dreuse>0:
    print("  GATE FAIL: %d novel (D-class) queries REUSED an existing MV (wrong-grain false positive)"%dreuse); fail=1
if not fail: print("  GATE PASS: A-class 100%% no-LLM, no D-class false positives")
sys.exit(fail)
PYEOF
