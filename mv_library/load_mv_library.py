#!/usr/bin/env python3
"""load_mv_library.py — seed a Blimp cluster's MV book from a shipped recipe library.

WHY THIS EXISTS. A verified MV recipe is expensive: an LLM call (billable), a
materialize over the whole fact, and a row-hash verify against the original
query. Until now that work lived only in one cluster's `/data/zs3/mv_rag.sqlite`,
so a fresh cluster re-earned all of it — and a cluster that was rebuilt lost it.
`tpcds.json` is that work, exported. This script puts it back.

WHAT A RECIPE IS. Four artifacts that must travel together:

  original_sql  the query the recipe answers
  mv_sql        the MV body (what gets materialized)
  rewrite_sql   the original, rewritten to read the MV instead of the facts
  delta_sql     the body with every fact reference in FROM/JOIN position wrapped
                $DELTA(<fact>) — this is what makes a refresh O(delta) instead of
                a full re-aggregate

A recipe missing `delta_sql` still serves, but every source append costs a full
rebuild. The library only ships recipes that have all four AND a row-hash from a
verify that passed.

WHAT TRANSFERS ACROSS DATASETS, AND WHAT DOES NOT. Be clear-eyed here: the SQL
in `tpcds.json` is TPC-DS SQL. It will not answer questions about your orders
table. What is dataset-independent is everything around it — the four-artifact
contract, the $DELTA slot convention, the one-fact-per-branch rule, and this
loader. To bootstrap a different schema you author its own library once and ship
it as `<dataset>.json` beside this one; the pipeline is identical.

THE ONE-FACT-PER-BRANCH RULE, measured. A merge is only O(delta) when each
UNION branch of `delta_sql` reads exactly ONE fact. When a branch joins k facts,
the binder must expand it into 2^k-1 inclusion-exclusion terms, and the terms
where a fact is UNCHANGED bind that fact to its FULL table — so the "delta"
merge full-scans. Measured on AWS SF1 (2026-08-04): every merge under 250 ms had
one fact per branch; every merge over 500 ms had two or three. At SF1000 the same
shape cost 15.2 s and scanned ~19.7 GB. `--check` flags these.

USAGE

    # what would be loaded, and which recipes look expensive to merge
    python3 load_mv_library.py --check

    # load into a cluster (idempotent; re-running skips recipes already banked)
    python3 load_mv_library.py --gateway http://localhost:9000 --token "$ZS3_ADMIN_TOKEN"

    # only a subset
    python3 load_mv_library.py --gateway ... --token ... --labels q9,q44,q88

Recipes are registered by REPLAYING them: the loader posts each `original_sql`
to /admin/query/run so the gateway authors it through its own normal path with
the library recipe as the seed. That is deliberate — it means the cluster's own
verifier decides whether the recipe is correct HERE, against THIS data, rather
than trusting a hash computed somewhere else. A recipe that does not verify on
your data is dropped, loudly, instead of silently serving wrong answers.
"""
import argparse, json, os, re, sys, time, urllib.request, urllib.error

HERE = os.path.dirname(os.path.abspath(__file__))
RE_DELTA = re.compile(r'(?i)\$DELTA\(\s*([A-Za-z_][\w.]*)\s*\)')


def load_library(path):
    with open(path) as f:
        return json.load(f)


# Only these are FACT tables. A $DELTA() around a dimension is always a defect:
# dimensions barely change, and slotting one adds an inclusion-exclusion term for
# nothing. Three library recipes carry them today — q24 slots customer,
# customer_address, item and store; q40 slots warehouse; q34 slots date_dim.
FACT_TABLES = frozenset((
    "store_sales", "store_returns", "catalog_sales", "catalog_returns",
    "web_sales", "web_returns", "inventory",
))


def _split_top_level(sql, pattern):
    """Split on `pattern` only at paren depth 0."""
    parts, depth, cur = [], 0, []
    for tok in re.split(r'(\(|\)|' + pattern + r')', sql, flags=re.I):
        if tok == '(':
            depth += 1
            cur.append(tok)
        elif tok == ')':
            depth -= 1
            cur.append(tok)
        elif tok and re.fullmatch(pattern, tok, re.I) and depth == 0:
            parts.append(''.join(cur))
            cur = []
        else:
            cur.append(tok or '')
    parts.append(''.join(cur))
    return [p for p in parts if p.strip()]


RE_CREATE_AS = re.compile(
    r'(?is)^\s*create\s+(?:or\s+replace\s+)?(?:temp(?:orary)?\s+)?(?:table|view)\s+'
    r'(?:if\s+not\s+exists\s+)?[^\s(]+\s+as\s*')


def unwrap_statement(sql):
    """Shed `CREATE ... AS` and a wrapping paren pair, repeatedly.

    Without this the UNION ALL inside `CREATE TABLE x AS ( SELECT ... UNION ALL
    SELECT ... )` is at paren depth 1, so top-level splitting finds no branches
    at all and reports one branch holding every fact. q5 ships exactly that
    shape and was mis-audited as 2 facts/branch when it is 6 branches of 1.
    """
    s = sql.strip()
    while True:
        before = s
        s = RE_CREATE_AS.sub('', s).strip()
        if len(s) > 1 and s[0] == '(' and s[-1] == ')':
            depth, spans_whole = 0, True
            for i, ch in enumerate(s):
                if ch == '(':
                    depth += 1
                elif ch == ')':
                    depth -= 1
                    if depth == 0 and i < len(s) - 1:
                        spans_whole = False
                        break
            if spans_whole:
                s = s[1:-1].strip()
        if s == before:
            return s


def _branches_of(stmt):
    """Top-level UNION branches, falling back to a NESTED union.

    Mirrors the gateway's splitTopLevelUnionAll -> splitNestedUnionAll chain.
    A body shaped `SELECT ... FROM ( SELECT ...a UNION ALL SELECT ...b ) GROUP BY`
    has no top-level UNION at all, but it is still two single-fact branches and
    the merge treats it as such. q5 ships exactly this — three channel MVs, each
    a derived-table union of a sales leg and a returns leg. Reading only the top
    level reports one branch holding both facts and wrongly condemns it.
    """
    top = _split_top_level(stmt, r'\bUNION\s+ALL\b|\bUNION\b')
    if len(top) > 1:
        return top
    # Descend into each parenthesised group and take the first that unions.
    best = top
    for m in re.finditer(r'\(', stmt):
        depth, start = 0, m.start()
        for i in range(start, len(stmt)):
            if stmt[i] == '(':
                depth += 1
            elif stmt[i] == ')':
                depth -= 1
                if depth == 0:
                    inner = stmt[start + 1:i]
                    parts = _split_top_level(inner, r'\bUNION\s+ALL\b|\bUNION\b')
                    if len(parts) > len(best):
                        best = parts
                    break
    return best


def fact_refs_per_branch(delta_sql):
    """ALL fact tables referenced per branch — slotted or not.

    The cost signal is not the slot count, it is the number of facts a branch
    touches. A branch may carry ONE $DELTA slot and still join two other facts
    at full size; the merge then reads them in full on every append. q17 is the
    case that exposed this: 3 branches, 1 slot each — "clean" by slot count —
    but every branch joins all three of store_sales, store_returns and
    catalog_sales, which is why its delta measured 3.9x SLOWER than a rebuild
    (945 ms vs 240 ms at SF1).
    """
    if not delta_sql:
        return []
    body = re.sub(r'--[^\n]*', '', delta_sql)
    out = []
    for stmt in _split_top_level(body, r';'):
        stmt = unwrap_statement(stmt)
        if '$delta(' not in stmt.lower():
            continue
        for branch in _branches_of(stmt):
            bare = RE_DELTA.sub(lambda m: m.group(1), branch)
            refs = {t for t in FACT_TABLES
                    if re.search(r'\b' + re.escape(t) + r'\b', bare, re.I)}
            out.append(sorted(refs))
    return out


def facts_per_branch(delta_sql):
    """Fact slots in each top-level UNION [ALL] branch, per statement.

    Three things this must get right, each of which it previously got wrong:

    1. COUNT OCCURRENCES, NOT DISTINCT NAMES. The old set comprehension folded a
       SELF-JOIN — the same fact slotted twice in one branch — down to a single
       entry, so it passed the one-fact-per-branch check. That is a correctness
       hole, not a cost one: the merge binds both occurrences to the SAME delta
       files, computing delta><delta while dropping base><delta and delta><base.
       Measured on q95 at SF1 (2026-08-04): 60,000 qualifying orders in truth,
       59,999 from base><base, and 11,164 from delta><delta.

    2. SPLIT ON UNION AT PAREN DEPTH 0. Splitting on every `UNION ALL` also cut
       inside subqueries and derived tables, inventing branches that do not
       exist.

    3. SPLIT STATEMENTS FIRST. A recipe file may hold one CREATE ... AS per MV
       (q5 ships three, one per channel), and treating the whole file as a
       single statement reported 1 branch with 6 facts where the truth is 6
       branches with 1 fact each.

    Dimensions are not counted — only FACT_TABLES.
    """
    if not delta_sql:
        return []
    body = re.sub(r'--[^\n]*', '', delta_sql)
    out = []
    for stmt in _split_top_level(body, r';'):
        stmt = unwrap_statement(stmt)
        if '$delta(' not in stmt.lower():
            continue
        for branch in _branches_of(stmt):
            out.append(sorted(t.lower() for t in RE_DELTA.findall(branch)
                              if t.lower() in FACT_TABLES))
    return out


def dimension_slots(delta_sql):
    """Non-fact tables wrapped in $DELTA() — always a defect (see FACT_TABLES)."""
    if not delta_sql:
        return []
    return sorted({t.lower() for t in RE_DELTA.findall(delta_sql)
                   if t.lower() not in FACT_TABLES})


def self_joined_slot(delta_sql):
    """First fact slotted more than once in a single branch, or None."""
    for branch in facts_per_branch(delta_sql):
        seen = set()
        for t in branch:
            if t in seen:
                return t
            seen.add(t)
    return None


def audit(rec):
    """Return (ok, notes) — structural checks that need no cluster."""
    notes = []
    for field in ("original_sql", "mv_sql", "rewrite_sql"):
        if not rec.get(field):
            notes.append(f"MISSING {field}")
    delta = rec.get("delta_sql", "")
    if not delta:
        # NOT a skip. Per this file's own contract a recipe without a delta still
        # serves; it just costs a full rebuild per append. Some shapes have no
        # sound delta at all and shipping the cover alone is the right answer —
        # q72 is the worked example: its only correct delta needs a SNAPSHOT of
        # the unchanged fact as of the previous build, which the harness does not
        # provide, and even with one the 3-term expansion measured 4.97 s against
        # 3.14 s to rebuild from scratch.
        notes.append("no delta — serves fine, but every source append costs a "
                     "FULL rebuild of this MV")
    branches = facts_per_branch(delta)
    worst = max((len(b) for b in branches), default=0)
    if dup := self_joined_slot(delta):
        # Blocking: this one is silently WRONG, not merely slow.
        notes.append(f"MISSING sound delta — a branch slots {dup} twice (self-join); "
                     "binding both to the same delta drops the base><delta cross-terms")
    if dims := dimension_slots(delta):
        notes.append(f"slots dimension(s) {', '.join(dims)} — dimensions barely change; "
                     "each one adds an expansion term for nothing")
    if worst >= 2:
        notes.append(f"multi-fact branch ({worst} slots, up to {2**worst - 1} expansion "
                     "terms) — merge will full-scan the unchanged facts, not O(delta)")
    # Unslotted facts are scanned at FULL size by every term of the branch that
    # references them. A branch can be "1 slot" and still be far worse than a
    # rebuild (q17: 3 branches x 1 slot, but 3 facts joined per branch).
    unslotted = max((len(refs) - len(slots) for refs, slots
                     in zip(fact_refs_per_branch(delta), branches)), default=0)
    if unslotted > 0:
        notes.append(f"{unslotted} unslotted fact(s) joined in a branch — scanned at FULL "
                     "size on every append, regardless of delta size")
    if worst == 0 and delta:
        notes.append("delta has no $DELTA() slot — every append re-scans every fact")
    return (not any(n.startswith("MISSING") for n in notes)), notes, worst


def post(gateway, token, path, payload, timeout=1800):
    req = urllib.request.Request(
        gateway.rstrip("/") + path,
        data=json.dumps(payload).encode(),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    try:
        return json.loads(urllib.request.urlopen(req, timeout=timeout).read())
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read())
        except Exception:
            return {"status": "http_error", "code": e.code}
    except Exception as e:
        return {"status": "error", "err": str(e)}


def merge_mode_of(resp):
    """Pull the refresh mode + merge cost out of a run's author trace."""
    mode, ms = "", None
    for s in (resp.get("author_trace") or []):
        if not isinstance(s, dict):
            continue
        ph, det = s.get("phase", ""), str(s.get("detail", ""))
        if ph == "merge_mode":
            mode = "append" if det.startswith("append (") else det[:40]
        if ph == "materialize_append_delta":
            ms = s.get("ms")
        if ph == "materialize_ctas" and not mode:
            mode, ms = "rebaseline", s.get("ms")
    return mode, ms


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--library", default=os.path.join(HERE, "tpcds.json"))
    ap.add_argument("--gateway", default="", help="e.g. http://localhost:9000")
    ap.add_argument("--token", default=os.environ.get("ZS3_ADMIN_TOKEN", ""))
    ap.add_argument("--labels", default="", help="comma-separated subset, e.g. q9,q44")
    ap.add_argument("--check", action="store_true", help="audit only; contact no cluster")
    ap.add_argument("--source", default="customer")
    a = ap.parse_args()

    lib = load_library(a.library)
    recipes = lib["recipes"]
    if a.labels:
        want = {x.strip() for x in a.labels.split(",") if x.strip()}
        recipes = [r for r in recipes if want & set(r.get("labels") or [])]

    print(f"library: {a.library}")
    print(f"dataset: {lib.get('dataset')}  version: {lib.get('version')}  recipes: {len(recipes)}")

    if a.check or not a.gateway:
        clean = slow = broken = 0
        for r in recipes:
            ok, notes, worst = audit(r)
            tag = ",".join(r.get("labels") or []) or r["mv_table"]
            if not ok:
                broken += 1
                print(f"  BROKEN  {tag:14} {r['mv_table'][:34]:36} {'; '.join(notes)}")
            elif worst >= 2:
                slow += 1
                print(f"  SLOW    {tag:14} {r['mv_table'][:34]:36} {'; '.join(notes)}")
            else:
                clean += 1
        print(f"\n  {clean} O(delta)-clean, {slow} multi-fact (merge full-scans), {broken} incomplete")
        if not a.gateway:
            print("\n(no --gateway given, so nothing was loaded)")
        return

    if not a.token:
        sys.exit("--token or ZS3_ADMIN_TOKEN required to load")

    loaded = failed = 0
    for r in recipes:
        ok, notes, _ = audit(r)
        tag = ",".join(r.get("labels") or []) or r["mv_table"]
        if not ok:
            print(f"  skip {tag}: {'; '.join(notes)}")
            continue
        t0 = time.time()
        resp = post(a.gateway, a.token, "/admin/query/run", {
            "original_sql": r["original_sql"],
            "label": f"{tag}:libload",
            "source": a.source,
            "skip_verify": False,   # THIS cluster's verifier decides, not the exporter's hash
        })
        mode, ms = merge_mode_of(resp)
        status = resp.get("status", "?")
        if status == "ok":
            loaded += 1
            print(f"  ok   {tag:14} {time.time()-t0:6.1f}s mode={mode or '-'} merge_ms={ms or '-'}")
        else:
            failed += 1
            print(f"  FAIL {tag:14} status={status} {str(resp.get('err',''))[:70]}")

    print(f"\nloaded {loaded}, failed {failed}, of {len(recipes)}")


if __name__ == "__main__":
    main()
