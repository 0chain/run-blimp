#!/usr/bin/env python3
"""query_tables.py — which catalog tables a SQL file reads, and which of them is
the FACT, derived from the query text and the Iceberg REST catalog. Nothing
about the dataset is assumed: names come from the SQL, existence from the
catalog listing, "fact" from the catalog's row counts (the biggest referenced
table), the way the gateway itself decides.

  python3 query_tables.py --sql-file q.sql [--sql-file ...] \
      --catalog http://GW:19122/iceberg [--prefix main] [--warehouse mv] \
      --namespace tpcds [--s3-key K --s3-secret S]

Prints one JSON object per SQL file: {"file","tables":[...],"fact":"...",
"rows":{table:n}}. --list-refs prints the raw identifiers found in the SQL and
exits (no catalog needed — used by the offline tests). Standard library only.
"""
import argparse, json, re, sys, urllib.parse, urllib.request

STOP = r"(?:where|join|left|right|full|inner|cross|natural|outer|group|order|limit|having|union|except|intersect|on|using|window|qualify|fetch|with)"
FROM_RE = re.compile(r"\b(from|join)\s+(.*?)(?=\b" + STOP + r"\b|[;()]|$)", re.I | re.S)
IDENT_RE = re.compile(r"^\s*([A-Za-z_][\w$]*(?:\.[A-Za-z_][\w$]*){0,2})")
KEYWORDS = {"select", "lateral", "unnest", "values", "generate_series", "read_parquet", "iceberg_scan"}

def sql_refs(sql):
    """Table names in FROM/JOIN position — every item of a comma list, subqueries
    skipped — minus CTE names defined in the query and function-like keywords.
    Lower-cased, last path component only, first occurrence order."""
    body = re.sub(r"--[^\n]*", " ", sql)
    body = re.sub(r"/\*.*?\*/", " ", body, flags=re.S)
    ctes = {m.group(1).lower() for m in re.finditer(r"\b([A-Za-z_]\w*)\s+as\s*\(", body, re.I)}
    out = []
    for m in FROM_RE.finditer(body):
        items = m.group(2).split(",") if m.group(1).lower() == "from" else [m.group(2)]
        for it in items:
            im = IDENT_RE.match(it)
            if not im:
                continue
            last = im.group(1).lower().split(".")[-1]
            if last in KEYWORDS or last in ctes or last in out:
                continue
            out.append(last)
    return out

def rest(url, token=""):
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    if token:
        req.add_header("Authorization", "Bearer " + token)
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.load(r)

def catalog_base(catalog, prefix, warehouse):
    """Resolve the REST prefix the way a client must: /v1/config[?warehouse=]. An
    explicit --prefix wins; a catalog that answers no prefix is served at the root."""
    catalog = catalog.rstrip("/")
    if prefix is None:
        q = "?warehouse=" + urllib.parse.quote(warehouse) if warehouse else ""
        try:
            cfg = rest(catalog + "/v1/config" + q)
            prefix = (cfg.get("overrides") or {}).get("prefix") or (cfg.get("defaults") or {}).get("prefix") or ""
            prefix = urllib.parse.unquote(prefix)
        except Exception:
            prefix = ""
    p = ("/" + urllib.parse.quote(prefix, safe="")) if prefix else ""
    return catalog + "/v1" + p

def catalog_tables(base, ns):
    doc = rest(base + "/namespaces/" + urllib.parse.quote(ns, safe="") + "/tables")
    return {t["name"].lower() for t in doc.get("identifiers", [])}

def table_rows(base, ns, table):
    doc = rest(base + "/namespaces/" + urllib.parse.quote(ns, safe="") + "/tables/" + urllib.parse.quote(table, safe=""))
    md = doc.get("metadata") or {}
    cur = md.get("current-snapshot-id")
    for s in md.get("snapshots") or []:
        if s.get("snapshot-id") == cur:
            try:
                return int((s.get("summary") or {}).get("total-records") or 0)
            except ValueError:
                return 0
    return 0

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sql-file", action="append", required=True)
    ap.add_argument("--catalog"); ap.add_argument("--prefix", default=None); ap.add_argument("--warehouse", default="")
    ap.add_argument("--namespace", default="")
    ap.add_argument("--list-refs", action="store_true", help="print the SQL's table references and exit")
    a = ap.parse_args()
    if a.list_refs:
        for f in a.sql_file:
            print(json.dumps({"file": f, "refs": sql_refs(open(f).read())}))
        return 0
    if not a.catalog or not a.namespace:
        print("--catalog and --namespace are required (or --list-refs)", file=sys.stderr); return 2
    base = catalog_base(a.catalog, a.prefix, a.warehouse)
    known = catalog_tables(base, a.namespace)
    rows_cache = {}
    for f in a.sql_file:
        refs = [t for t in sql_refs(open(f).read()) if t in known]
        for t in refs:
            if t not in rows_cache:
                try: rows_cache[t] = table_rows(base, a.namespace, t)
                except Exception: rows_cache[t] = 0
        fact = max(refs, key=lambda t: rows_cache.get(t, 0)) if refs else ""
        print(json.dumps({"file": f, "tables": refs, "fact": fact, "rows": {t: rows_cache[t] for t in refs}}))
    return 0

if __name__ == "__main__":
    sys.exit(main())
