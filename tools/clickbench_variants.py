#!/usr/bin/env python3
"""Generate ClickBench query variants (clickbench_variants/v_qNN_<kind>.sql).

The TPC-DS bench runs each original beside variants that move one thing —
a measure, a filter, the grain, the cut — so a fix proven on the original is
also proven on the shapes around it. This does the same for ClickBench's 43
single-block queries (SELECT … FROM hits [WHERE] [GROUP BY] [HAVING]
[ORDER BY] [LIMIT [OFFSET]]), one rule per kind:

  agg       an extra additive measure        SUM(ResolutionWidth)
  caseagg   a conditional count               SUM(CASE WHEN IsRefresh = 0 …)
  distinct  a distinct count                  COUNT(DISTINCT UserID)
  having    a group threshold                 HAVING COUNT(*) > 10
  filter    one more predicate                AND IsRefresh = 0
  dates     a date window                     AND EventDate >= DATE '2013-07-10'
  coarser   the grain less its last column
  rollup    GROUP BY ROLLUP(<grain>)
  rank      the groups ranked by the cut's leading key, top 10 kept
  topn      a ten times larger LIMIT
  nolimit   the LIMIT dropped

A kind that does not apply to a query's shape is skipped. With --validate
<parquet glob>, each variant runs in DuckDB over a `hits` view of those files
(the loader's types: EventDate DATE, unsigned widened) and is kept only when it
runs and returns 1..--max-rows rows — a nolimit over 17M UserID groups is not
a query anyone serves.
"""
import argparse, glob, os, re, subprocess, sys

KW = ["select", "from", "where", "group by", "having", "order by", "limit"]


def top_level_positions(sql):
    """Lower-cased sql and, for each clause keyword, its depth-0 position."""
    low, depth, pos, i, inq = sql.lower(), 0, {}, 0, False
    while i < len(sql):
        c = sql[i]
        if c == "'":
            inq = not inq
        elif not inq and c == "(":
            depth += 1
        elif not inq and c == ")":
            depth -= 1
        elif not inq and depth == 0:
            for k in KW:
                if k not in pos and low.startswith(k, i) and (i == 0 or not low[i - 1].isalnum()) \
                        and (i + len(k) >= len(low) or not low[i + len(k)].isalnum() and low[i + len(k)] != "_"):
                    pos[k] = i
        i += 1
    return pos


def parse(sql):
    s = re.sub(r"\s+", " ", sql.strip().rstrip(";").strip())
    pos = top_level_positions(s)
    order = sorted(pos.items(), key=lambda kv: kv[1])
    parts = {}
    for n, (k, p) in enumerate(order):
        end = order[n + 1][1] if n + 1 < len(order) else len(s)
        parts[k] = s[p + len(k):end].strip()
    return parts


def split_commas(text):
    out, depth, cur, inq = [], 0, "", False
    for c in text:
        if c == "'":
            inq = not inq
        if not inq and c == "(":
            depth += 1
        if not inq and c == ")":
            depth -= 1
        if c == "," and depth == 0 and not inq:
            out.append(cur.strip()); cur = ""
        else:
            cur += c
    if cur.strip():
        out.append(cur.strip())
    return out


def render(p):
    s = "SELECT " + p["select"] + " FROM " + p["from"]
    for k, kw in (("where", "WHERE"), ("group by", "GROUP BY"), ("having", "HAVING"), ("order by", "ORDER BY"), ("limit", "LIMIT")):
        if p.get(k):
            s += f" {kw} " + p[k]
    return s + ";"


def variants(sql):
    p = parse(sql)
    if "select" not in p or not p.get("from", "").lower().startswith("hits"):
        return {}
    grouped = bool(p.get("group by"))
    grain = split_commas(p["group by"]) if grouped else []
    out = {}

    def v(kind, **kw):
        q = dict(p)
        q.update(kw)
        out[kind] = render(q)

    has_agg = bool(re.search(r"(?i)\b(count|sum|avg|min|max)\s*\(", p["select"]))
    if has_agg:
        v("agg", select=p["select"] + ", SUM(ResolutionWidth) AS sum_rw")
        v("caseagg", select=p["select"] + ", SUM(CASE WHEN IsRefresh = 0 THEN 1 ELSE 0 END) AS not_refresh")
        if not re.search(r"(?i)count\s*\(\s*distinct\s+userid", p["select"]):
            v("distinct", select=p["select"] + ", COUNT(DISTINCT UserID) AS users")
    where = p.get("where", "")
    # the original WHERE is parenthesized (an OR in it must not bind to the
    # added conjunct), and a conjunct it already implies is not a variant
    w = f"({where}) AND " if where else ""
    if not re.search(r"(?i)\bIsRefresh\s*=\s*0\b", where):
        v("filter", where=w + "IsRefresh = 0")
    if not re.search(r"(?i)\bEventDate\s*(>=|>|=|between)", where):
        v("dates", where=w + "EventDate >= DATE '2013-07-10'")
    if grouped and has_agg:
        hv = p.get("having", "")
        v("having", having=(hv + " AND " if hv else "") + "COUNT(*) > 10")
        if len(grain) >= 2 and not any(re.fullmatch(r"\d+", g) for g in grain):
            v("coarser", select=", ".join(i for i in split_commas(p["select"]) if not re.sub(r"\s+AS\s+\S+$", "", i, flags=re.I).strip() == grain[-1]
                                          and not re.search(r"(?i)\bAS\s+" + re.escape(grain[-1]) + r"$", i)),
              **{"group by": ", ".join(grain[:-1])})
            v("rollup", **{"group by": "ROLLUP(" + ", ".join(grain) + ")"})
        ob = p.get("order by", "")
        if ob:
            lead = split_commas(ob)[0]
            key = re.sub(r"(?i)\s+(asc|desc)\b.*$", "", lead).strip()
            desc = "DESC" if re.search(r"(?i)\bdesc\b", lead) else "ASC"
            inner = dict(p); inner.pop("order by", None); inner.pop("limit", None)
            body = render(inner).rstrip(";")
            # ranked OVER the grouped relation, so the key may be an output alias
            out["rank"] = (f"SELECT * FROM (SELECT g.*, RANK() OVER (ORDER BY {key} {desc}) AS rnk FROM ({body}) g) ranked "
                           f"WHERE rnk <= 10 ORDER BY rnk;")
    lim = p.get("limit", "")
    m = re.match(r"(\d+)(.*)$", lim)
    if m and p.get("order by"):
        v("topn", limit=str(int(m.group(1)) * 10) + m.group(2))
    if lim and grouped:
        q = dict(p); q.pop("limit")
        out["nolimit"] = render(q)
    return out


def validate(sql, files, max_rows, duckdb):
    view = ("CREATE VIEW hits AS SELECT * REPLACE (DATE '1970-01-01' + CAST(EventDate AS INTEGER) AS EventDate) "
            f"FROM read_parquet({files!r}, binary_as_string=true);")
    q = sql.rstrip(";")
    r = subprocess.run([duckdb, "-csv", "-noheader", "-c", view + f"SELECT count(*) FROM ({q}) t;"],
                       capture_output=True, text=True, timeout=600)
    if r.returncode != 0:
        return False, r.stderr.strip().splitlines()[-1][:160] if r.stderr.strip() else "error"
    n = int((r.stdout.strip().splitlines() or ["0"])[-1])
    if n < 1 or n > max_rows:
        return False, f"{n} rows"
    return True, f"{n} rows"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--queries", default=os.path.join(os.path.dirname(__file__), "..", "clickbench_queries"))
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "..", "clickbench_variants"))
    ap.add_argument("--validate", default="", help="parquet glob of raw hits files to run each variant over")
    ap.add_argument("--max-rows", type=int, default=200000)
    ap.add_argument("--recheck", default="", help="parquet glob a variant that returned 0 rows on --validate is re-run over (the full base) before it is dropped")
    ap.add_argument("--duckdb", default="duckdb")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    files = sorted(glob.glob(a.validate)) if a.validate else []
    kept = skipped = 0
    for f in sorted(glob.glob(os.path.join(a.queries, "q*.sql"))):
        name = os.path.basename(f)[:-4]
        for kind, sql in sorted(variants(open(f).read()).items()):
            if files:
                ok, why = validate(sql, files, a.max_rows, a.duckdb)
                if not ok and why == "0 rows" and a.recheck:
                    ok, why = validate(sql, sorted(glob.glob(a.recheck)), a.max_rows, a.duckdb)
                if not ok:
                    skipped += 1
                    print(f"skip v_{name}_{kind}: {why}", file=sys.stderr)
                    continue
            with open(os.path.join(a.out, f"v_{name}_{kind}.sql"), "w") as w:
                w.write(sql + "\n")
            kept += 1
    print(f"{kept} variant(s) written to {a.out}, {skipped} skipped")


if __name__ == "__main__":
    main()
