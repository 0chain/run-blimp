#!/usr/bin/env python3
"""Generate shape variants of the 99 TPC-DS queries for authoring/merge tests.

Each variant is one structural change to an official query (DuckDB's tpcds
extension = the official qgen text), so the gateway sees queries it has never
banked — new grains, new aggregates, new filters, new outer shapes — over the
same data. Every variant must EXECUTE on a local SF0.1 TPC-DS (dsdgen, seconds)
or it is dropped; a variant whose result is empty there is kept only when its
base query is empty too.

Transforms (applied to the outermost SELECT that has a GROUP BY, else the
outermost SELECT):
  years      shift every 4-digit year literal 1998..2002 by +1 (or -1 at 2002)
  agg        one sum(...) -> avg(...)/max(...)/count(...)
  coarser    drop one plain GROUP BY column (and its SELECT/ORDER BY uses)
  rollup     GROUP BY a, b -> GROUP BY ROLLUP (a, b)
  having     add HAVING count(*) > 1
  topn       LIMIT n -> a different n, first ORDER BY key direction flipped
  rank       add rank() OVER (ORDER BY <first aggregate> DESC) AS v_rank
  filter     AND d_moy <= 6 next to an unqualified d_year predicate
  cube       GROUP BY a, b (or ROLLUP) -> GROUP BY CUBE (a, b)
  distinct   one sum(x) -> count(DISTINCT x)   (non-additive measure)
  caseagg    one sum(x) -> sum(CASE WHEN x > 0 THEN x ELSE 0 END)
  between    widen a numeric BETWEEN a AND b to (a - w) AND (b + w)
  inlist     drop the last value of an IN list of >= 3 literals
  nolimit    drop the outermost LIMIT (the whole ordered result)
  combo      two transforms applied together (years+having, filter+agg, ...)

  dates      shift every DATE 'YYYY-MM-DD' literal by one year (TPC-H's spelling)

  gen_tpcds_variants.py --out tpcds_variants [--per-transform 15] [--seed 7]
  gen_tpcds_variants.py --bench tpch --out tpch_variants   (TPC-H: v_h<NN>_<t>.sql)
"""
import argparse, os, random, re, sys

import duckdb
import sqlglot
from sqlglot import exp

TRANSFORMS = ["years", "agg", "coarser", "rollup", "having", "topn", "rank", "filter",
              "cube", "distinct", "caseagg", "between", "inlist", "nolimit", "combo", "dates"]


def target_select(tree):
    sels = list(tree.find_all(exp.Select))
    grouped = [s for s in sels if s.args.get("group")]
    # outermost first: find_all is pre-order
    return grouped[0] if grouped else (sels[0] if sels else None)


def t_years(tree, rnd):
    hit = False
    for lit in tree.find_all(exp.Literal):
        if not lit.is_string and re.fullmatch(r"(1998|1999|2000|2001|2002)", lit.this or ""):
            y = int(lit.this)
            lit.set("this", str(y - 1 if y == 2002 else y + 1))
            hit = True
    return hit


def t_agg(tree, rnd):
    s = target_select(tree)
    sums = [e for e in (s.find_all(exp.Sum) if s else []) if e.find_ancestor(exp.Select) is s]
    if not sums:
        return False
    e = rnd.choice(sums)
    new = rnd.choice([exp.Avg, exp.Max, exp.Count])(this=e.this.copy())
    e.replace(new)
    return True


def _plain_group_cols(s):
    g = s.args.get("group")
    if not g:
        return []
    return [c for c in g.expressions if isinstance(c, exp.Column)]


def t_coarser(tree, rnd):
    s = target_select(tree)
    if not s:
        return False
    cols = _plain_group_cols(s)
    if len(cols) < 2:
        return False
    c = rnd.choice(cols)
    name = c.sql()
    c.pop()
    for proj in list(s.expressions):
        inner = proj.this if isinstance(proj, exp.Alias) else proj
        if isinstance(inner, exp.Column) and inner.sql() == name:
            proj.pop()
    o = s.args.get("order")
    if o:
        for oe in list(o.expressions):
            if isinstance(oe.this, exp.Column) and oe.this.sql() == name:
                oe.pop()
        if not o.expressions:
            s.set("order", None)
    return bool(s.expressions)


def t_rollup(tree, rnd):
    s = target_select(tree)
    if not s:
        return False
    g = s.args.get("group")
    cols = _plain_group_cols(s)
    if not g or len(cols) < 2 or len(cols) != len(g.expressions) or g.args.get("rollup") or g.args.get("cube"):
        return False
    s.set("group", exp.Group(rollup=[exp.Rollup(expressions=[c.copy() for c in cols])]))
    return True


def t_having(tree, rnd):
    s = target_select(tree)
    if not s or not s.args.get("group") or s.args.get("having"):
        return False
    s.set("having", exp.Having(this=sqlglot.parse_one("count(*) > 1")))
    return True


def t_topn(tree, rnd):
    lim = tree.args.get("limit")
    o = tree.args.get("order")
    if not lim or not o or not o.expressions:
        return False
    tree.set("limit", exp.Limit(expression=exp.Literal.number(rnd.choice([10, 25, 50, 200]))))
    first = o.expressions[0]
    first.set("desc", not first.args.get("desc"))
    return True


def t_rank(tree, rnd):
    s = target_select(tree)
    if not s or not s.args.get("group"):
        return False
    aggs = [p for p in s.expressions if (p.this if isinstance(p, exp.Alias) else p).find(exp.AggFunc)]
    if not aggs:
        return False
    a = aggs[0]
    key = (a.this if isinstance(a, exp.Alias) else a).copy()
    w = sqlglot.parse_one("rank() OVER (ORDER BY x DESC)")
    w.find(exp.Order).expressions[0].set("this", key)
    s.append("expressions", exp.alias_(w, "v_rank"))
    return True


def t_filter(tree, rnd):
    for w in tree.find_all(exp.Where):
        for eq in w.find_all(exp.EQ):
            col = eq.this
            if isinstance(col, exp.Column) and col.name.lower() == "d_year" and not col.table:
                w.set("this", exp.and_(w.this.copy(), sqlglot.parse_one("d_moy <= 6")))
                return True
    return False


def t_cube(tree, rnd):
    s = target_select(tree)
    if not s:
        return False
    g = s.args.get("group")
    if not g or g.args.get("cube"):
        return False
    roll = g.args.get("rollup")
    cols = [c.copy() for r in (roll or []) for c in r.expressions] if roll else [c.copy() for c in _plain_group_cols(s)]
    if len(cols) < 2 or (not roll and len(cols) != len(g.expressions)) or len(cols) > 4:
        return False
    s.set("group", exp.Group(cube=[exp.Cube(expressions=cols)]))
    return True


def _target_sums(tree):
    s = target_select(tree)
    return [e for e in (s.find_all(exp.Sum) if s else []) if e.find_ancestor(exp.Select) is s and not e.find_ancestor(exp.Window)]


def t_distinct(tree, rnd):
    sums = _target_sums(tree)
    if not sums:
        return False
    e = rnd.choice(sums)
    e.replace(exp.Count(this=exp.Distinct(expressions=[e.this.copy()])))
    return True


def t_caseagg(tree, rnd):
    sums = _target_sums(tree)
    if not sums:
        return False
    e = rnd.choice(sums)
    x = e.this.copy()
    case = exp.Case(ifs=[exp.If(this=exp.GT(this=x.copy(), expression=exp.Literal.number(0)), true=x)], default=exp.Literal.number(0))
    e.set("this", case)
    return True


def t_between(tree, rnd):
    bs = [b for b in tree.find_all(exp.Between)
          if isinstance(b.args.get("low"), exp.Literal) and isinstance(b.args.get("high"), exp.Literal)
          and not b.args["low"].is_string and not b.args["high"].is_string]
    if not bs:
        return False
    b = rnd.choice(bs)
    lo, hi = float(b.args["low"].this), float(b.args["high"].this)
    w = max(1, int((hi - lo) * 0.25)) if hi > lo else 1
    fmt = (lambda v: str(int(v))) if b.args["low"].this.lstrip("-").isdigit() and b.args["high"].this.lstrip("-").isdigit() else (lambda v: str(v))
    b.set("low", exp.Literal.number(fmt(lo - w)))
    b.set("high", exp.Literal.number(fmt(hi + w)))
    return True


def t_inlist(tree, rnd):
    ins = [i for i in tree.find_all(exp.In) if len(i.expressions) >= 3 and all(isinstance(x, exp.Literal) for x in i.expressions)]
    if not ins:
        return False
    i = rnd.choice(ins)
    i.set("expressions", [x.copy() for x in i.expressions[:-1]])
    return True


def t_dates(tree, rnd):
    hit = False
    for lit in tree.find_all(exp.Literal):
        m = lit.is_string and re.fullmatch(r"(19|20)(\d\d)-(\d\d)-(\d\d)", lit.this or "")
        if m and not (m.group(3) == "02" and m.group(4) == "29"):
            lit.set("this", f"{int(lit.this[:4]) + 1}{lit.this[4:]}")
            hit = True
    return hit


def t_nolimit(tree, rnd):
    if not tree.args.get("limit") or not tree.args.get("order"):
        return False
    tree.set("limit", None)
    return True


COMBOS = [("years", "having"), ("dates", "having"), ("dates", "inlist"), ("filter", "agg"), ("years", "topn"), ("rollup", "having"),
          ("coarser", "rank"), ("between", "having"), ("inlist", "caseagg"), ("filter", "coarser")]


def t_combo(tree, rnd):
    a, b = rnd.choice(COMBOS)
    return FN[a](tree, rnd) and FN[b](tree, rnd)


FN = {"years": t_years, "agg": t_agg, "coarser": t_coarser, "rollup": t_rollup,
      "having": t_having, "topn": t_topn, "rank": t_rank, "filter": t_filter,
      "cube": t_cube, "distinct": t_distinct, "caseagg": t_caseagg, "between": t_between,
      "inlist": t_inlist, "nolimit": t_nolimit, "combo": t_combo, "dates": t_dates}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--bench", default="tpcds", choices=["tpcds", "tpch"])
    ap.add_argument("--per-transform", type=int, default=15)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--timeout", type=float, default=20.0)
    ap.add_argument("--transforms", default="", help="comma list (default all)")
    ap.add_argument("--skip-existing", default="", help="dir of earlier variants: never re-emit a (query, transform) already there")
    a = ap.parse_args()
    rnd = random.Random(a.seed)
    con = duckdb.connect()
    if a.bench == "tpch":
        con.execute("INSTALL tpch; LOAD tpch; CALL dbgen(sf=0.1)")
        qs = con.execute("SELECT query_nr, query FROM tpch_queries()").fetchall()
        prefix = "v_h"
    else:
        con.execute("INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf=0.1)")
        qs = con.execute("SELECT query_nr, query FROM tpcds_queries()").fetchall()
        prefix = "v_q"
    base = {int(n): q.strip().rstrip(";") for n, q in qs}
    base_rows = {}

    def run(sql):
        con.execute("SET threads=4")
        return con.execute(sql).fetchall()

    os.makedirs(a.out, exist_ok=True)
    seen, kept = set(), {t: 0 for t in TRANSFORMS}
    for f in (os.listdir(a.skip_existing) if a.skip_existing and os.path.isdir(a.skip_existing) else []):
        seen.add(re.sub(r"\s+", " ", open(os.path.join(a.skip_existing, f)).read().strip()))
    order = list(base)
    todo = [x for x in a.transforms.split(",") if x] or TRANSFORMS
    prior = set(os.listdir(a.skip_existing)) if a.skip_existing and os.path.isdir(a.skip_existing) else set()
    for t in todo:
        rnd.shuffle(order)
        for n in order:
            if kept[t] >= a.per_transform:
                break
            try:
                tree = sqlglot.parse_one(base[n], read="duckdb")
            except Exception:
                continue
            if f"{prefix}{n:02d}_{t}.sql" in prior:
                continue
            if not FN[t](tree, rnd):
                continue
            sql = tree.sql(dialect="duckdb", pretty=True)
            norm = re.sub(r"\s+", " ", sql)
            if norm in seen:
                continue
            try:
                rows = run(sql)
            except Exception:
                continue
            if not rows:
                if n not in base_rows:
                    try:
                        base_rows[n] = len(run(base[n]))
                    except Exception:
                        base_rows[n] = -1
                if base_rows[n] != 0:
                    continue
            seen.add(norm)
            kept[t] += 1
            with open(os.path.join(a.out, f"{prefix}{n:02d}_{t}.sql"), "w") as f:
                # No leading comment: the gateway accepts only a statement that
                # starts with SELECT/WITH (the file name carries q and transform).
                f.write(f"{sql}\n")
    print(" ".join(f"{t}={kept[t]}" for t in todo), f"total={sum(kept[t] for t in todo)}")


if __name__ == "__main__":
    sys.exit(main())
