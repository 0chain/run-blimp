#!/usr/bin/env python3
"""Tell a tie at the LIMIT cut from a wrong answer — phase 4 (--verify) helper.

WHY THIS EXISTS
---------------
`ORDER BY k LIMIT n` is not deterministic when the n-th row sits inside a group
of rows tied on k: any n rows of that group are a correct answer. TPC-DS q59 at
SF1000: rows 99-100 come from 98 rows tied on (s_store_name, s_store_id,
d_week_seq); the served (MV) answer and the original query over base kept
different tied rows, so their md5s differed although both are correct.

THE RULE (all must hold, else the MISMATCH stands)
    - the query's outermost statement ends in ORDER BY ... LIMIT n, every ORDER BY
      item resolves to a result column (an ordinal or a bare output column name);
    - both results have exactly n rows (fewer = no cut, so any difference is real);
    - the last group (rows sharing the final ORDER BY key) has the same key and
      size on both sides and sits at the end of each result;
    - every row outside that group is identical on both sides.
Floats compare at 9 significant digits, as the gateway's md5_rounded does.

FLOAT NOISE FIRST. md5_rounded rounds every float to 9 significant digits, and a
value on a rounding boundary flips with its last bit: v_q57_agg's served and base
answers were the same 100 rows to 15 digits, yet 44.78453125 rounded to ...313 on
one side and ...312 on the other and phase 4 said MISMATCH (node 144, SF10,
2026-10-22). Before the tie rule, the two results are compared value by value:
same row count, same non-float fields, floats within a relative 1e-9. That
verdict prints "float: ..." and the bench reports MATCH(float).

Usage:
    verify_ties.py --sql "<query>" --served A.parquet --base B.parquet
    verify_ties.py --sql "<query>" --served-url <result_url> --base-url <result_url>
A result_url's path= names the parquet in the MV bucket (MV_BUCKET / MV_S3_* env,
as mv_delta_rows.py). Prints the verdict reason; exit 0 = MATCH(ties).
"""
import argparse, os, re, sys, time
from collections import Counter
from urllib.parse import parse_qs, urlparse


def strip_sql(sql):
    """The SQL with comments removed and string literals blanked, so keywords
    and parentheses inside them cannot confuse the scan."""
    out, i = [], 0
    while i < len(sql):
        if sql.startswith("--", i):
            j = sql.find("\n", i); i = len(sql) if j < 0 else j
        elif sql.startswith("/*", i):
            j = sql.find("*/", i + 2); i = len(sql) if j < 0 else j + 2
        elif sql[i] == "'":
            j = i + 1
            while j < len(sql) and not (sql[j] == "'" and not sql.startswith("''", j)):
                j += 2 if sql.startswith("''", j) else 1
            out.append("''"); i = j + 1
        else:
            out.append(sql[i]); i += 1
    return "".join(out)


def top_level(sql):
    """Mask of characters at parenthesis depth 0."""
    depth, mask = 0, []
    for c in sql:
        if c == "(": depth += 1
        mask.append(depth == 0)
        if c == ")": depth -= 1
    return mask


def order_by_limit(sql):
    """(ORDER BY items, n) of the outermost statement, or None."""
    s = strip_sql(sql).strip().rstrip(";").strip()
    mask = top_level(s)
    hits = [m for m in re.finditer(r"\border\s+by\b", s, re.I) if mask[m.start()]]
    if not hits:
        return None
    tail = s[hits[-1].end():]
    m = re.search(r"\blimit\s+(\d+)\s*$", tail, re.I)
    if not m or not all(mask[hits[-1].end() + m.start():]):
        return None
    body, tmask = tail[:m.start()], mask[hits[-1].end():hits[-1].end() + m.start()]
    items, cur = [], ""
    for c, top in zip(body, tmask):
        if c == "," and top:
            items.append(cur); cur = ""
        else:
            cur += c
    items.append(cur)
    return [it.strip() for it in items], int(m.group(1))


def select_projections(sql):
    """{normalized expression: output name} for the outermost SELECT list's
    items — `dt.d_year` (output d_year), `item.i_brand_id AS brand_id`."""
    s = strip_sql(sql).strip().rstrip(";").strip()
    mask = top_level(s)
    sel = [m for m in re.finditer(r"\bselect\b", s, re.I) if mask[m.start()]]
    frm = [m for m in re.finditer(r"\bfrom\b", s, re.I) if mask[m.start()]]
    if not sel or not frm:
        return {}
    start = sel[-1].end()
    ends = [m.start() for m in frm if m.start() > start]
    if not ends:
        return {}
    body, bmask = s[start:ends[0]], mask[start:ends[0]]
    items, cur = [], ""
    for c, top in zip(body, bmask):
        if c == "," and top:
            items.append(cur); cur = ""
        else:
            cur += c
    items.append(cur)
    out = {}
    for it in items:
        it = re.sub(r"^\s*distinct\s+", "", it.strip(), flags=re.I)
        m = re.fullmatch(r"(.+?)\s+(?:as\s+)?(\w+)", it, re.I | re.S)
        expr, name = (m.group(1), m.group(2)) if m and not re.fullmatch(r"\w+(\.\w+)?", it) else (it, it.split(".")[-1])
        out[re.sub(r"\s+", "", expr).lower()] = name.lower()
    return out


def resolve(items, columns, sql=""):
    """Result column indexes for the ORDER BY items, or None when any item is
    not an ordinal, an output column name, or an expression the SELECT list
    projects verbatim (an expression we cannot read)."""
    lower = [c.lower() for c in columns]
    proj = select_projections(sql) if sql else {}
    idx = []
    for it in items:
        e = re.sub(r"\s+nulls\s+(first|last)$", "", it, flags=re.I)
        e = re.sub(r"\s+(asc|desc)$", "", e, flags=re.I).strip()
        if re.fullmatch(r"\d+", e):
            k = int(e) - 1
            if not 0 <= k < len(columns):
                return None
        elif re.fullmatch(r'"[^"]+"|\w+', e):
            # A bare name is the output column (the engine binds ORDER BY names to
            # output aliases first); a qualified t.c may be a different column.
            name = e.strip('"').lower()
            if lower.count(name) != 1:
                return None
            k = lower.index(name)
        elif re.sub(r"\s+", "", e).lower() in proj:
            # A qualified name or expression the SELECT list projects as is
            # (`dt.d_year`, v_q03_cube) IS that output column — no other
            # column can be meant. Anything else stays unresolved.
            name = proj[re.sub(r"\s+", "", e).lower()]
            if lower.count(name) != 1:
                return None
            k = lower.index(name)
        else:
            return None
        idx.append(k)
    return idx


def norm(v):
    return float("%.9g" % v) if isinstance(v, float) else v


def read_raw(path, fs=None):
    import pyarrow.parquet as pq
    t = pq.read_table(fs.open(path, "rb") if fs else path)
    return t.column_names, [tuple(r.values()) for r in t.to_pylist()]


def read_rows(path, fs=None):
    cols, rows = read_raw(path, fs)
    return cols, [tuple(norm(v) for v in r) for r in rows]


def close(x, y, rel=1e-9):
    if isinstance(x, float) and isinstance(y, float):
        return x == y or abs(x - y) <= rel * max(abs(x), abs(y), 1.0)
    return x == y


def float_verdict(served, base):
    """(ok, reason): the same rows, floats equal to a relative 1e-9. Rows are
    paired in result order first, then (an order the float noise moved within
    equal keys) after sorting both sides on their 6-significant-digit image."""
    (cs, a), (cb, b) = served, base
    if [c.lower() for c in cs] != [c.lower() for c in cb] or len(a) != len(b):
        return False, "float: different columns or row counts"
    same = lambda xs, ys: all(len(r) == len(s) and all(close(u, v) for u, v in zip(r, s)) for r, s in zip(xs, ys))
    if same(a, b):
        return True, "float: %d rows equal within 1e-9 (relative) in result order" % len(a)
    img = lambda r: tuple(("%.6g" % v) if isinstance(v, float) else ("" if v is None else str(v)) for v in r)
    if same(sorted(a, key=img), sorted(b, key=img)):
        return True, "float: %d rows equal within 1e-9 (relative) as a multiset" % len(a)
    return False, "float: values differ beyond 1e-9"


def tie_verdict(sql, served, base):
    """(ok, reason). served/base are (column_names, rows) in result order."""
    ob = order_by_limit(sql)
    if not ob:
        return False, "no ORDER BY ... LIMIT on the outermost statement"
    items, n = ob
    (cs, a), (cb, b) = served, base
    if [c.lower() for c in cs] != [c.lower() for c in cb]:
        return False, "different result columns"
    idx = resolve(items, cs, sql)
    if idx is None:
        return False, "ORDER BY items %s do not all resolve to result columns" % items
    if not (len(a) == len(b) == n):
        return False, "rows served=%d base=%d limit=%d (no cut at the limit)" % (len(a), len(b), n)
    key = lambda r: tuple(r[k] for k in idx)
    last = key(b[-1])
    if key(a[-1]) != last:
        return False, "the last ORDER BY key differs: served=%s base=%s" % (key(a[-1]), last)
    ga = [r for r in a if key(r) == last]; gb = [r for r in b if key(r) == last]
    if len(ga) != len(gb) or a[-len(ga):] != ga or b[-len(gb):] != gb:
        return False, "the last tied group is not the same size at the end of both results"
    if Counter(a[:-len(ga)]) != Counter(b[:-len(gb)]):
        return False, "rows outside the last tied group differ"
    return True, "only the last tied group (%d rows, key %s) differs" % (len(gb), last)


def url_path(url):
    """The bucket key a result_url's viewer link names (path=...)."""
    return (parse_qs(urlparse(url).query).get("path") or [""])[0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sql", required=True)
    ap.add_argument("--served", default=""); ap.add_argument("--base", default="")
    ap.add_argument("--served-url", default=""); ap.add_argument("--base-url", default="")
    a = ap.parse_args()
    fs, served, base = None, a.served, a.base
    if not (served and base):
        import s3fs
        bucket = os.environ.get("MV_BUCKET", "")
        ck = {"region_name": os.environ.get("AWS_REGION", "us-east-1")}
        if os.environ.get("MV_S3_ENDPOINT"):
            ck["endpoint_url"] = os.environ["MV_S3_ENDPOINT"]
        fs = s3fs.S3FileSystem(key=os.environ.get("MV_S3_KEY") or None,
                               secret=os.environ.get("MV_S3_SECRET") or None, client_kwargs=ck)
        sp, bp = url_path(a.served_url), url_path(a.base_url)
        if not (bucket and sp and bp) or sp == bp:
            print("result parquets not addressable (served=%r base=%r)" % (sp, bp)); sys.exit(1)
        served, base = "%s/%s" % (bucket, sp), "%s/%s" % (bucket, bp)
        # The gateway uploads the base answer just after it responds.
        for _ in range(12):
            fs.invalidate_cache()
            if fs.exists(base) and fs.exists(served):
                break
            time.sleep(5)
    try:
        ok, why = float_verdict(read_raw(served, fs), read_raw(base, fs))
        if not ok:
            ok, why = tie_verdict(a.sql, read_rows(served, fs), read_rows(base, fs))
    except Exception as e:
        ok, why = False, "unreadable result parquet: %s" % e
    print(why)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
