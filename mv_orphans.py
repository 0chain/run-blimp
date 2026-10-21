#!/usr/bin/env python3
"""Read-only orphan report for the node's MV store (run after a bench pass).

An ORPHAN is MV data no bank row names (a directory under <data>/<mv ns>/ with
no rag_authors.mv_table), or a bank row whose data is gone while its recipe is
kept. The bench purges each query's family once it is measured (phase 5), so a
clean pass leaves neither; anything listed here is what the reaper would have
to find later (its orphan sweep waits ZS3_MV_ORPHAN_GRACE, 24 h by default).
Nothing is deleted.
"""
import os, sqlite3, sys

data = os.environ.get("ZS3_DATA_DIR", "/opt/dlio/zs3data/zs3")
ns = os.environ.get("MV_NAMESPACE", "tpcds_mv")
db = os.path.join(data, "mv_rag.sqlite")
mvdir = os.path.join(data, ns)
dirs = set()
if os.path.isdir(mvdir):
    for d in os.listdir(mvdir):
        p = os.path.join(mvdir, d)
        if d.startswith("mv_") and os.path.isdir(p) and any(f.endswith(".parquet") for f in os.listdir(p)):
            dirs.add(d)
rows, covers = set(), set()
if os.path.exists(db):
    c = sqlite3.connect("file:%s?mode=ro" % db, uri=True)
    rows = {r[0] for r in c.execute("SELECT DISTINCT mv_table FROM rag_authors WHERE mv_table LIKE 'mv_%'")}
    try:
        cols = [r[1] for r in c.execute("PRAGMA table_info(rag_cover_keys)")]
        col = next((x for x in cols if "table" in x), None)
        if col:
            covers = {r[0].split(".")[-1] for r in c.execute(f"SELECT DISTINCT {col} FROM rag_cover_keys")}
    except sqlite3.Error:
        pass
# Partner units (mv_u_) and covers are local by design — the reaper skips them
# too (mv_reaper.go) — so they are reported apart, not as orphans.
units = {d for d in dirs if d.startswith("mv_u_")}
data_orphans = sorted(dirs - rows - covers - units)
recipe_only = sorted(rows - dirs)
size = 0
for d in data_orphans:
    for root, _, fs in os.walk(os.path.join(mvdir, d)):
        size += sum(os.path.getsize(os.path.join(root, f)) for f in fs)
print(f"   orphans: {len(data_orphans)} MV data dir(s) no bank row names ({size/1e9:.2f} GB); {len(recipe_only)} bank row(s) without local data; local by design: {len(units)} unit(s), {len(dirs & covers)} cover(s)")
for d in data_orphans[:20]:
    print(f"     data-orphan {ns}.{d}")
sys.exit(0)
