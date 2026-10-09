#!/usr/bin/env python3
"""Generate TPC-H parquet with DuckDB's tpch extension (the official dbgen,
embedded) into <out>/<table>/part-NNNN.parquet, one directory per table — the
layout `blimp --register --bucket <bucket>/<prefix>` discovers.

Large scale factors are generated in `--children` steps (dbgen's own
partitioning: each step is a disjoint slice of every table), so memory stays at
one slice (dbgen splits every table across the steps, nation/region too).

  gen_tpch.py --sf 10 --out /data/tpch/sf10
  gen_tpch.py --sf 1000 --children 200 --out /data/tpch/sf1000 [--steps 0-49]
Re-running skips steps whose files already exist (resume after an interrupt).
"""
import argparse, os, time

TABLES = ["lineitem", "orders", "partsupp", "part", "customer", "supplier", "nation", "region"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sf", type=float, required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--children", type=int, default=0,
                    help="dbgen steps (0 = one pass; default 1 step per 10 SF)")
    ap.add_argument("--steps", default="", help="subset 'a-b' of steps to run (default all)")
    ap.add_argument("--threads", type=int, default=0)
    ap.add_argument("--memory", default="", help="duckdb memory_limit, e.g. 32GB")
    a = ap.parse_args()
    import duckdb
    children = a.children or max(1, int(a.sf // 10))
    lo, hi = 0, children - 1
    if a.steps:
        lo, hi = (int(x) for x in a.steps.split("-"))
    for t in TABLES:
        os.makedirs(os.path.join(a.out, t), exist_ok=True)
    for step in range(lo, hi + 1):
        mark = os.path.join(a.out, f".step-{step:04d}.done")
        if os.path.exists(mark):
            continue
        t0 = time.time()
        con = duckdb.connect()
        con.execute("INSTALL tpch; LOAD tpch;")
        if a.threads:
            con.execute(f"SET threads={a.threads}")
        if a.memory:
            con.execute(f"SET memory_limit='{a.memory}'")
        if children == 1:
            con.execute(f"CALL dbgen(sf={a.sf})")
        else:
            con.execute(f"CALL dbgen(sf={a.sf}, children={children}, step={step})")
        for t in TABLES:
            n = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
            if n == 0:
                continue
            path = os.path.join(a.out, t, f"part-{step:04d}.parquet")
            con.execute(f"COPY {t} TO '{path}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1000000)")
        con.close()
        open(mark, "w").write(f"{time.time() - t0:.1f}s\n")
        print(f"step {step}/{children - 1} done in {time.time() - t0:.1f}s", flush=True)


if __name__ == "__main__":
    main()
