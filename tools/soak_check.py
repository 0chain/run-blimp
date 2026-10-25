#!/usr/bin/env python3
"""soak_check.py <run_dir> [--ratio 1.25] [--floor 0.5]

Growth soak verdict. A soak run appends without rolling back
(SKIP_ROLLBACK=1 TPC_BATCH_TICKS=N via tools/node/cang.sh), so the source
tables grow tick over tick the way a streaming datalake does. Merges are
O(delta): a query's tick time must stay flat as the tables grow. For every
<q>_1.log in run_dir this reads each "tick: X seconds … MV: R rows x C cols"
line and compares the median of the last third of the ticks to the first
third. FAIL when last/first > ratio and the last median is above floor
seconds (sub-floor noise is not growth). The MV row trend is printed beside
it: a tick that grows with its MV is O(MV) work hiding in the tick.
Exit status 1 when any query fails.
"""
import glob, os, re, statistics, sys

RE = re.compile(r"tick: ([0-9.]+) seconds.*?MV: (\S+) rows x (\S+) cols")


def main():
    args = sys.argv[1:]
    if not args:
        sys.exit(__doc__)
    run_dir = args[0]
    ratio = float(args[args.index("--ratio") + 1]) if "--ratio" in args else 1.25
    floor = float(args[args.index("--floor") + 1]) if "--floor" in args else 0.5
    bad = 0
    for log in sorted(glob.glob(os.path.join(run_dir, "*_1.log"))):
        q = os.path.basename(log)[:-6]
        ticks = [(float(m.group(1)), int(m.group(2)) if m.group(2).isdigit() else -1) for m in RE.finditer(open(log, errors="replace").read())]
        if len(ticks) < 6:
            print(f"{q:28s} SKIP  {len(ticks)} tick(s) — a soak needs >= 6")
            continue
        k = max(2, len(ticks) // 3)
        first = statistics.median(t for t, _ in ticks[:k])
        last = statistics.median(t for t, _ in ticks[-k:])
        r = last / first if first > 0 else float("inf")
        rows0, rows1 = ticks[0][1], ticks[-1][1]
        verdict = "FAIL" if r > ratio and last > floor else "ok"
        bad += verdict == "FAIL"
        print(f"{q:28s} {verdict:4s}  ticks={len(ticks)} first={first:.2f}s last={last:.2f}s x{r:.2f}  MV rows {rows0}->{rows1}")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
