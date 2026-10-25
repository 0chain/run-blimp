# tools/node — the bench drivers that run on each bench node

Versioned here so every node runs the same reviewed copy (they drifted while
they lived only on the nodes: 65's `cang.sh` lacked the `EVICT_ALL` override).
Install on a node: copy each file to `/root/` (the paths the scripts assume), the
`*.py` helpers to `/root/bench_tools/`.

- `cang.sh <env> <irc|-> <s3e|-> <sha|latest> <LABEL> "<list>"` — one cold run under a
  node lock (waits for any running `tpc_batch`); `CANG_TAG=latest` after
  `blimp --update zs3 --tag zs3=latest` (REQUIRED first — the launcher only checks the image name contains "latest"); `EVICT_ALL=1` for a first pass on a new namespace.
- `canary_node.sh` — what `cang.sh` runs: update/wire the gateway, evict, roll the
  sources back to `bench_baseline`, expire abandoned snapshots, run `tpc_batch`.
- `restart_clean.sh <env> <irc|-> <s3e|-> <LABEL> <argsfile>` — stop the running pass,
  update to `zs3:latest`, start a new one.
- `stopone.sh <q>` — stop one query's run (client and gateway).
- `rollback.py <env> dry|apply|restore <backup.json>` — roll every table to its
  `bench_baseline` tag (tag set by `register_tpcds_tables.py --tag-baseline`).
- `expire_abandoned.py <env> apply` — expire snapshots off the current lineage.
- `ns_backup.py <env>` — back up every table's metadata location / current snapshot / refs.
- `tagrows.py <env> <tables…>` — rows at the baseline tag vs current.
- `stats_snapshot.sh <env> <bucket> <out.json>` — exact rows + approx NDV per column of the
  base files (the plan-golden test's input).

Endpoints per node: 144 / 37 `- -`; 65 TPC-H `http://iceberg-rest:8181 http://tpcds-src:9000`.
See devOps `AGENT.md`.

## Growth soak

Prod data only grows; the fixed-base regression rolls back before every run and so
never tests that ticks stay flat as tables grow. A soak run does:

    SKIP_ROLLBACK=1 TPC_BATCH_TICKS=20 CANG_TAG=latest \
      bash /root/cang.sh <env> <irc|-> <s3e|-> latest SOAK_<node> "<list>"
    python3 tools/soak_check.py /root/SOAK_<node>   # FAIL: last-third median tick > 1.25x first third

Run it on its own namespace or after a fixed-base pass — it leaves the sources grown.
