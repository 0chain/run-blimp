# Testing the kit itself

Three offline suites — no cluster, no network, no credentials. Run all three
before shipping a change to the kit:

```
./test_kit.sh                      # decision logic behind the measurements
./test_setup_options.sh            # the --setup A/B/C choices and their guards
python3 test_seed_tpcds.py         # the CDC delta generator
```

Every case in them corresponds to a bug that shipped a *wrong number* rather
than an error, which is the class a run log cannot show you.

## `./test_kit.sh` — measurement decisions

- the fact table `--bench` appends to is derived from the query, so a run can't
  silently measure a query whose source never changed
- a non-blank `S3_ENDPOINT` always produces `--endpoint-url`; without it origin
  calls quietly go to the public cloud endpoint and the cache leg reports "no objects"
- the cache set is sized so the per-blobber shard share exceeds per-blobber RAM
  (below that the "hit" is served from blobber page cache and nothing
  client-side reveals it)
- mlperf runs accel=1 on a client under 16 vCPU, because extra ranks starve the
  mount-s3 daemon (it sits outside the dlio memory cgroup and gets OOM-killed)
- dlio's `<value> (<stddev>)` is parsed to the value
- **the kit never shells into a REMOTE node** — no `ssh`/`scp`/`aws ssm`, no
  docker against a remote daemon, no `docker exec` anywhere. Local docker is
  allowed and expected: installed on the node (the on-prem default) the kit
  reads the co-located gateway container for its real S3 keys, the catalog
  address the gateway itself uses, and the cross-node read count

## `./test_setup_options.sh` — the setup choices

54 assertions over the three `--setup` questions, with `curl` stubbed to replay
real captured responses (including Nessie's 500 on an unknown warehouse):

- a Nessie warehouse is a **name**, and the name survives `standup_data.sh`
  printing an `s3://…` warehouse of its own (it used to clobber it, so a run
  registered 24/24 tables into `mv` and then wired the node with
  `s3://blimp-e2e/wh`)
- choosing another S3 endpoint (B = 2) with the node's own catalog (A = 1) is
  demoted to a local catalog — metadata and data must share one endpoint
- the live admin-token file wins over the container env, which is only a
  provision-time snapshot
- `query_tables.py` extracts the tables a query reads from every `FROM`/`JOIN`
  position (comma lists and nested subqueries included, CTE names excluded)

Mutation-checked: breaking the warehouse-name handling or removing the A1+B2
guard both fail the suite.

## `python3 test_seed_tpcds.py` — the CDC generator

The appended rows must be *referentially* valid or a merge silently measures
nothing: returns reference the sales rows just written, dimension keys are drawn
from the pools the queries filter on, and decimals are clamped to the target
Iceberg precision.
