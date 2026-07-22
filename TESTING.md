# Testing guide — Blimp cluster test kit

Reproducible tests for a Blimp enterprise cluster, run from a client box **inside
the cluster's VPC** so every path is private (no egress). `setup_tests.sh` launches
that box and wires it; `run_e2e.sh` runs the whole catalog end-to-end in one command.

Reference numbers for every test: **[`EXPECTED_TEST_RESULTS.md`](EXPECTED_TEST_RESULTS.md)**.
API reference + sample output: **[`API.md`](API.md)**.

## The test catalog — 9 executable tests (7 pass/fail + 2 benchmarks)

| # | Script | What it validates | Pass criterion (exit-gated) |
|---|--------|-------------------|------|
| 1 | **`test_cache.sh`** | Storage/cache: warp (S3 :9000), fio (NFS :2049), mlperf (mp-s3), read-through cache (direct-S3 vs cache-S3/mp-s3/NFS), router eviction | ran clean; numbers graded by `check_perf.sh` (±20% band) |
| 2 | **`test_flow_panel.sh`** | Every "Cache data flow" panel tile — exact counter deltas | **8/8** exact (misses +N, hits +3N, hit-rate 4dp, CPU>0); exit≠0 on any fail |
| 3 | **`test_query.sh`** | Optimizer: baseline vs Blimp MV (row-hash), **CDC incremental**, **upsert → full re-materialize** | **all row-hash MATCH** vs the referee; exit≠0 on any MISMATCH |
| 4 | **`test_api.sh`** | Every gateway API endpoint (:9000 + cdc :9401) with real payloads → `API_RESULTS.md` | 0 **unexpected** 5xx/ERR; exit≠0 otherwise |
| 5 | **`test_fingerprint.sh`** | Fingerprint/scheme-match robustness — N unique q1 variants | A-class 100% no-LLM **and** 0 D-class false positives; exit≠0 otherwise |
| 6 | **`verify_panel.sh`** | All 15 panel stats vs cluster-side truth | per-tile PASS + consistency checks; exit≠0 on any FAIL |
| 7 | **`verify_result_freshness.sh`** | Serve-time freshness gate: post-CDC query re-executes | re-exec + warm-hash match; exit≠0 on FAIL |
| 8 | **`bench_incremental.sh`** | MV lifecycle timing — append→incremental vs upsert→full | benchmark (min/median/avg/max) — no gate |
| 9 | **`bench_tpcds99.sh`** | All 99 TPC-DS: cold + warm + **cdc** blast-radius | benchmark — no gate |

> The **incremental-MV** and **upsert** tests are stages 3–3f of `test_query.sh`
> (append→delta-merge, then upsert→full-rebuild, each row-hash-verified against a fresh
> referee recompute). Not separate scripts. `bench_incremental.sh` is the statistical
> version of the same paths.
>
> **Referee / client size:** by default the correctness referee is a **gateway-side
> no-MV full source scan** (no client-side Spark) — so the client box can be **xlarge**.
> `SPARK_REFEREE=1` swaps in local Spark for a cross-engine check (needs a big box + JVM).
> Full cache-throughput fidelity still wants a 4xlarge client (warp/fio/mlperf are
> client-bound); an xlarge client just yields lower, client-limited cache MB/s.
>
> **LLM authoring test** lives in the ops repo, not here:
> `devOps/0chain/scripts/blimp-tests/test_llm.sh` (needs an `ANTHROPIC_API_KEY` on the
> gateway; internal-only).

Pipeline helpers (not tests): `setup_tests.sh` (launch+wire the box), `register_tpcds_tables.py`
(metadata-only register 24 tables), `hookup_cluster_source.sh` (point a cluster at your
catalog), `seed_tpcds.py` / `upsert_tpcds.py` / `gen_incr_delta.py` (CDC deltas),
`run_e2e.sh` (run everything, timed).

---

## Hardware (one AWS region, e.g. `ap-south-1`)

1. **Blimp cluster** — provisioned from `blimp.software`: gateway + eblobbers, erasure-coded
   (e.g. 2/1). Note the **cluster id** and gateway **private IP** (NFS `:2049`, S3 `:9000`,
   router `:8088`). Size the gateway to your target (2/1 → `c6in.4xlarge`).
2. **Client box** — EC2 in the cluster's **VPC & subnet**, matching the gateway class
   (a small client bottlenecks everything ~6×). `setup_tests.sh` launches it.

## Quick start — one command

```bash
CLUSTER_ID=<id> REGION=ap-south-1 \
  ORIGIN_BUCKET=<your-bucket> WAREHOUSE=s3://<your-bucket>/wh \
  KEY_NAME=<ec2-keypair> ./run_e2e.sh
```

Launches the client box, wires creds + VPC endpoint + IAM, registers the 24 tables,
hooks up the cluster, then runs **all 8 pass/fail tests** — cache → flow-panel → query
→ api (parallel with setup) → **verify_panel → verify_result_freshness → test_fingerprint** — and grades the perf numbers against the ±20% reference with `check_perf.sh`.
Prints a per-phase wall-clock PASS/FAIL table. Measured clean run: **~20–25 min** on a
4xlarge cluster.

The 2 benchmarks are opt-in (they're long): `RUN_BENCH=1` adds `bench_incremental`
(~30 min), `RUN_TPCDS99=1` adds `bench_tpcds99` (hours). `STRICT_PERF=1` makes a
perf-band regression fail the run (default: advisory).

```bash
RUN_BENCH=1 STRICT_PERF=1 CLUSTER_ID=<id> … ./run_e2e.sh   # everything, perf-gated
```

## Shared environment

All suites read the same env (setup_tests + hookup print these):

```bash
export CLUSTER_ID=… REGION=ap-south-1 GW=<gw-priv-ip> \
       GW_AK=<gw-minio-key> GW_SK=<gw-minio-secret> \
       ORIGIN_BUCKET=<bucket> WAREHOUSE=s3://<bucket>/wh \
       ICEBERG_URL=http://<box-priv-ip>:8181 NAMESPACE=tpcds SOURCE=customer
```
The API/panel/fingerprint tests need only `GATEWAY=<gw-ip>` + `CLUSTER_ID`
(token is `zus-<CLUSTER_ID>`), so they can run standalone from anywhere with reach.

---

## Running each test

### 1. `test_cache.sh` — storage & cache (~45 min incl. mlperf)
```bash
env $SHARED_ENV bash test_cache.sh          # auto-cleans bench data on exit
```
warp/fio/mlperf/4-path-cache/eviction. Pass = numbers within ~±20% of the reference.
`KEEP_BENCH_DATA=1` to skip cleanup (don't — leftovers pin the alloc over the evictor watermark).

### 2. `test_flow_panel.sh` — cache-flow tiles (~5 min)
```bash
env $SHARED_ENV TABLE=customer_address bash test_flow_panel.sh
```
Pass = **8/8**: cold sweep `misses +N` / `fetched +B` exact, fill converges ≤10 sweeps,
warm ×3 `hits +3N`, `hit_rate` to 4dp, gateway CPU>0 under load.

### 3. `test_query.sh` — optimizer + incremental + upsert (~25–30 min)
```bash
env $SHARED_ENV bash test_query.sh          # SKIP_UPSERT=1 to skip stage 3d–3f
```
baseline full source scan (gateway no-MV referee) → Blimp author+materialize (row-hash MATCH)
→ **append→incremental merge** (Blimp == referee full recompute) → **upsert→full re-materialize** (row-hash MATCH) → MV eviction.

### 4. `test_api.sh` — every endpoint (~5 min, or ~1 min safe)
```bash
GATEWAY=<gw-ip> CLUSTER_ID=<id> bash test_api.sh                    # safe: reads + probes
GATEWAY=<gw-ip> CLUSTER_ID=<id> bash test_api.sh --run-query --destructive  # full
```
Writes `API_RESULTS.md`. Full mode: **24 × 2xx** + 2 auth proofs (401/403), 0 × 5xx.

### 5. `test_fingerprint.sh` — fingerprint/scheme match
```bash
GATEWAY=<gw-ip> CLUSTER_ID=<id> N=30 bash test_fingerprint.sh      # N up to 100
```
N unique q1 variants (cosmetic/constant/structural/novel) through the read-only
resolver. Reports no-LLM coverage per class. Reference: **30/30 (100%)** no-LLM.

### 6. `verify_panel.sh` — all 15 panel stats
```bash
GATEWAY=<gw-ip> CLUSTER_ID=<id> ./verify_panel.sh                  # FLOW=1 chains test_flow_panel
```
Live-increment test on Queries-run, MVs live (reap inventory), CDC/refreshes,
cache byte-consistency, evictions, gateway CPU/storage/disk, per-blobber, MV shape.
Pass = **11/11** (+ 2 documented notes).

### 7. `verify_result_freshness.sh` — freshness gate
```bash
env $SHARED_ENV ./verify_result_freshness.sh
```
Pre-append q1 → 50k append + `snapshot_changed` → post-append q1 **re-executes** →
warm 0ms serve → independent referee (gateway no-MV) row match.

### 8. `bench_incremental.sh` — lifecycle timing
```bash
env $SHARED_ENV ITERS=5 AUTHOR_ITERS=0 bash bench_incremental.sh   # append+upsert medians
```
Prints min/median/avg/max for append→incremental and upsert→full refresh.
Reference: append merge ~1–11s (median ~5s), upsert full ~27–33s.

### 9. `bench_tpcds99.sh` — all 99, cold+warm+cdc
```bash
env $SHARED_ENV TIMEOUT_S=200 bash bench_tpcds99.sh                # per-query cap 200s
```
Per-query TSV + distributions (author/materialize/cold/warm/cdc), fresh-vs-reuse split,
CDC blast-radius. Heavy sf1000 queries → hours; too-big MVs record `timeout` and move on.

---

## Deploying / accelerating

Build+deploy the stack with `../devOps/0chain/scripts/deploy.sh`:
`deploy.sh webapps` (runner build + replica sync), `deploy.sh zs3 --wait` (image CI),
`deploy.sh gateway <cluster_id>` (recreate one gateway on the latest image),
`deploy.sh datalake` (b4/b9 redeploy).
