# test-blimp — playbook + expected results (storage, cache, query, API)

The single reference for what a healthy run looks like. Merged from the former
`EXPECTED_CACHE_RESULTS.md`, `EXPECTED_QUERY_RESULTS.md`, and the checked-in
`API_RESULTS.md` sample (2026-07-17); those files are retired.

## PASS / LOW / HIGH criteria — one table

Run everything with **`./run_e2e.sh`**; grade the perf numbers with
**`./check_perf.sh <run.log>`**. Reference = the 2026-07-17 fresh-4xlarge E2E.
**PASS** = within ±20% of reference. **LOW** = >20% below; **HIGH** = >20% above.
A LOW throughput or a HIGH timing is a **regression** (bad direction → `check_perf`
exits non-zero); the opposite direction is informational.

**Correctness / functional gates (hard pass/fail, exit-coded — not a band):**

| Test | PASS = | FAIL (exit≠0) = |
|---|---|---|
| test_query | every row-hash **MATCH** vs referee (initial, incremental, upsert) | any MISMATCH |
| test_flow_panel | 8/8 exact counter deltas + hit-rate 4dp + CPU>0 | any tile off |
| test_api | 0 unexpected 5xx/ERR (401/403 auth-proofs + llm-no-key expected) | any unexpected error |
| test_fingerprint | A-class 100% no-LLM **and** 0 D-class false positives | below either |
| verify_panel | all tiles consistent + live-increment works | any FAIL |
| verify_result_freshness | post-CDC re-execute + warm-hash match | stale serve / hash drift |

**Performance bands (±20% of the 2026-07-17 reference; higher=better unless noted):**

| Metric | Reference | LOW (<−20%) | HIGH (>+20%) |
|---|---|---|---|
| warp 1KiB TTFB p99 (ms, lower=better) | 5 | <4 (good) | >6 (bad) |
| warp PUT (MiB/s) | 875 | <700 | >1050 |
| warp GET (MiB/s) | 3247 | <2598 | >3896 |
| fio NFS write (MiB/s) | 1142 | <914 | >1370 |
| fio NFS cold read (MiB/s) | 1132 | <906 | >1358 |
| mlperf AU (%) | 92.3 | <74 | >100(cap) |
| mlperf samples/s | 18090 | <14472 | >21708 |
| mlperf MB/s | 1978 | <1582 | >2374 |
| cache direct-S3 (MB/s) | 1011 | <809 | >1213 |
| cache-S3 router (MB/s) | 3141 | <2513 | >3769 |
| cache-mp-s3 (MB/s) | 2326 | <1861 | >2791 |
| cache-NFS (MB/s) | 1659 | <1327 | >1991 |
| Spark/referee full scan (s, lower=better) | 85.5 | <68 (good) | >103 (slow) |
| Blimp author+shape+materialize (s) | 24.1 | <19 (good) | >29 (slow) |
| initial materialize (s) | 12.0 | <10 (good) | >14 (slow) |
| upsert full re-materialize (s) | 38.5 | <31 (good) | >46 (slow) |
| incremental merge_ms (wave) | 5.0 | <4 (good) | >6 (slow) |

Absolute numbers scale with EC width + client class — on an **xlarge client** (Spark
off by default) the *cache* MB/s will read LOW (client-bound); that's expected, not a
regression. For headline cache fidelity use a 4xlarge client. Ratios (each cache path
beats direct-S3; Blimp MV beats full scan) hold regardless of box.

Two measured environments:

| | **Reference (canonical)** | **Small-client sanity run (2026-07-17)** |
|---|---|---|
| Cluster | 2/1 EC, gateway `c6in.4xlarge` (16 vCPU / 30 GiB / 50 Gbps) | same |
| Client box | `c6in.4xlarge`, same VPC | `c6in.large` (2 vCPU / 4 GiB) |
| Use | the numbers to expect | proves a small client bottlenecks everything ~6× |

**The client must match the gateway class.** A 2-vCPU client measures ~1/6 of the
reference throughput and starves under mlperf (ssh/SSM brownouts). Ratios and
behaviours are portable; absolute numbers scale with EC width + instance class.

## Component glossary

| Term | What it is |
|------|------------|
| **Gateway** | `zs3server` EC2: `minioserver` (S3 :9000 + optimizer + DuckDB), `zus-nfs` (Ganesha :2049), `zus-router` (:8088), `zus-nessie` (:19120 MV catalog), `zus-cdc` (:9401 ops/probe). |
| **Router ("the cache")** | :8088 — read-through cache *logic*: hit/miss, origin streaming, async ingest (4 slots, `ZUS_ROUTER_SPOOL_CONCURRENCY`), eviction. Cached *bytes* live on the **eblobbers** in a cache bucket. |
| **MV store** | `tpcds-mv` bucket on the eblobbers, cataloged in zus-nessie. MV reads never touch the router or external S3. |
| **Client box** | Launched by `setup_tests.sh` in the cluster VPC; runs the customer Iceberg REST catalog (:8181) + all suites. |

## Endpoints — how the scripts get configured

Everything derives from `CLUSTER_ID`:

| Value | Source |
|-------|--------|
| VPC / subnet / `GW` (gateway private IP) | `cluster-<id>-zs3server` EC2 tags (`setup_tests.sh`) |
| `GW_AK` / `GW_SK` | gateway minio root creds via SSM `docker inspect minioserver` |
| `ADMIN_TOKEN` | deterministic `zus-<CLUSTER_ID>` |
| `ICEBERG_URL` | client box private IP :8181 |
| S3 VPC endpoint + IAM profile + warehouse write policy | created/attached by `setup_tests.sh` |
| Customer-supplied | `CLUSTER_ID`, `REGION`, `ORIGIN_BUCKET`, `WAREHOUSE`, EC2 keypair, AWS creds |

```bash
export CLUSTER_ID=… REGION=ap-south-1 GW=<gw-priv-ip> GW_AK=… GW_SK=… \
       ORIGIN_BUCKET=<bucket> WAREHOUSE=s3://<bucket>/wh \
       ICEBERG_URL=http://<box-priv-ip>:8181 NAMESPACE=tpcds SOURCE=customer
```

## Run order & durations

One command runs all of it: `run_e2e.sh` (per-phase wall-clock + PASS/FAIL table).
Measured unattended E2E, 2026-07-17, cluster 1784282017272 (4xlarge gateway,
c6in.4xlarge client): **total 35m07s**.

| Step | Measured (4xlarge E2E) | Small client | Pass signal |
|------|------------------------|--------------|-------------|
| 0 `setup_tests.sh` | **238s** | ~6 min | `BENCH BOX READY`; `curl <box>:8181/v1/config` = JSON |
| — gateway creds (SSM) + ship kit | **30s** | — | creds parsed |
| 1 `register_tpcds_tables.py` | **323s** | ~15 min | `registered 24/24` |
| 2 `hookup_cluster_source.sh` | **16s** | ~2 min | `Using default AWS credential chain`, router+gateway up |
| 3 `test_cache.sh` (incl. mlperf) | **533s** | ~45 min | numbers below; auto-cleanup on exit |
| 4 `test_flow_panel.sh` | **75s** | ~5 min | 8/8 PASS |
| 5 `test_query.sh` | **866s** first run | ~30 min | hash MATCH + timings below |
| 6 `test_api.sh` | **24s** | ~5 min | statuses below |

Registration reference row counts (sf1000, exact): `store_sales 2,879,987,999 ·
catalog_sales 1,439,980,416 · web_sales 720,000,376 · inventory 783,000,000 ·
store_returns 287,999,764 · customer 12,000,000`. Needs `pyiceberg>=0.9`
(int-backed decimals); `chunkINCR/` CDC deltas are excluded from base registration.

---

# Storage & cache (`test_cache.sh`)

## 1. warp — S3 :9000, 96 MiB objects, conc 16 (2/1)

| Metric | Reference | 2026-07-17 E2E | Small client |
|--------|-----------|----------------|--------------|
| 1 KiB TTFB (conc 1) | **~3 ms** median, p99 ~5 ms | 3 ms / p99 5 ms | ~3 ms |
| PUT | **~860 MiB/s** (write-marker-commit bound) | **874 MiB/s** | 154 MiB/s |
| GET (blobber-served) | **~3200 MiB/s** (nears NIC at conc 32) | **3208 MiB/s** | 550 MiB/s |

## 2. fio — NFS, 16 jobs × 2304 MiB, bs 1M

| Metric | Reference | 2026-07-17 E2E | Small client |
|--------|-----------|----------------|--------------|
| WRITE (direct/libaio) | **~1000 MiB/s** | **1146 MiB/s** | 78 MiB/s |
| READ (cold seq, set > gateway RAM) | **~2300 MiB/s** | **2223 MiB/s** | 397 MiB/s |

The 36 GiB read set exceeds gateway RAM (30 GiB) so the read is genuinely
cold-from-eblobber. Raw `dd` of the same files ≈ 2065 MB/s — NFS itself is fast.

## 3. mlperf resnet50 (dlio 2.0) via mountpoint-s3

Reference at accel-4 / rt-20 / pf-40: **AU 92.6 %, 18,223 samples/s, ~1993 MB/s**.
2026-07-17 E2E (fresh 4xlarge cluster): **AU 92.3 %, 18,090 samples/s, 1978 MB/s
(~1.98 GB/s), `train_au_meet_expectation: success`** — within 0.5 % of reference.
- Generate the 238-file dataset once, reuse (`MLPERF_KEEP=1`).
- **NFS is NOT the ML read path** (AU ~20 % — shuffled TFRecord reads are
  latency-bound over kernel-NFS; mp-s3's prefetch hides it).
- On a small client, dlio starves the box (ssh brownouts) — use a matching client.

## 4. read-through cache — 4 paths over the same origin table

`store_returns`, 18.6 GiB, 384 × ~50 MiB parquet, warm cache:

| Path | Reference | 2026-07-17 E2E | Small client |
|------|-----------|----------------|--------------|
| direct-S3 (origin) | ~990 MB/s (1.0×) | 952 (1.0×) | 127 (1.0×) |
| **cache-S3 (router)** | **~3400 MB/s (~3.5×)** | **3121 (3.3×)** | 255 (2.0×) |
| cache-mp-s3 | ~2300 MB/s (~2.3×) | 8429 (8.9×)* | 474 (3.7×) |
| cache-NFS (Ganesha) | ~1850 MB/s (~1.9×) | 9248 (9.7×)* | 203 (1.6×) |

\* page-cache-assisted (18.6 GiB set < 30 GiB gateway RAM — see caveat below).

Every cache path must beat direct S3. The router cache-fill is **async and
slot-capped (4)** — first pass fills partially; re-reads re-trigger skipped fills.

## 5. eviction (capacity-based, staggered watermarks)

- Signal = gateway `/admin/alloc/usage` (max of on-chain and blobber-measured pushed
  reading; on enterprise clusters the chain reads 0 so the push is authoritative).
- Router cache drains at **high 80 % → low 75 %**; the **MV reaper** only acts at 90 % —
  cheap-to-refetch cache is sacrificed before expensive MVs.
- Crossing high logs `[evict] alloc X% > high Y% — capacity-evict cache toward NB`;
  under continued reads the cache hovers at high; grace window spares in-flight objects.
- **Bench leftovers (~100 GB) will pin the alloc over the watermark** and the evictor
  then drains every cache fill (observed: alloc 94.7 %, 378 evictions, hit-rate
  collapse). `test_cache.sh` now auto-cleans on exit/abort (`KEEP_BENCH_DATA=1` to skip).

## Flow-panel verification (`test_flow_panel.sh`) — 8/8 expected

Cold sweep: `misses +N` and `fetched-from-origin +B` **exact**; fill converges ≤10
sweeps. Warm ×3: `hits +3N`, `served-from-cache +3B`, origin unchanged. `hit_rate ==
hits/(hits+misses)` to 4dp. Gateway CPU > 0 under load (38.6 % measured), 0 % idle is
honest. Panel semantics: `fetched > written` is normal (slot-capped ingest);
`evictions` with a small cache = the *allocation* crossed 80 %, check the router log.

---

# Query optimizer (`test_query.sh` + production numbers)

Reference profile: TPC-DS q1, `source=customer` (BYO Iceberg over your S3), SF1000
(`store_returns` 288M rows), query box c6in.xlarge. Result fingerprint — identical in
every mode (the speedups are not accuracy trade-offs):

```
sha256 = d2cc8119b02f5ed82b8c8f30e467b50860482d37f66a499ac6b3f6f41d45a800   rows = 100
```

| Mode | Wall-clock | vs Spark full |
|---|---|---|
| Spark full scan (no MV) | **300.5 s** | 1× |
| Spark on Blimp MV | **14.0 s** | **21.5×** |
| DuckDB on Blimp MV (Blimp engine) | **~5.9 s** | **~51×** |
| Blimp result-cache hit | **0–161 ms** | ~1,900× |
| Author + materialize (one-time) | **166.1 s** (rank 68.7 + materialize 97.4) | — |
| **Incremental refresh** (source changed) | **~4 s** (`merge_ms` 3.6–4.1) | vs ~107–166 s re-author |

2026-07-17 sanity run (2-vCPU client, Spark local): Spark baseline 665–769 s,
Blimp author+materialize 69.5 s, hash MATCH after the harness ivy-line fix.

2026-07-17 4xlarge E2E (cluster 1784282017272, c6in.4xlarge client): Spark full
76–82 s · Blimp one-time MV build 132 s (author 66.1 + shape 52.3 + materialize
13.8) · Blimp query over the MV 735 ms first serve · warm result-cache 0 ms ·
incremental delta-merge `merge_ms` 4.9–9.0 s per MV after a 50 k append.

## Result-cache freshness (`verify_result_freshness.sh`)

The serve-time freshness gate (zs3 `aa4cfac`) invalidates `qresults/<sig>` when
the MV's parquet is newer. Verified live 2026-07-17 on the 4xlarge cluster:
pre-append q1 1.69 s → 50 k append + `snapshot_changed` (`marked_stale: 3`) →
drift-poller delta-merged the q1 MV proactively (`merge_ms 8996`) → post-append
q1 **re-executed** (3.83 s wall, gateway log `engine=duckdb query_ms=1944`, new
result parquet written) → warm re-run 0.10 s served the refreshed result
(`[result-cache] HIT`). An independent Spark run over the post-append source
returned row-for-row the same 100 rows as Blimp's served result (**ROWS_MATCH**).
Note: an unchanged post-append hash is NOT a failure — q1's `order by
c_customer_id limit 100` can be legitimately stable under a small random append;
the pass signal is re-execution + Spark row match, not hash drift.

Incremental scope: the ~4 s brings the **MV** up to date; the query then re-executes
over the refreshed MV (~5.9 s) unless the flag-gated incremental *result*-maintenance
mode is enabled (off by default; keeps post-change reads sub-second, row-hash-verified).

## Incremental MV fleet (10 MVs, TPC-DS SF1000 — measured 2026-07)

One source change, every dependent MV delta-merged and row-hash verified:

| query | MV | fact | MV rows | delta-merge | verified |
|---|---|---|---:|---:|:-:|
| q1 | mv_9dea9e27 | store_returns | 54,446,236 | **11.1 s** | ✓ |
| q3 | mv_c6b05298 | store_sales | 541,666 | **3.9 s** | ✓ |
| q3 (variant) | mv_96deadde | store_sales | 862 | **3.8 s** | ✓ |
| q19 | mv_9154eb26 | store_sales | 1,564,783 | **4.6 s** | ✓ |
| q19 | mv_db28a568 | store_sales | 549,528 | **4.0 s** | ✓ |
| q19 (net_profit) | mv_a1509238 | store_sales | 65,715 | **3.7 s** | ✓ |
| q43 | mv_b2ee1155 | store_sales | 18,663 | **3.9 s** | ✓ |
| q52 | mv_b0fa0382 | store_sales | 319,900 | **4.0 s** | ✓ |
| q52 (narrow) | mv_4f4bed59 | store_sales | 1,057 | **4.0 s** | ✓ |
| q55 | mv_3afa058c | store_sales | 917,663 | **4.0 s** | ✓ |

- 9 of 10 MVs delta-merge in **3.7–4.6 s**, flat from 862-row to 1.5 M-row MVs
  — the merge cost tracks the source delta, not the MV size.
- The 54 M-row store_returns aggregate (q1) merges in **11.1 s**.
- Every refresh is **row-hash verified** against a full recompute (the
  `verified` column).
- Cold author — the one-time build per query shape — runs ~3–6 min.

Suite notes: needs `unzip` (in setup now); Spark stdout is filtered for ivy `::` noise;
stage 4 (author q1–q99 then reap) needs `~/tpcds_queries/` (duckdb tpcds extension).

---

# API (`test_api.sh`) — expected statuses per endpoint

Safe by default (reads + plan-only + auth probes). `--run-query` executes one real
query; `--destructive` exercises mutating calls. Writes runtime results to
`./API_RESULTS.md` (generated, not tracked). **authprobe 400s with a precise error
message are PASSES** — they prove the route is alive, token-gated, and validating.

| Endpoint | Expected | Correct response looks like |
|---|---|---|
| `GET /admin/mv/list` (no token) | **401** | proves token enforcement |
| `POST /admin/query/run` (probe) | 400 | `original_sql required` |
| `POST /admin/query/explain` (probe) | 400 | `engines_yaml, engine, and sql required` |
| `GET /optimizer/advice_log` | 200 | `{"count":N,"entries":[…]}` |
| `GET /admin/mv/list` | 200 | array of `{namespace,table,snapshot_id,row_count,schema[…]}` |
| `GET /admin/mv/registered_queries` | 200 | `{"queries":[{sig,original_sql,mv_table}…]}` |
| `GET /admin/mv/source_tables` | 200 | `{"tables":[…]}` |
| `GET /admin/mv/stream` | 200-stream | SSE `event: snapshot` + data frames (curl `-m` cutoff is a pass) |
| `POST /admin/mv/llm_authoring` | 200 | `{"llm_authoring_enabled":bool}` |
| `POST /admin/mv/reap` (dry) | 200 | `{"dry_run":true,"total_mvs":N,"total_bytes":B,…}` |
| `POST /admin/source/snapshot_changed` (probe) | 400 | `table required` |
| `POST /admin/source/append` (probe) | 400 | `namespace, table, and at least one row required` |
| `POST /cdc/append` (:9401) | 200 | `{"appended":N,"snapshot_id":…,"total_rows":N}` |
| `GET /view?token=&ns=&table=` (:9401) | 200 | HTML table page |
| `GET /view` (:9401, no token) | 403 | viewer is token-gated |
| `GET /bench/list?token=` (:9401) | 200 | `{"runs":[…]}` (the panel bench API) |
| `GET /metrics` | 200 | Prometheus text |
| `GET /admin/slow_log` | 200 | `{"count":N,"entries":[…],"threshold_ms":1000,"total_queries":N,"total_rewrites":N}` |
| `GET /admin/sidecar/config` | 200 | `{"version":…,"mode":…,"source":{…}}` |
| `GET /admin/alloc/usage` | 200 | `{"capacity_bytes":C,"used_bytes":U,"used_pct":P}` |

Harness notes: per-run mktemp probe files (concurrent runs raced on a shared
`/tmp/.probe_body` and produced bogus ERRs); an SSE stream that delivered data before
the timeout is counted `200-stream`. **Do not run while mlperf is saturating the
client box** — every probe times out locally and reports ERR.
