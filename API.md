# Prod-Query API reference — what each endpoint does + the expected response

Every endpoint here is **customer-facing**: it is *your* cluster's own API, called with
*your* cluster token. You reach it either directly on your cluster's gateway with the
token `zus-<cluster-id>`, or through the Blimp control-plane proxy using your normal
Blimp app login. Nothing here talks to Blimp-internal systems.

> **Read this once:** the `/admin/*` prefix means **"this cluster's admin token"** — it
> is *your* administrative access to *your own cluster*. It does **not** mean
> Blimp-internal admin. Every `/admin/*` route below is authenticated by your cluster
> token and operates only on your cluster and your data.

Two servers on the gateway:

| Server | Port | Auth |
|---|---|---|
| zs3 gateway (S3 + query optimizer) | `:9000` | `Authorization: Bearer zus-<cluster-id>` (`?token=` accepted for SSE) |
| zus-cdc helper (viewer / bench / CDC) | `:9401` | `?token=zus-<cluster-id>` query param (browser pages) or Bearer |

Every expected response below was **verified live** (2026-07-17, cluster
1784282017272). This document is the *reference*; the measured counterpart from an
actual run is the generated `API_RESULTS.md` written by `test_api.sh` (last section).

---

## Query & optimizer (`:9000`)

### `POST /admin/query/run` — run a query through the optimizer
The main entry point. Give it SQL; the optimizer resolves or authors an MV
(library/scheme-match first; the LLM is used only for novel shapes *and only if LLM
authoring is enabled*), rewrites the query onto the MV, executes on DuckDB, and
result-caches the answer. Repeats serve from the result cache at ~0 ms; a
CDC-refreshed MV invalidates the cached result (serve-time freshness gate).
- Body: `{"original_sql":"…","source":"customer"}`
- **200** `{status:"ok", engine, mv_table, rows, query_ms, author_ms, result_sig,
  result_bucket, md5}` — engine `"cached"` + `query_ms:0` on a warm hit
- 400 `original_sql required` on an empty body

### `POST /admin/query/explain` — EXPLAIN on a chosen engine
Plan-only; nothing executes against your data. `engines_yaml` may be **inline YAML**
(or a gateway file path); `duckdb-stdin` always ships in the gateway image.
- Body: `{"engine":"local","sql":"SELECT 1","engines_yaml":"engines:\n  - name: local\n    transport: duckdb-stdin\n    dialect: duckdb\n"}`
- **200** plan output · 400 `engines_yaml, engine, and sql required`

### `GET /admin/query/suite` — the curated TPC-DS suite
Returns the proven seed query set in numeric order; the UI's "Run full suite" fetches
this then POSTs each query to `query/run` for per-query timings.
- **200** `[{qid:"q03", sql:"…"}, …]`

### `GET /optimizer/advice_log` — advisory history
- **200** `{"count":N,"entries":[…]}`

*`POST /optimizer/advise` + `GET /optimizer/job/<id>` were removed 2026-07-17: the
legacy heuristic advisor's AI path was retired May 2026, and the real LLM surface
is `query/run` + `mv/llm_author` (the money path that builds the MV). advise only
recommended.*

## Materialized views (`:9000`)

### `GET /admin/mv/list` — tables visible to the optimizer
- **200** array of `{namespace, table, snapshot_id, row_count, schema[…]}`

### `GET /admin/mv/registered_queries` — every query with an authored MV
- **200** `{"queries":[{sig, original_sql, mv_table}…]}`

### `GET /admin/mv/source_tables` — attached source tables
- **200** `{"tables":[…]}`

### `GET /admin/mv/data?namespace=<ns>&table=<t>&limit=N` — page table/MV rows
- **200** rows · 400 `namespace and table query params are required`

### `GET /admin/mv/stream` — live MV events (SSE)
EventSource cannot set headers, so `?token=` is accepted.
- **200** stream: `event: snapshot` frame, then updates

### `POST /admin/mv/resolve` — would this SQL hit an MV?
Read-only resolution: exact-library → scheme-match → (mode `llm`) LLM.
- Body: `{"original_sql":"…","mode":"match"}`
- **200** `{path:"library|scheme|miss", …}`

### `POST /admin/mv/llm_author` — author an MV for a query
Library/scheme-matched queries need no LLM and return immediately. A **novel** shape
needs LLM authoring — **disabled by default** (no `ANTHROPIC_API_KEY` on the cluster):
- **200** `{mv_table, …}` for library/scheme-matched queries
- 500 `auto_author returned no MV (check ANTHROPIC_API_KEY…)` for novel shapes with
  LLM off — expected on default clusters

### `POST /admin/mv/llm_authoring` — toggle/inspect LLM authoring
- Body `{}` reads, `{"enabled":true|false}` sets · **200** `{"llm_authoring_enabled":bool}`

### `POST /admin/mv/reap` — reclaim MV storage
- Body: `{"policy":"age","dry_run":true}` · **200** `{dry_run, total_mvs, total_bytes, …}`
  (`dry_run:false` actually deletes)

### `DELETE /admin/mv/delete?namespace=<ns>&table=<t>` — drop one MV
- **200** (idempotent; an unknown target reports zero deletions)

## Source & CDC (`:9000`)

### `POST /admin/source/snapshot_changed` — tell the cluster your table changed
The CDC webhook — wire it into your writer or an S3-event notification for continuous
CDC (see [`README.md`](./README.md)). Marks dependent MVs stale; the wave/drift poller
then **incremental-merges** mergeable aggregates (seconds) or re-materializes.
- Body: `{"namespace":"tpcds","table":"store_returns","trigger":"…"}`
- **200** `{status:"ok", marked_stale:N, elapsed_ms}` · 400 `table required`

### `POST /admin/source/append` — direct streaming append
Writes rows straight into an Iceberg table (no Spark/Trino), ~50–200 ms per call.
- Body: `{"namespace":"…","table":"…","rows":[{…}]}`
- **200** `{appended:N, snapshot_id, total_rows}` · 400 lists the required fields

*Removed 2026-07-17 — the auto-iceberg registrar (`/admin/iceberg/state`,
`/admin/iceberg/snapshot_at`, `/admin/iceberg/scan`) self-catalogs raw parquet for
clusters with no Iceberg catalog; a BYO-catalog cluster's own catalog IS the source
of truth, so it was dead customer surface. `/admin/source/ingest` (Spark CTAS bulk
ingest) also removed — caller-less; `append` + metadata-only registration cover the
real paths.*

## CDC helper (`:9401`)

### `GET /view?token=<token>&ns=<ns>&table=<t>` — HTML table viewer
Browser page (auth via `?token=` — headers unavailable to a browser link). Renders MV
parquet or catalog-resolved source tables, paginated.
- **200** HTML · **403 without the token — by design**

### `GET /bench/list?token=<token>` — benchmark history
Backs the panel's benchmark table (`/bench/run`, `/bench/stop`, `/bench/log` manage
runs on bench-enabled builds).
- **200** run list
*(The bare `/bench` and `/snapshot` HTML pages exist only on demo-source clusters.)*

### `POST /iceberg/added_files` — register data files into a table
- Body: `{"table":"store_returns","files":[…]}` (empty `files` = report only)
- **200** `{"current_snapshot":"…","files":[…]}` · 400 `table required`

### `POST /cdc/wave` — run one CDC wave
- Body: `{"dry_run":true}` · **200** wave report `{summary:{rows, incremental, fallback}, …}`

### `POST /cdc/append` — demo append helper
Appends demo rows to `cdc_demo.sales` (mutates even with `{}` — destructive-gated in
the test suite). · **200** `{appended, snapshot_id, total_rows}`

## Ops & billing (`:9000`)

| Endpoint | What it is | Expected |
|---|---|---|
| `GET /metrics` | Prometheus text (query counters, cache, MV stats) | **200** |
| `GET /admin/slow_log` | `{threshold_ms, count, total_queries, entries[]}` — `total_queries` counts **every** query-run incl. cached 0 ms serves (the panel's "Queries run"); `count`/`entries` are only the >1 s ring | **200** |
| `GET /admin/sidecar/config` | resolved source/sidecar config `{version, mode, source{…}}` | **200** |
| `GET /admin/alloc/usage` | allocation usage `{capacity_bytes, used_bytes, used_pct}` (max of on-chain and blobber-pushed reading) | **200** |

## Auth — expected *failures* (the security proofs)

| Probe | Expected | Proves |
|---|---|---|
| any `/admin/*` without token | **401** | admin token enforced |
| `/view` without `?token=` | **403** | viewer gated |

## Internal — not customer-facing

These exist on the gateway but are **not part of the customer API** and `test_api.sh`
never touches them — the datalake/provisioning signing path Blimp's own provisioning
uses to bootstrap a cluster:

- `/admin/onchain/*` — on-chain signing operations
- `/admin/wallet/load-from-sm` — loads a wallet from secrets management
- the provisioning bootstrap endpoint

---

## `test_api.sh` — probe all of the above

```bash
# safe by default — reads + plan-only + auth probes, mutates nothing
GATEWAY=<gateway-ip> CLUSTER_ID=<cluster-id> ./test_api.sh
# full: executes one real query + the mutating set
./test_api.sh --gateway <ip> --cluster-id <id> --run-query --destructive
```

Needs only the gateway IP + cluster id (public or private) — no client box, no
catalog — so it can run **in parallel with `setup_tests.sh`**. Env: `PORT` (9000),
`CDC_PORT` (9401), `SCHEME`, `OUT` (default `./API_RESULTS.md`, the generated
*measured* counterpart to this reference), `TIMEOUT` (default 20 s; exec/plan calls
use `EXEC_TIMEOUT`, default 180 s — a cold gateway's first materialize is slow).

Verified full-mode tally (2026-07-17): **24 × 2xx** plus the two auth proofs
(401/403) and one documented feature-off response (llm_author 500 for a novel query
shape when LLM authoring is disabled — the only remaining non-2xx besides the two
auth proofs).

## Sample run output (`test_api.sh --run-query`, 2026-07-17)

Real responses from cluster `1784282017272`, truncated for width. This is what
`API_RESULTS.md` looks like after a run (that file is generated, not tracked).

| Method | Path | Status | Response (truncated) |
|---|---|---|---|
| `GET` | `/admin/mv/list (no token)` | **401** | expect 401 — token enforced |
| `POST` | `/admin/query/explain` | **200** | `{"engine":"local","dialect":"duckdb","duration_ms":1,…}` |
| `GET` | `/optimizer/advice_log` | **200** | `{"count":0,"entries":[]}` |
| `POST` | `/admin/query/run` | **200** | `{"status":"ok","mv_table":"mv_c5a41497…","authored":true,"author_ms":25063,"shape_ms":15151,"materialize_ms":9912,"query_ms":68…}` |
| `GET` | `/admin/query/suite` | **200** | `{"queries":[{"qid":"q01","sql":"WITH customer_total_return AS …"}…]}` |
| `GET` | `/admin/mv/list` | **200** | `[{"namespace":"cdc_demo","table":"sales","snapshot_id":"1105…","row_count":600,"schema":["item","amount","ts"]…}]` |
| `GET` | `/admin/mv/registered_queries` | **200** | `{"queries":[{"sig":"c5a41497…","original_sql":"WITH …","mv_table":"mv_c5a41497…","rows":2688400,"cols":3}]}` |
| `GET` | `/admin/mv/source_tables` | **200** | `{"tables":[{"name":"call_center"},{"name":"catalog_returns"}…]}` |
| `GET` | `/admin/mv/data?namespace=cdc_demo&table=sales&limit=1` | **200** | `{"schema":[{"name":"item","type":"large_string"}…],"rows":[…]}` |
| `GET` | `/admin/mv/stream` | **200-stream** | `event: snapshot data: [{"namespace":"cdc_demo",…}]` |
| `POST` | `/admin/mv/resolve` | **200** | `{"path":"library_exact","matched":true,"mv_table":"mv_c5a41497…","mv_sql":"SELECT …"}` |
| `POST` | `/admin/mv/llm_author` | **200** | `{"status":"ok","table":"mv_c5a41497…","rewrite_sql":"WITH customer_total_return AS (SELECT * FROM iceberg.tpcds_mv.mv_…)"}` |
| `POST` | `/admin/mv/llm_authoring` | **200** | `{"llm_authoring_enabled":false}` |
| `POST` | `/admin/mv/reap` (dry) | **200** | `{"dry_run":true,"total_mvs":13,"total_bytes":129384761,"reaped_total":0,"evicted":0}` |
| `DELETE` | `/admin/mv/delete` (no target) | **200** | `{"namespace":"__none__","status":"ok","table":"__none__"}` |
| `POST` | `/admin/source/snapshot_changed` (probe) | **400** | `table required` |
| `POST` | `/admin/source/append` (probe) | **400** | `namespace, table, and at least one row required` |
| `GET` | `/view?token=&ns=&table=` | **200** | `<!doctype html>…<title>cdc_demo.sales</title>…` |
| `GET` | `/view` (no token) | **403** | `<h3>unauthorized</h3>` |
| `GET` | `/bench/list?token=` | **200** | `{"runs":[],"gateway_storage":"25G used of 97G"}` |
| `POST` | `/iceberg/added_files` | **200** | `{"current_snapshot":"9054774094882937635","files":[]}` |
| `GET` | `/metrics` | **200** | `# HELP zs3_tmpfs_hits_total …` (Prometheus) |
| `GET` | `/admin/slow_log` | **200** | `{"count":0,"threshold_ms":1000,"total_queries":10,"total_rewrites":5}` |
| `GET` | `/admin/sidecar/config` | **200** | `{"version":"v348","mode":"split","source":{"irc_url":"http://…:8181\|s3://…","namespace":"tpcds"…}}` |
| `GET` | `/admin/alloc/usage` | **200** | `{"capacity_bytes":113816633344,"used_bytes":81928844682,"used_pct":71.98}` |

Note the `query/run` fields the RECENT RUNS panel table renders: `author_ms`,
`shape_ms`, `materialize_ms`, `query_ms`, `mv_table`, `rows`, `cols`, `scheme`,
`mv_sql`, `rewrite_sql`, plus `result_url`/`mv_url` viewer links.

## `gen_incr_delta.py` — a delta for the incremental test

`gen_incr_delta.py` generates a **seeded, pseudo-random** `store_returns` delta parquet
for exercising the incremental-refresh path. It is deterministic (a fixed `--seed`
produces byte-identical rows, so results are reproducible and diffable), but the rows
are genuinely randomized (customer, store, date, and lognormal amounts) and tuned to
**move the query's result** — a tunable fraction land on the query's target stores so
per-customer totals actually cross selection thresholds. It also writes a
`<chunk>.manifest.json` sidecar with the seed, the exact knobs, and summary stats.

```bash
python3 gen_incr_delta.py --seed 42 --rows 20000 \
  --out s3://<your-bucket>/store_returns/chunkINCR_s42/data.parquet \
  --customers 2000000 --target-store-sks 15,42,77 --target-frac 0.35
# then add_files that parquet into your store_returns table and
# POST /admin/source/snapshot_changed to trigger the incremental refresh.
```

Needs `pip install pyarrow` (and `s3fs` for `s3://` output). See
[`EXPECTED_TEST_RESULTS.md`](./EXPECTED_TEST_RESULTS.md) for the incremental-refresh timing.
