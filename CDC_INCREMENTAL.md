# Incremental materialized views (CDC) — how it works and how to use it

Blimp keeps an MV fresh as your source grows by **delta-merging** newly-appended
rows into the existing MV, instead of re-scanning the whole fact. This doc is the
practical guide: what makes a query eligible, how to drive a refresh, the setup it
requires, the performance model, and the current limits.

## The lifecycle

1. **Author** — run a query; Blimp materializes its MV (one-time cost).
2. **Append** — write new rows to a source table (an Iceberg *append* commit).
3. **Notify** — `POST /admin/source/snapshot_changed` (or wire an S3-event/webhook).
4. **Re-serve** — the next query hits the serve-time freshness gate: it diffs the
   source snapshots, and if the MV is **decomposable** and the source is
   **append-only**, it **delta-merges** the new files into the MV (`mode=incremental`
   in `/admin/mv/wave/report`). Otherwise it falls back to a full re-author.

```bash
# append rows, then tell the cluster the source changed:
python3 seed_tpcds.py --catalog http://<catalog-host>:8181 \
  --warehouse s3://<bucket>/wh --namespace tpcds --table store_returns --rows 5000
curl -s -X POST http://<gateway>:9000/admin/source/snapshot_changed \
  -H "Authorization: Bearer zus-<cluster-id>" -H "Content-Type: application/json" \
  -d '{"namespace":"tpcds","table":"store_returns","trigger":"customer"}'
# then re-run the query — it delta-merges and serves fresh.
```

`bench_incremental.sh` runs exactly this loop and prints `materialize_ms`; the wave
mode is in `/admin/mv/wave/report`.

## Which queries are CDC-refreshable

An MV can be incrementally merged only when its body is **per-column
decomposable** — i.e. the merge can re-combine old + delta per output column:

**Eligible:**
- **Additive aggregates**: `SUM`, `COUNT` (not `COUNT(DISTINCT)`), `MIN`, `MAX`.
- Every aggregate **aliased** — `sum(x) AS x_total` *or* space-aliased `sum(x) x_total`
  (TPC-DS uses the no-`AS` form); the merge references it by name.
- A **single GROUP BY** over the appended fact (+ stable dimension joins).
- **Append-only** source (inserts only; no updates/deletes/compaction — a
  `removed_files` diff forces a full rebuild to avoid double-counting).

**Not eligible (falls back to full re-author):**
- `AVG`, `STDDEV`, `COUNT(DISTINCT)`, percentiles — not distributive per-column.
  (`AVG` *could* be made eligible by storing `SUM`+`COUNT` and dividing at serve.)
- **CTE / `WITH`** bodies — opaque to the column reasoner. *But* if the optimizer's
  MV is the mergeable **inner aggregate** of a CTE query, that MV is eligible even
  though the full query isn't. TPC-DS **q1** is the canonical example: its MV is
  `customer_total_return` (`sum(sr_return_amt)` by customer/store) → merges fine,
  while the outer `AVG` comparison runs at serve.
- **Multi-fact joins / `EXISTS` over other facts** — a body aggregating or gating on
  several facts (e.g. TPC-DS **q10**: customer-demographics gated by `EXISTS` over
  `store_sales` AND `web_sales`/`catalog_sales`) can't merge a **single-table** delta
  meaningfully — the delta leg still re-scans the unchanged facts, so it's refused.
- **Correlated subqueries / derived-grain result-shapes** — bodies whose grain isn't a
  clean additive group-by (e.g. per-transaction needle queries) don't merge.

Rule of thumb: **single-fact `SUM`/`COUNT` group-by = incremental; everything else
= full recompute** (still correct, just not delta-fast).

## Verified set (SF1000, independently checked)

These MVs delta-merge **and** are confirmed correct by `spark_verify.sh` (an
independent Spark row-hash of *original-over-source* vs *rewrite-over-MV*, an engine
separate from the gateway's DuckDB). Merge stays ~4 s for store_sales MVs
(0.9K–1.6M rows) and ~11 s for the 54M-row store_returns MV — **O(MV size)**, not
O(fact); cold author (full 2.88B-row scan) is 3–6 min, so the merge is a ~50–90×
refresh speedup.

| query | fact | shape |
|---|---|---|
| q1  | store_returns | CTE inner-aggregate (`sum(sr_return_amt)` by customer) |
| q3, q19, q43, q52, q55 | store_sales | single-fact `SUM` group-by (brand/category/store × year) |

**Always verify correctness independently, not just merge speed.** Run
`INCR=1 QIDS="q1 q3 q19 q43 q52 q55" … bash spark_verify.sh` after an append: a
merge that's *fast* is not proof it's *right*. This surfaced a real bug — a widened
MV served with a bare `SELECT *` rewrite returned wrong rows (dropped the query's
filters) while the built-in row-hash verifier (budget-limited on billion-row
originals) missed it.

## Known limits (as of this writing)

- **Very large MVs (tens of millions of rows)** can materialize but fail to
  register/serve (`bind-check exit 1` / "not in catalog") — a big-MV persistence gap.
- **`spark_verify.sh` itself** can't replay every rewrite shape in Spark yet
  (self-aliased aggregates like `SUM(agg0) AS agg0`, derived-grain `EXISTS`), so a
  verifier ERROR is not proof of a gateway bug — it may be a replay-translation gap.

## Performance model + tuning knobs

The delta-merge re-runs
`SELECT keys, SUM(agg) FROM (read_parquet(old_MV) UNION ALL BY NAME <delta>) GROUP BY keys`,
so its cost is **O(MV size), not O(delta size)** — it re-aggregates the whole MV plus
the delta. Measured on q1's MV: **merge ≈ 11 s** for a 5k-row delta; the subsequent
**serve ≈ 20 ms**. Contrast: a full source re-author ≈ 24 s, and a Spark full scan of
the fact is minutes.

To reduce merge time:
- **More vCPU** — the `GROUP BY` parallelizes across duckdb threads (biggest lever).
- **More RAM** — avoids spill for large MVs.
- **Coarser MV grain** — a smaller MV re-aggregates faster.
- **Bigger gateway instance class** — vCPU + RAM together.
- *Algorithmic (roadmap):* make the merge **O(delta)** by upserting only the
  affected group keys instead of re-aggregating the entire MV.

## Required setup (easy to get wrong)

1. **Explicit S3 credentials on the Iceberg REST catalog.** With only the instance
   role, the catalog's per-commit S3 credential resolution holds the SQLite commit
   transaction open long enough to fail with `SQLITE_BUSY — database is locked`, so
   **appends never commit** and there's nothing to merge. Set `AWS_ACCESS_KEY_ID` /
   `AWS_SECRET_ACCESS_KEY` in the catalog's `.env` (a scoped IAM user with
   read/write on the warehouse bucket).
2. **pyiceberg on the writer + the gateway's CDC helper.** The append writer and the
   delta engine (`cdc_service.py`, `:9401`, `/iceberg/added_files`) both need
   pyiceberg. Missing pyiceberg → `added_files` returns nothing → full re-author.
3. **Compatible pyiceberg/pyarrow.** pyiceberg 0.9.x calls
   `store_decimal_as_integer`, which needs **pyarrow ≥ 18**; the older
   `pyarrow<17` pin fails writing decimal columns (`store_returns.sr_return_amt`).
   Use **`pyiceberg==0.9.1` + `pyarrow>=18`**.
4. The client box IAM role needs `s3:PutObject` on the warehouse bucket.

If a refresh unexpectedly full-rebuilds, check `/admin/mv/wave/report` — the
`reason` tells you which of these bit you (`not per-column decomposable`,
`added_files`, `not append-only`, etc.).
