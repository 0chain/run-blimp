# Blimp prod-source kit

Stand up an **Iceberg REST catalog + S3 warehouse in your own environment**, then point a
[Blimp](https://blimp.software) cloud-native cluster at it (Production tab, BYO source) to
run the query optimizer + incremental MVs against your data. Your data stays in your
cloud; Blimp only reads it.

> **Running the cluster tests?** See **[TESTING.md](TESTING.md)** — end-to-end setup
> (hardware, VPC/S3 endpoint, env) and the full **10-test catalog** (8 pass/fail +
> 2 benchmarks): `test_cache`, `test_flow_panel`, `test_query` (incl. CDC incremental
> + upsert), `test_api`, `test_fingerprint`, `verify_panel`,
> `verify_result_freshness`, `bench_incremental`, `bench_tpcds99`. One-command run:
> **`run_e2e.sh`** (~20 min). Reference numbers: [EXPECTED_TEST_RESULTS.md](EXPECTED_TEST_RESULTS.md).
> API reference + sample output: [API.md](API.md).

## Test architecture

The tests require three components in the **same region/AZ and the same VPC**: the
prod (client) instance, the Blimp cluster, and the S3 bucket holding the TPC-DS
dataset. Every path uses private IPs; S3 is reached through a VPC gateway endpoint,
so no traffic leaves the VPC and no egress is billed.

```
┌───────────────────────── AWS region, one AZ ─────────────────────────┐
│  ┌──────────────────────────── VPC ─────────────────────────────┐    │
│  │                                                              │    │
│  │  ┌─ prod instance (client) ──┐      ┌── Blimp cluster ─────┐ │    │
│  │  │ test_cache.sh             │      │ gateway              │ │    │
│  │  │ test_query.sh             │─────▶│  S3 :9000  NFS :2049 │ │    │
│  │  │ test_api.sh               │ priv │  router :8088        │ │    │
│  │  │ Iceberg REST catalog :8181│  IP  │ eblobbers (EC 2/1)   │ │    │
│  │  │ Spark baseline            │      └──────────┬───────────┘ │    │
│  │  └────────────┬──────────────┘                 │             │    │
│  │               └───────────┬────────────────────┘             │    │
│  │                S3 VPC gateway endpoint                       │    │
│  └───────────────────────────┼───────────────────────────────── ┘    │
│                   ┌──────────▼───────────┐                           │
│                   │ external S3 bucket   │  TPC-DS parquet           │
│                   │ (same region)        │  store_returns, …         │
│                   └──────────────────────┘                           │
└───────────────────────────────────────────────────────────────────────┘
```

- **Prod instance** — provisioned by `setup_tests.sh`, sized the same as the gateway
  (a smaller client bottlenecks every number). Runs the three test sets, the Iceberg
  REST catalog (:8181), and the Spark baseline.
- **Blimp cluster** — gateway + eblobbers, provisioned from `blimp.software`. The
  tests hit the gateway's private IP (S3 :9000, NFS :2049, router :8088).
- **S3 bucket** — holds the TPC-DS parquet (external to the cluster); the read-through
  cache and the query suite read it via the VPC S3 gateway endpoint.

Setup details (VPC endpoint, IAM instance profile, env): [TESTING.md](TESTING.md).

## What it is

A `docker-compose` running `tabulario/iceberg-rest` (a JDBC/SQLite-backed Iceberg REST
catalog, port **8181**) over an **S3** (or MinIO) warehouse, plus a `seed_tpcds.py` helper to
create a test table.

## Run it

```bash
cp .env.example .env      # set WAREHOUSE (s3://your-bucket/iceberg_wh) + AWS creds/region
mkdir -p catalog          # persistent sqlite metadata (must be writable)
docker compose up -d
curl -s localhost:8181/v1/config          # {"defaults":{},"overrides":{}}  = catalog up

# seed a table (needs: pip install "pyiceberg[s3fs]" pyarrow)
python3 seed_tpcds.py --catalog http://localhost:8181 \
  --warehouse s3://your-bucket/iceberg_wh --namespace tpcds --table store_returns --rows 100000
```

## Point Blimp at it

In your cluster's **Query Optimizer → Production** tab:

| Field | Value |
|---|---|
| Iceberg REST URL | `http://<this-host>:8181` |
| Warehouse | `s3://your-bucket/iceberg_wh` |
| S3 endpoint | `https://s3.<region>.amazonaws.com` (or your MinIO URL) |
| S3 access/secret key | **leave blank** if the Blimp cluster is in the **same AWS account** (it reads via its instance role); fill in only for cross-account |
| Namespace | `tpcds` |
| Tables | blank (inferred from SQL) or `store_returns,date_dim,…` |

Then **Run Production Query**. Blimp ingests only the query's tables onto the cluster
blobbers, materializes an MV, and serves it (warm re-runs are sub-second).

## Continuous CDC (incremental MVs)

Append to a table and notify the cluster so the MV refreshes incrementally:

```bash
# append rows (re-run seed_tpcds.py), then tell the cluster the source changed:
curl -s -X POST http://<gateway>:9000/admin/source/snapshot_changed \
  -H "Authorization: Bearer zus-<cluster-id>" -H "Content-Type: application/json" \
  -d '{"namespace":"tpcds","table":"store_returns","trigger":"customer"}'
```

Or wire it into your writer / an S3-event notification so every commit auto-triggers a
refresh. See the Blimp docs: *Incremental materialized views (CDC)* and *Prod-Query & MV API
reference*.

## Networking

The Blimp cluster's gateway must reach this host on **:8181** (open that port in your
firewall/security group to the cluster's gateway IP). S3 reads go directly to your bucket.

## Notes

- **Writable catalog dir**: the sqlite catalog (`./catalog/cat.db`) *and its directory* must be
  writable by the container, or commits fail with SQLite "readonly database".
- **MinIO / S3-compatible**: uncomment `CATALOG_S3_ENDPOINT` + path-style in `docker-compose.yml`.

## More docs

- **[EXPECTED_TEST_RESULTS.md](./EXPECTED_TEST_RESULTS.md)** — measured TPC-DS q1 comparison on SF1000: full scan vs.
  Spark-on-MV vs. Blimp's DuckDB vs. cached, plus one-time build and ~4 s incremental refresh.
- **[API.md](./API.md)** — the customer-facing Prod-Query API, `test_api.sh` (a
  safe-by-default API walker → [API_RESULTS.md](./API_RESULTS.md)), and `gen_incr_delta.py`.
