# Blimp prod-source kit

## Blimp

**Efficient data. Simple, small, scalable cache and MVs — autonomous operation as a sidecar to your existing production pipeline.**

Blimp is an ACID cache and autonomous materialized view on Iceberg — an efficient per-core cache (2–4 GB/s per node) that keeps your AI/ML context fresh with CDC delta-merges in seconds. One small, simple, scalable node runs in your own VPC; point your existing engines and Iceberg catalog at it to simplify your pipeline and lower inference and training cost — no data migration, no rewrites. → [blimp.software](https://blimp.software)


Stand up an **Iceberg REST catalog + S3 warehouse in your own environment**, then point a
[Blimp](https://blimp.software) cloud-native cluster at it (Production tab, BYO source) to
run the query optimizer + incremental MVs against your data. Your data stays in your
cloud; Blimp only reads it.

> **Running the cluster tests?** See **[TESTING.md](TESTING.md)** — end-to-end setup
> (hardware, VPC/S3 endpoint, env) and the full **8-test catalog** (7 pass/fail +
> 1 benchmark): `test_cache`, `test_flow_panel`, `test_query` (incl. CDC incremental
> + upsert), `test_api`, `test_fingerprint`, `verify_panel`,
> `verify_result_freshness`, `bench_incremental` (incremental/additive queries only).
> One-command run:
> **`run_e2e.sh`** (~20 min). Reference numbers: [EXPECTED_TEST_RESULTS.md](EXPECTED_TEST_RESULTS.md).
> API reference + sample output: [API.md](API.md).
>
> **Exact reproducible transcript** (every command + real output from a live
> fresh-node run): **[WALKTHROUGH.md](WALKTHROUGH.md)**.

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
│  │  └────────────┬──────────────┘      └──────────┬───────────┘ │    │
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
  REST catalog (:8181). (Correctness referee is gateway-side: a no-MV full
  source scan compared against the MV-served result.)
- **Blimp cluster** — gateway + eblobbers, provisioned from `blimp.software`. The
  tests hit the gateway's private IP (S3 :9000, NFS :2049, router :8088).
- **S3 bucket** — holds the TPC-DS parquet (external to the cluster); the read-through
  cache and the query suite read it via the VPC S3 gateway endpoint.

Setup details (VPC endpoint, IAM instance profile, env): [TESTING.md](TESTING.md).

## Install

`blimp` is a self-contained CLI — install it once, then it bootstraps its own
prerequisites on first `--setup`. Two supported ways to get it:

```bash
# 1. one-line installer (open repo, no Docker) — puts `blimp` on your PATH
curl -fsSL https://raw.githubusercontent.com/0chain/run-blimp/main/install.sh | sh

# 2. or just clone and run in place
git clone https://github.com/0chain/run-blimp && cd test-blimp && ./blimp
```

You need **nothing** pre-installed — `blimp --setup` installs what it uses
(docker for the catalog, python + pyiceberg, aws CLI, duckdb, unzip). Set
`BLIMP_SKIP_DEPS=1` on hardened/offline hosts to manage deps yourself.

> **Docker alternative** (optional): a prebuilt image bundles everything, for
> hosts where you'd rather not install anything on the OS:
> ```bash
> docker build -t blimp-kit . && \
> docker run --rm --network host -e CLUSTER_ID=… -e WAREHOUSE=s3://… blimp-kit --setup
> ```
> `--network host` lets it use the node's IAM role + reach the gateway private
> IP (that's what makes vpc mode key-free). See `docker-compose.yml` to bring up
> the catalog + CLI together.

## Quick start — the `blimp` command, beginning to end

```
blimp                 list the commands
blimp --setup         connect a Blimp cluster to this node's data (interactive)
blimp --query         test: query optimizer + incremental CDC (q1 delta-merge)
blimp --storage       test: storage & read-through cache suite
blimp --bench         bench: author / materialize / delta-merge timing profile
blimp --all           --storage then --query
```

Wiring is saved to `~/.blimp_env` by `--setup`; every test command reads it.
**Running a test with no wiring offers to run `--setup` for you first.**

### Step 1 — create a cluster

blimp.software → Create Cluster. Note the **cluster id**.

### Step 2 — `./blimp --setup` on your node

Fully **interactive** — every value is prompted with a default (Enter accepts);
any env var already set skips its prompt (that's the zero-touch/CI path):

```
AWS region [ap-south-1]:
Blimp cluster id (from blimp.software): 1784970467881
```

It then runs the **network assessment**: probes the gateway's *private* IP.
Reachable → **vpc mode** (private IPs, IAM instance role, no keys typed).
Not reachable (other cloud/account) → **external mode** (public DNS
`zus-<id>-0.zus.network`, explicit S3 keys):

```
network assessment → MODE=vpc (gateway private 10.10.12.249 reachable: yes)
  gateway → 10.10.12.249 · advertise this node as → 10.10.12.168 · S3 creds → blank (IAM role)
```

Remaining prompts: gateway (auto-filled), namespace, existing Iceberg REST URL
(blank = stand one up here via docker), S3 endpoint, **data bucket**,
warehouse (defaults to `s3://<data-bucket>/wh`), S3 keys (blank in vpc mode).

Guardrails `--setup` enforces (each is a real failure mode):

1. **Warehouse co-located with the data bucket.** A warehouse in a different
   bucket breaks the gateway's catalog-metadata reads → MV author refuses
   ("grain not sampleable"). Divergence warns and offers to fix.
2. **Bucket access grant** (vpc/same-account): applies a bucket policy for the
   gateway's instance role + your account — no silent 403 at author time.
3. **Blank keys = IAM instance role** — the correct vpc answer; real keys are
   only ever typed in external mode.

At the end it prints the exact values for the cluster UI (Query Optimizer →
Production) and the `snapshot_changed` webhook for your pipeline.

### Step 3 — register your tables (if not already Iceberg)

```
venv/bin/python3 register_tpcds_tables.py \
  --catalog http://localhost:8181 --warehouse s3://my-bucket/wh \
  --source-bucket my-bucket --region ap-south-1 --namespace myns
```

(`add_files` registration — no data copy.)

### Step 4 — point the cluster at the source

Paste `--setup`'s printed values into the cluster UI (Production tab) — or,
scripted (SSH/SSM as root on the gateway):

```
ICEBERG_URL=http://<node>:8181 WAREHOUSE=s3://my-bucket/wh NAMESPACE=myns \
ORIGIN_BUCKET=my-bucket S3_REGION=ap-south-1 bash hookup_cluster_source.sh
```

> Firewall: the gateway must reach this node on the catalog port. If the
> cluster SG doesn't open 8181, serve the catalog on an open port
> (e.g. `docker run -p 8081:8181 …`) and use that URL.

### Step 5 — `./blimp --query` (prove authoring + CDC)

Defaults to **q1** on `store_returns`: ① cold author (`force_author`) →
② append rows + fire the webhook → ③ verify the gateway **delta-merged**
incrementally (the refresh runs async on the webhook; the gate reads
`/admin/mv/wave/report` and requires `mode=incremental`):

```
q1  store_returns  merge_ms=3523  mode=incremental   RESULT: PASS
```

Wider: `SUITES="store_sales:3 19 43 52 55;store_returns:1" ./blimp --query`

### Step 6 — `./blimp --storage` / `./blimp --bench`

Storage = warp S3 PUT/GET + TTFB, fio NFS, mlperf (tools via
`setup_tests.sh`). Cap sizes on small nodes:
`WARP_BUDGET_MIB=5120 FIO_JOBS=4 FIO_SIZE=1280M MLPERF_NUM_FILES=35 ./blimp --storage`.
Bench = min/median/avg/max author / materialize / delta-merge profile
(`ITERS AUTHOR_ITERS CDC_ROWS`).

### Step 7 — router corner cases (operator)

`test_router_freshness.sh` toggles the gateway's router flags via SSM and
proves both invariants: **router ON** → an append still changes the served
result (immutable Iceberg files = new keys = no stale hit); **router OFF** →
a source read writes **zero** bytes to the cache.

### Step 8 — cluster stop/start (self-heal, no touch)

Raw EC2 stop/start: private IPs survive (in-VPC wiring reconnects untouched);
public IPs change and the control plane's 60s cron reconciles the DB + every
`zus-<id>-N` DNS record (gateway **and** blobbers) in ~1–2 min. MVs + wiring
persist — re-run `--query`: the MV serves without re-author, CDC keeps merging.

### Credentials model

- **vpc mode: nothing typed.** Node + gateway each use their IAM instance
  role (EC2 metadata); bucket access is a policy naming the role.
- **external mode:** S3 keys typed once into `--setup` (`~/.blimp_env`,
  mode 600); pasted into the cluster UI they are stored in **AWS Secrets
  Manager** — only the ARN reaches cluster config, never state/logs.
- Gateway admin API: `Authorization: Bearer zus-<CLUSTER_ID>`.

---

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
  Blimp's DuckDB on the MV vs. cached, plus one-time build and ~4 s incremental refresh.
- **[API.md](./API.md)** — the customer-facing Prod-Query API, `test_api.sh` (a
  safe-by-default API walker → [API_RESULTS.md](./API_RESULTS.md)), and `gen_incr_delta.py`.

---

## Verified walkthrough (real commands + output)

A fresh Ubuntu 24.04 EC2 node in the cluster's VPC, cluster `1784970467881`.
Every line below is the actual command and its actual output from a live run.

**1. Confirm the node's identity + IAM role (no keys anywhere).**
```
$ whoami; hostname; hostname -I
ubuntu
ip-10-10-12-62
10.10.12.62 172.17.0.1

$ curl -s -H "X-aws-ec2-metadata-token: $TOK" \
    http://169.254.169.254/latest/meta-data/iam/security-credentials/
ec2-ssm-role-1784970467881
```

**2. One command does setup end-to-end** (deps → mode → catalog → wiring).
`blimp --setup` on the bare box:
```
== blimp --setup — connect a Blimp cluster to this node's data ==
== checking prerequisites ==
  installing: docker-compose-v2               # ← installs its own missing deps
  ✓ deps ready (python: /home/ubuntu/.blimp_venv/bin/python3)

network assessment → MODE=vpc (gateway private 10.10.12.249 reachable: yes)
  gateway → 10.10.12.249 · advertise this node as → 10.10.12.62 · S3 creds → blank (IAM instance role)

standing up an Iceberg REST catalog on :8181 over s3://blimp-tpcds-sf1-aps1/wh3
 Container iceberg-rest  Started
  Iceberg catalog up — cluster reaches it at http://10.10.12.62:8181
saved wiring -> /home/ubuntu/.blimp_env

================= POINT THE BLIMP CLUSTER AT THIS SOURCE =================
  Iceberg REST URL : http://10.10.12.62:8181
  Warehouse        : s3://blimp-tpcds-sf1-aps1/wh3
  Namespace        : tpcds_sf1x
setup done — validate with:  blimp --query   (and  blimp --storage )
```
(The `installing: docker-compose-v2` line was later removed — the catalog now
starts with a plain `docker run`, so the client box needs only the docker
engine, no compose plugin.)

**3. Register your parquet as Iceberg** (once):
```
$ ~/.blimp_venv/bin/python3 register_tpcds_tables.py --catalog http://localhost:8181 \
    --warehouse s3://my-bucket/wh --source-bucket my-bucket --namespace tpcds_sf1x
registered 24/24 tables into http://localhost:8181 ns=tpcds_sf1x
```

**4. `blimp --query` — author + incremental CDC:**
```
== CDC bench: cluster=1784970467881 gw=10.10.12.249 rows/append=20000 suites=[store_returns:1] ==
>> phase 1: author all (force_author)
   q1: author=8489 materialize=3953 cold_serve=2507ms mv_rows=251737 mv=mv_10490a66ba4e398b
>> phase 2: append +20000 to store_returns + snapshot_changed
   tpcds_sf1x.store_returns: +20000 rows -> snapshot 4844583755386112254
>> phase 3: re-run all (incremental)

============================== CDC CONTRIBUTIONS ==============================
query      fact              mv_rows   author_ms   merge_ms     mode   incr_ms
q1         store_returns      271490           ?       3523 incremental      2557
RESULT: PASS (1/1 authored + delta-merged)
```

**5. Router corner cases** (operator, via SSM):
```
TEST A+B — router ON: append visible through warm cache
  PASS router ON: result changed after append (93707b6c… -> 1e21abc5…)
TEST C — router OFF: source not cached on blobbers
  PASS router OFF: cache bytes unchanged (52146)
RESULT: 2 passed, 0 failed
```

**6. Raw stop/start (all 4 instances) → self-heal + re-validate:**
```
private IPs unchanged; all 4 public IPs changed
DNS reconciled by the 60s cron (no touch):
  zus-1784970467881-0 -> 13.201.26.189   (gateway)
  zus-1784970467881-1 -> 3.110.120.231   (blobber-1)
  zus-1784970467881-2 -> 13.127.74.12    (blobber-2)
  zus-1784970467881-3 -> 3.111.245.151   (blobber-3)
post-restart q1: merge_ms=5138 mode=incremental   RESULT: PASS
```

**7. External-cloud host** (non-AWS box, over public DNS):
```
network assessment → MODE=external (gateway private unknown reachable: no)
live query over zus-1784970467881-0.zus.network:9000
  {status: ok, rows: 1, author_ms: 575, query_ms: 2447, md5: 7a26dcec…}
```

**8. `blimp --storage` numbers** (2/1 cluster, 5 GB warp set):
```
warp   S3 PUT 749 MiB/s · GET 1673 MiB/s   (0 errors)
fio    NFS write 1137 MiB/s · read 937 MiB/s
```

Net: bare node → **two commands** (`install.sh`, `blimp --setup`) → a wired,
validated Blimp source, with authoring, incremental CDC, cache correctness,
stop/start self-heal, and cross-cloud all proven — zero credentials typed in
vpc mode.
