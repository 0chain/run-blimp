# Blimp prod-source kit

## Blimp

Blimp is an ACID cache and autonomous materialized view on Iceberg — an efficient per-core cache (2–4 GB/s per Blimp node) that keeps your AI/ML context fresh with CDC delta-merges in seconds. One small, simple, scalable Blimp node runs in your own VPC; point your existing engines and Iceberg catalog at it to simplify your pipeline and lower inference and training cost. → [blimp.software](https://blimp.software)

`blimp` connects a **Blimp node** to **your** data and keeps it fresh. Run it on
your own **Iceberg node** — same VPC as the Blimp node, a peered / different-region
VPC, or a different cloud entirely. It wires the Blimp node's query optimizer +
read-through cache to your Iceberg + S3 data (or **stands the source up for you** if
you have none), authors materialized views, and delta-merges each change via a
webhook your pipeline fires on commit. Your data stays in your environment; Blimp
only reads it.

```
   ┌─ your Iceberg node (any cloud) ─┐     ┌──── Blimp node (AWS) ────┐
   │  blimp CLI                      │     │  gateway                 │
   │  Iceberg REST  :8181  ──────────┼────▶│   S3 :9000 · router      │
   │  your S3 / MinIO data           │ wire│   eblobbers              │
   └───────────────┬─────────────────┘     └───────────┬──────────────┘
                   └────── snapshot_changed webhook ─────┘
                          (your pipeline, on each Iceberg commit)
```

Vpc-mode reaches the Blimp node's gateway on its private IP with the Iceberg node's
IAM role (no keys); cross-region / cross-account / non-AWS reach it over public DNS.
`blimp --setup` detects which and wires it accordingly.


## Install

`blimp` is a self-contained CLI — install it once, then it bootstraps its own
prerequisites on first `--setup`. Two supported ways to get it:

```bash
# 1. one-line installer (open repo, no Docker) — puts `blimp` on your PATH
curl -fsSL https://raw.githubusercontent.com/0chain/run-blimp/main/install.sh | sh

# 2. or just clone and run in place
git clone https://github.com/0chain/run-blimp && cd run-blimp && ./blimp
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
blimp --setup         connect a Blimp node to your data (interactive)
blimp --query         query optimizer + incremental CDC (q1 delta-merge)
blimp --storage       storage & read-through cache suite
blimp --bench         author / materialize / delta-merge timing profile
```

Wiring is saved to `~/.blimp_env` by `--setup`; every command reads it.
**Running a command with no wiring offers to run `--setup` for you first.**

### Step 1 — create a Blimp node

blimp.software → Create a Blimp node. Note the **node id**.

### Step 2 — `./blimp --setup` on your Iceberg node

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

At the end it prints the exact values for the Blimp node UI (Query Optimizer →
Production) and the `snapshot_changed` webhook for your pipeline.

### Step 3 — register your tables (if not already Iceberg)

```
venv/bin/python3 register_tpcds_tables.py \
  --catalog http://localhost:8181 --warehouse s3://my-bucket/wh \
  --source-bucket my-bucket --region ap-south-1 --namespace myns
```

(`add_files` registration — no data copy.)

### Step 4 — point the Blimp node at the source

Paste `--setup`'s printed values into the Blimp node UI (Production tab) — or,
scripted (SSH/SSM as root on the gateway):

```
ICEBERG_URL=http://<node>:8181 WAREHOUSE=s3://my-bucket/wh NAMESPACE=myns \
ORIGIN_BUCKET=my-bucket S3_REGION=ap-south-1 bash hookup_cluster_source.sh
```

> Firewall: the gateway must reach this node on the catalog port. If the
> Blimp node security group doesn't open 8181, serve the catalog on an open port
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

Storage = warp S3 PUT/GET + TTFB, fio NFS, mlperf (needs `warp`, `fio`,
`mount-s3` on the node). Cap sizes on small nodes:
`WARP_BUDGET_MIB=5120 FIO_JOBS=4 FIO_SIZE=1280M MLPERF_NUM_FILES=35 ./blimp --storage`.
Bench = min/median/avg/max author / materialize / delta-merge profile
(`ITERS AUTHOR_ITERS CDC_ROWS`).

## Notes

**Router corner cases (operator).** `test_router_freshness.sh` toggles the
gateway's router flags via SSM and proves both invariants: **router ON** → an
append still changes the served result (immutable Iceberg files = new keys = no
stale hit); **router OFF** → a source read writes **zero** bytes to the cache.

**Blimp node stop/start (self-heal, no touch).** Raw EC2 stop/start: private IPs
survive (in-VPC wiring reconnects untouched); public IPs change and the control
plane's 60s cron reconciles the DB + every `zus-<id>-N` DNS record (gateway
**and** blobbers) in ~1–2 min. MVs + wiring persist — re-run `--query`: the MV
serves without re-author, CDC keeps merging.

**Credentials model.**
- **vpc mode: nothing typed.** Iceberg node + gateway each use their IAM instance
  role (EC2 metadata); bucket access is a policy naming the role.
- **external mode:** S3 keys typed once into `--setup` (`~/.blimp_env`,
  mode 600); pasted into the Blimp node UI they are stored in **AWS Secrets
  Manager** — only the ARN reaches Blimp node config, never state/logs.
- Gateway admin API: `Authorization: Bearer zus-<CLUSTER_ID>`.

---

## Reference

- **[WALKTHROUGH.md](WALKTHROUGH.md)** — the same flow as a full transcript: every
  command and its real output, start to finish, on a fresh node.
- Product docs: [docs.zus.network/zus-docs/webapps/blimp](https://docs.zus.network/zus-docs/webapps/blimp)
  — the optimizer, incremental MVs (CDC), and the Prod-Query & MV API.

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
