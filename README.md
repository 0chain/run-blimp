# Blimp prod-source kit

## Blimp

Blimp is an ACID cache and autonomous materialized view on Iceberg — an efficient per-core cache (2–4 GB/s per Blimp node) that keeps your AI/ML context fresh with CDC delta-merges served queries in 1-2s. One small, simple, scalable Blimp node runs in your own VPC; point your existing engines and Iceberg catalog at it to simplify your pipeline and lower inference and training cost. → [blimp.software](https://blimp.software)

`blimp` connects a **Blimp node** to **your** data which stays in your environment; Blimp
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

**All three (same VPC, cross-region/peered, different cloud) are supported and
proven by this kit for demo/test setup — pick whichever matches where your
Iceberg node already lives.** For **production**, run the Iceberg node in the
**same VPC as the Blimp node** (same account, same region — ideally the same
AZ): that path uses private IPs and the IAM instance role with no S3 keys, and
every S3 call to the origin stays on the VPC S3 gateway endpoint with no
internet egress. Cross-region, cross-account, or cross-cloud setups work, but
add network hops and (outside same-account/region) real data-transfer egress
cost on every read — fine for a demo or a one-off test, but not the
recommended production topology.


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
blimp --query         query optimizer + incremental CDC delta-merge
blimp --storage       storage & read-through cache suite
blimp --bench         author / materialize / delta-merge timing profile
```

Wiring is saved to `~/.blimp_env` by `--setup`; every command reads it.
**Running a command with no wiring offers to run `--setup` for you first.**

**Zero-touch / CI:** every prompt is skipped when its env var is pre-set —
export these (or source a file with `set -a`) and `--setup` runs unattended:

```
REGION CLUSTER_ID ICEBERG_URL WAREHOUSE ORIGIN_BUCKET NAMESPACE \
S3_KEY S3_SECRET        # external mode only; blank/unset in vpc mode
GW GW_AK GW_SK          # optional — auto-derived from CLUSTER_ID when unset
```

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

**What `--setup` does, in order:**

1. **Deps bootstrap** — installs docker / python venv + pyiceberg / aws CLI /
   unzip if missing (`BLIMP_SKIP_DEPS=1` to manage yourself).
2. **Network assessment** — probes the gateway's private IP → picks vpc mode
   (IAM role, no keys) or external mode (public DNS, explicit keys).
3. **Catalog** — reuses your Iceberg REST URL, or stands one up here via
   `docker run` (iceberg-rest on :8181 over your warehouse).
4. **Bucket grant** (vpc/same-account) — applies the bucket policy so the
   gateway role can read; skipped in external mode (bring your own grant).
5. **Saves the wiring** to `~/.blimp_env` (mode 600) for every later command.
6. **Wires the Blimp node over its admin API** — no SSH, no restart:

   ```
   POST http://<gateway>:9000/admin/source/configure
   Authorization: Bearer zus-<CLUSTER_ID>
   {"source":"customer","iceberg_url":"http://<node>:8181","warehouse":"s3://…/wh",
    "namespace":"…","bucket":"…","s3_key":"…","s3_secret":"…","s3_region":"…"}
   ```

   The gateway applies this to its **live** source config (`ZS3_SRC_CUSTOMER_*`)
   — the very next query reads your data — and persists it so restarts keep the
   wiring. Keys travel over the authenticated admin API and are stored root-only
   on the gateway volume. On success `--setup` prints
   `✓ cluster wired: source=customer … (live, no restart)`.

Finally it prints the same values for the Blimp node UI (Query Optimizer →
Production, the manual path) and the `snapshot_changed` webhook for your
pipeline.

### Step 3 — register your tables (if not already Iceberg)

```
venv/bin/python3 register_tpcds_tables.py \
  --catalog http://localhost:8181 --warehouse s3://my-bucket/wh \
  --source-bucket my-bucket --region ap-south-1 --namespace myns
```

(`add_files` registration — no data copy.)

### Step 4 — point the Blimp node at the source

**Automatic (no SSH):** `--setup` wires the cluster itself over the
authenticated admin API — `POST http://<gw>:9000/admin/source/configure`
(`Authorization: Bearer zus-<CLUSTER_ID>`). The gateway applies the source in
its live env (effective on the next query, **no restart**) and persists it
across reboots. Requires a gateway image ≥ 2026-07-27; on older images the
call fails gracefully and `--setup` prints the manual steps.

Manual fallback (older gateway image, or the admin-API call failed): paste
`--setup`'s printed values into the Blimp node UI (Production tab). `--setup`
also retargets the read-through cache router at the new bucket itself
(`POST /admin/cache/config`) — that isn't a separate manual step.

External / cross-account mode **requires** `S3_KEY`/`S3_SECRET`; `--setup`
sends them in the `/admin/source/configure` body. In vpc mode leave them
unset — the gateway reads via its IAM role.

> Firewall: the gateway must reach this node on the catalog port. If the
> Blimp node security group doesn't open 8181, serve the catalog on an open port
> (e.g. `docker run -p 8081:8181 …`) and use that URL.

## Testing the Blimp node

The `--query`, `--storage`, and `--bench` commands are **testing/validation
tools** — they prove the wiring, measure the node, and gate a rollout. They are
not part of production operation (production is your pipeline + the
`snapshot_changed` webhook from Step 4).

### A — `./blimp --query` (prove authoring + CDC)

Defaults to **q9 q88 q14 q64 q4** on `store_sales`: ① serve/author each →
② append rows to all five facts + fire the webhook → ③ re-run and report how
each MV refreshed. CDC is **lazy**: the webhook only marks the MV stale — the
*next query* pays the merge and reports `merge_ms` inline (read from the
phase-3 query response, with `/admin/mv/wave/report` as fallback):

```
q64  store_sales  merge_ms=8856  mode=incremental
q4   store_sales  author_ms=45048  no MV — served from base
```

There is **no PASS/FAIL verdict**. The suite reports what each query did and
you judge it: `mode=incremental` is the delta-merge fast path, `no-delta` is a
full re-author, and "no MV" means the query fell back to scanning base. Nothing
here asserts a threshold, because the interesting outcomes (a query that authors
no MV, a merge that silently measured unchanged data) are not binary.

Add `--verify` to check the merged rows are *correct* rather than merely fast —
off by default because the gateway runs no verification while serving, so an
unflagged run measures the production path.

Wider: `SUITES="store_sales:3 19 43 52 55;store_returns:1" ./blimp --query`

Join-CTE queries (q64-class, `SUITES="catalog_sales:64"`) only see a delta
when the append touches **both** sides of the join — the seeder therefore
appends `catalog_sales` **referentially** (matching `catalog_returns` rows,
keys copied). A sales-only append correctly reports `no-delta`, not a bug.

### B — `./blimp --storage` (storage & cache suite)

Storage = warp S3 PUT/GET + TTFB, fio NFS, mlperf. The suite is
**self-contained**: it installs its own tools first (warp pinned v1.1.4, fio,
mount-s3, dlio) — `BLIMP_SKIP_DEPS=1` opts out on hardened hosts, and a
section whose tool still can't install is skipped loudly. Cap sizes on small
nodes:
`WARP_BUDGET_MIB=5120 FIO_JOBS=4 FIO_SIZE=1280M MLPERF_NUM_FILES=35 ./blimp --storage`.

### C — `./blimp --bench` (timing profile)

Bench = min/median/avg/max author / materialize / delta-merge profile
(`ITERS AUTHOR_ITERS CDC_ROWS`, all defaulting to 3/3/50000; `BENCH_QNR=64`
benches a different TPC-DS query from `$Q_DIR/q<N>.sql` instead of the built-in
q1; `BENCH_FACT=catalog_sales` appends referentially for join-CTE queries).

Check the `n=` on each summary line before quoting it — an append that
re-materializes instead of merging contributes no `delta_merge_ms`, so `n` can
be lower than `ITERS`. See the worked example in the walkthrough below for what
the numbers do and don't mean.

## Notes

**Router cache-freshness guarantee.** Router ON: an append still changes the
served result (immutable Iceberg files = new keys = no stale hit). Router OFF:
a source read writes zero bytes to the cache.

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
- **[TESTING.md](TESTING.md)** — testing the kit itself (`./test_kit.sh`), not
  the Blimp node.
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

### Testing the Blimp node (walkthrough)

The remaining items are the TESTING commands — validation of the wired node,
not production operation.

**A. `blimp --query` — author + incremental CDC:**

Five queries against a single fact, appending to all five facts each cycle so a
multi-fact query exercises a multi-fact merge. Live run, AWS SF1 cluster
`1785550395356`, 2026-08-01:
```
== CDC bench: cluster=1785550395356 gw=10.10.114.87 rows/append=5000 suites=[store_sales:9 88 14 64 4] ==
==== fact: store_sales (q9 q88 q14 q64 q4) ====
>> phase 1: serve/author all (force_author=0)
   q9: author=? materialize=? cold_serve=2826ms mv=?x? (mv_h_0e90d0b63216)
   q88: author=? materialize=? cold_serve=2668ms mv=?x? (mv_h_ae8ceee390cc)
   q14: author=12252 materialize=? cold_serve=3134ms mv=?x? (none)
   q64: author=? materialize=? cold_serve=2657ms mv=?x? (mv_h_ad47f860697f)
   q4: author=45048 materialize=? cold_serve=2971ms mv=?x? (none)
>> phase 2: append +5000 to [store_sales store_returns catalog_sales catalog_returns web_sales] + snapshot_changed
tpcds.store_sales: +5000 rows -> snapshot 3827416906052195600
tpcds.store_returns: +5000 rows -> snapshot 686337096249913447
tpcds.catalog_returns: +1666 referential rows -> snapshot 6382376194974615624
tpcds.catalog_returns: +5000 rows -> snapshot 7838483972766842389
tpcds.web_sales: +5000 rows -> snapshot 4945461575749320971
>> phase 3: re-run all (incremental)

============================== CDC CONTRIBUTIONS ==============================
query      fact             mv_rows x cols   author_ms   merge_ms     mode   incr_ms
q9         store_sales                 ?x?           ?       5879 incremental      2781
q88        store_sales                 ?x?           ?       6507 incremental      2693
q14        store_sales                 ?x?       12252          -        -      3031
q64        store_sales                 ?x?           ?       8856 incremental      3875
q4         store_sales                 ?x?       45048          -        -      2959
  q9: MV mv_h_0e90d0b63216 — delta-merged
  q88: MV mv_h_ae8ceee390cc — delta-merged
  q14: no MV — served from base
  q64: MV mv_h_ad47f860697f — delta-merged
  q4: no MV — served from base
```
How to read it: `mode=incremental` with a `merge_ms` means the MV was refreshed
by an append-only delta-merge (the O(|MV|+|delta|) fast path). A query with no
MV serves from base and reports `author_ms` instead — q14 and q4 do that here,
which is a real gap, not a pass/fail. There is no PASS/FAIL line: the suite
reports what happened and you judge it.

An append that fails leaves nothing to merge, and phase 3 then measures
UNCHANGED data while still printing plausible-looking `no-delta` rows. The suite
now prints the seeder's full traceback and says so explicitly when that happens
— if you see `APPEND FAILED`, every number below it is meaningless.

**B. Raw stop/start (all 4 instances) → self-heal + re-validate:**
```
private IPs unchanged; all 4 public IPs changed
DNS reconciled by the 60s cron (no touch):
  zus-1784970467881-0 -> 13.201.26.189   (gateway)
  zus-1784970467881-1 -> 3.110.120.231   (blobber-1)
  zus-1784970467881-2 -> 13.127.74.12    (blobber-2)
  zus-1784970467881-3 -> 3.111.245.151   (blobber-3)
post-restart q1: merge_ms=5138 mode=incremental   RESULT: PASS
```

**C. External-cloud host** (non-AWS box, over public DNS):
```
network assessment → MODE=external (gateway private unknown reachable: no)
live query over zus-1784970467881-0.zus.network:9000
  {status: ok, rows: 1, author_ms: 575, query_ms: 2447, md5: 7a26dcec…}
```

**D. `blimp --storage` numbers** (2/1 cluster, 5 GB warp set):
```
warp   S3 PUT 749 MiB/s · GET 1673 MiB/s   (0 errors)
fio    NFS write 1137 MiB/s · read 937 MiB/s
```

Those are from a 2/1 cluster on larger instances. On a small node (c6in.large,
2 vCPU) the same suite gave `fio NFS write 75.0MiB/s · read 352MiB/s` — the
cache leg is only meaningful when the origin set is large enough to exercise it
(a single 17 MB object measures nothing, and reports the router as *slower* than
direct S3 purely from per-request overhead). Size the set to the cluster.

**E. `blimp --bench` — MV lifecycle timing profile:**

Phase A cold-authors the same query `AUTHOR_ITERS` times (evicting the MV
between iterations); phase B appends and refreshes `ITERS` times. Live run, AWS
SF1 cluster `1785550395356`, 2026-08-01:
```
== MV lifecycle benchmark v2: cluster 1785550395356 (authors=3, appends=3, upserts=3, rows/cycle=50000) ==
  author[1] q1 COLD status=ok author_ms=24087 materialize_ms=4024 wall_ms=27393.2 mv=mv_h_aff2e89bc41f
  author[2] q1 COLD status=ok author_ms=15188 materialize_ms=3773 wall_ms=18415.7 mv=mv_h_aff2e89bc41f
  author[3] q1 COLD status=ok author_ms=15239 materialize_ms=3705 wall_ms=18522.2 mv=mv_h_aff2e89bc41f
  append[1] refresh delta_merge_ms=? materialize_ms=3763 query_ms=2955 engine=duckdb wall_ms=21891.6
  append[2] refresh delta_merge_ms=8321 materialize_ms=? query_ms=3151 engine=duckdb wall_ms=11930.5
  append[3] refresh delta_merge_ms=6627 materialize_ms=? query_ms=3059 engine=duckdb wall_ms=10051.4

== SUMMARY (q1-scale MV over 50000-row commits) ==
  one-time author_ms (fresh queries):   n=3 min=15188 median=15239 avg=18171 max=24087 (ms)
  one-time materialize_ms:              n=3 min=3705 median=3773 avg=3834 max=4024 (ms)
  one-time wall_ms:                     n=3 min=18416 median=18522 avg=21444 max=27393 (ms)
  append  delta_merge_ms:               n=2 min=6627 median=7474 avg=7474 max=8321 (ms)
  append  refresh materialize_ms:       n=1 min=3763 median=3763 avg=3763 max=3763 (ms)
  append  commit→answer wall_ms:        n=3 min=10051 median=11930 avg=14624 max=21892 (ms)
```

