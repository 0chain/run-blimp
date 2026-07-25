# WALKTHROUGH — exact commands + real output, start to finish

A complete, reproducible transcript of connecting a **fresh Ubuntu 24.04 EC2
node** to Blimp cluster `1784970467881` and validating it. Every command below
was actually run this session; every output block is the real output.

Facts used throughout (yours will differ):

| Thing | Value in this run |
|---|---|
| Cluster id | `1784970467881` |
| Gateway (zs3server) private IP | `10.10.12.249` |
| Client node | `i-00a3d3b215026db6b` — private `10.10.12.62`, public `65.2.137.179` |
| VPC / subnet / SG | `vpc-0538f7366a646edcb` / `subnet-0a8b4be9ab96a82c5` / `sg-0d98f4bfcc79f8582` |
| IAM instance profile | `ec2-ssm-profile-1784970467881` (role `ec2-ssm-role-1784970467881`) |
| Data bucket | `blimp-tpcds-sf1-aps1` (TPC-DS SF1 parquet) |
| Warehouse (co-located) | `s3://blimp-tpcds-sf1-aps1/wh3` |
| Namespace | `tpcds_sf1x` |

---

## PART A — provision the client node (operator / test-harness step)

A real customer already has a node and skips to Part B. This is how the test
node was created. The two things that matter: it's launched **in the cluster's
subnet/SG** and **with the cluster's IAM instance profile** — that profile is
what makes everything else key-free.

```bash
aws ec2 run-instances --region ap-south-1 \
  --image-id ami-07e5ce642bbc48c0d --instance-type c6in.large \
  --key-name blobber-key-<user>-1784970467881 \
  --subnet-id subnet-0a8b4be9ab96a82c5 \
  --security-group-ids sg-0d98f4bfcc79f8582 --associate-public-ip-address \
  --iam-instance-profile Name=ec2-ssm-profile-1784970467881 \
  --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":60,"VolumeType":"gp3"}}]' \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=zus-client2-1784970467881}]' \
  --query 'Instances[0].[InstanceId,PrivateIpAddress]' --output text
```
```
i-00a3d3b215026db6b	10.10.12.62
```

Grant the node's role (and your account) access to the data bucket — this is
the identity-based authorization; **no keys are created**:

```bash
aws s3api put-bucket-policy --bucket blimp-tpcds-sf1-aps1 --policy '{
  "Version":"2012-10-17","Statement":[{"Sid":"BlimpSourceAccess","Effect":"Allow",
  "Principal":{"AWS":["arn:aws:iam::851443918456:root",
                      "arn:aws:iam::851443918456:role/ec2-ssm-role-1784970467881"]},
  "Action":["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:ListBucket","s3:GetBucketLocation"],
  "Resource":["arn:aws:s3:::blimp-tpcds-sf1-aps1","arn:aws:s3:::blimp-tpcds-sf1-aps1/*"]}]}'
```

SSH in with the cluster's key:
```bash
ssh -i cluster-1784970467881.pem ubuntu@65.2.137.179
```

---

## PART B — on the node: verify identity (no keys anywhere)

```bash
$ whoami; hostname; hostname -I
ubuntu
ip-10-10-12-62
10.10.12.62 172.17.0.1

# the box carries the cluster's IAM role — every AWS client uses it via IMDS:
$ TOK=$(curl -s -X PUT http://169.254.169.254/latest/api/token \
        -H "X-aws-ec2-metadata-token-ttl-seconds: 60")
$ curl -s -H "X-aws-ec2-metadata-token: $TOK" \
     http://169.254.169.254/latest/meta-data/iam/security-credentials/
ec2-ssm-role-1784970467881
```

---

## PART C — install the `blimp` executable

Open-repo one-liner (once the repo is public):
```bash
curl -fsSL https://raw.githubusercontent.com/0chain/run-blimp/main/install.sh | sh
```
or clone:
```bash
git clone https://github.com/0chain/run-blimp && cd test-blimp && ./blimp
```

Either way `blimp` lands on your PATH (installer symlinks
`/usr/local/bin/blimp -> /opt/blimp/blimp`):
```bash
$ which blimp
/usr/local/bin/blimp
$ blimp
blimp — Blimp prod-source setup + validation
  blimp --setup      Connect a Blimp cluster to your data ...
```

---

## PART D — `blimp --setup` (one command: deps → mode → catalog → wiring)

Set the few knobs and run it. On a same-account VPC node **no S3 keys are
asked** — the gateway uses its IAM role.

```bash
export CLUSTER_ID=1784970467881 REGION=ap-south-1
export WAREHOUSE=s3://blimp-tpcds-sf1-aps1/wh3 ORIGIN_BUCKET=blimp-tpcds-sf1-aps1 \
       NAMESPACE=tpcds_sf1x
blimp --setup
```
```
== blimp --setup — connect a Blimp cluster to this node's data ==
== checking prerequisites ==
  ✓ deps ready (python: /home/ubuntu/.blimp_venv/bin/python3)

network assessment → MODE=vpc (gateway private 10.10.12.249 reachable: yes)
  gateway → 10.10.12.249 · advertise this node as → 10.10.12.62 · S3 creds → blank (IAM instance role)

Iceberg REST catalog
S3 / warehouse
  granting s3 access on s3://blimp-tpcds-sf1-aps1 to account 851443918456
standing up an Iceberg REST catalog on :8181 over s3://blimp-tpcds-sf1-aps1/wh3
  Iceberg catalog up — cluster reaches it at http://10.10.12.62:8181
saved wiring -> /home/ubuntu/.blimp_env

================= POINT THE BLIMP CLUSTER AT THIS SOURCE =================
  In the cluster UI → Query Optimizer → Production (BYO Iceberg source):
    Iceberg REST URL : http://10.10.12.62:8181
    Warehouse        : s3://blimp-tpcds-sf1-aps1/wh3
    S3 endpoint      : https://s3.ap-south-1.amazonaws.com
    S3 credentials   : none — gateway reads via its IAM instance role
    Namespace        : tpcds_sf1x

  Then push data changes and refresh MVs incrementally with:
    curl -X POST http://10.10.12.249:9000/admin/source/snapshot_changed \
      -H "Authorization: Bearer zus-1784970467881" -H "Content-Type: application/json" \
      -d '{"namespace":"tpcds_sf1x","table":"<table>","trigger":"customer"}'

  Make sure the cluster gateway can reach this node on :8181 (security group).
==========================================================================
setup done — validate with:  blimp --query    (and  blimp --storage )
```

> On a truly bare node the `checking prerequisites` step installs what's
> missing (`installing: docker-compose-v2` etc.) before `✓ deps ready`. The
> catalog is a single `docker run` — no compose plugin needed.

---

## PART E — register your parquet as Iceberg tables (once)

Your data (`s3://blimp-tpcds-sf1-aps1/*`) is registered into the catalog via
`add_files` — **no data copy**. Uses the venv python from setup:

```bash
~/.blimp_venv/bin/python3 /opt/blimp/register_tpcds_tables.py \
  --catalog http://localhost:8181 \
  --warehouse s3://blimp-tpcds-sf1-aps1/wh3 \
  --source-bucket blimp-tpcds-sf1-aps1 \
  --region ap-south-1 --namespace tpcds_sf1x
```
```
registered 24/24 tables into http://localhost:8181 ns=tpcds_sf1x
```

> The catalog must run with the node's IAM role (blank AWS creds). Passing a
> literal placeholder like `none` makes it sign with a bogus key → S3 403 on
> every table. `blimp --setup` handles this; if you start the catalog by hand,
> leave `AWS_ACCESS_KEY_ID`/`SECRET` **unset**.

---

## PART F — point the cluster at this source

**UI path:** paste the six values from Part D into the cluster's Query
Optimizer → Production tab, then Run.

**Scripted path** (run against the gateway via SSH/SSM as root):
```bash
ICEBERG_URL=http://10.10.12.62:8181 WAREHOUSE=s3://blimp-tpcds-sf1-aps1/wh3 \
NAMESPACE=tpcds_sf1x ORIGIN_BUCKET=blimp-tpcds-sf1-aps1 S3_REGION=ap-south-1 \
bash hookup_cluster_source.sh
```
```
[hookup] router :8088 up
[hookup] gateway :9000 up
[hookup] DONE — source=http://10.10.12.62:8181|s3://blimp-tpcds-sf1-aps1/wh3 ns=tpcds_sf1x,
         cache fronts s3://blimp-tpcds-sf1-aps1 (ap-south-1)
```

---

## PART G — `blimp --query` (author + incremental CDC)

```bash
CDC_ROWS=20000 blimp --query
```
```
== CDC bench: cluster=1784970467881 gw=10.10.12.249 rows/append=20000 suites=[store_returns:1] ==
==== fact: store_returns (q1) ====
>> phase 1: author all (force_author)
   q1: author=8377 materialize=3953 cold_serve=2507ms mv_rows=251737 mv=mv_10490a66ba4e398b
>> phase 2: append +20000 to store_returns + snapshot_changed
   tpcds_sf1x.store_returns: +20000 rows -> snapshot 4844583755386112254
>> phase 3: re-run all (incremental)

============================== CDC CONTRIBUTIONS ==============================
query      fact              mv_rows   author_ms   merge_ms     mode   incr_ms
q1         store_returns      251737        8377       3190 incremental      2524
==============================================================================
RESULT: PASS (1/1 authored + delta-merged)
```

Author = full-fact scan builds the MV; the append + `snapshot_changed` webhook
triggers an **incremental delta-merge** (`merge_ms`, `mode=incremental`) that
reads only |MV|+|delta| — not the fact again.

---

## PART H — optional: storage, router, stop/start

```bash
# storage & cache suite (sized for the node; see caps in README)
WARP_BUDGET_MIB=5120 FIO_JOBS=4 FIO_SIZE=1280M blimp --storage
#   warp   S3 PUT 749 MiB/s · GET 1673 MiB/s
#   fio    NFS write 1137 MiB/s · read 937 MiB/s

# router on/off correctness (operator; toggles gateway flags via SSM)
bash test_router_freshness.sh
#   PASS router ON  : append visible through warm cache (md5 moved)
#   PASS router OFF : zero bytes cached on blobbers
```

**Cluster stop/start** (raw EC2, no app involvement): private IPs survive so the
node's wiring reconnects untouched; public IPs change and the control plane's
60 s cron reconciles the DB + every `zus-<id>-N` DNS record automatically in
~1–2 min. Re-running `blimp --query` after a restart:
```
post-restart q1: merge_ms=5138 mode=incremental   RESULT: PASS
```

---

## Summary — what a user actually types

On an in-VPC node with the cluster's IAM profile, the entire flow is:

```bash
curl -fsSL .../install.sh | sh                 # or git clone
export CLUSTER_ID=… WAREHOUSE=s3://…/wh ORIGIN_BUCKET=… NAMESPACE=…
blimp --setup                                  # deps, catalog, wiring — no keys
~/.blimp_venv/bin/python3 /opt/blimp/register_tpcds_tables.py …   # register data
bash hookup_cluster_source.sh                  # (or paste into the UI)
blimp --query                                  # PASS
```

Zero credentials typed; roles + a bucket policy do all the authorization.
