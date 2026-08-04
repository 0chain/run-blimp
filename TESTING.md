# Testing the kit itself

`./test_kit.sh` unit-tests the kit's pure decision logic — no cluster, no
network. Every case in it corresponds to a bug that shipped a *wrong number*
rather than an error, which is the class a run log cannot show you:

- the fact table `--bench` appends to is derived from the query, so a run can't
  silently measure a query whose source never changed
- a non-blank `S3_ENDPOINT` always produces `--endpoint-url`; without it origin
  calls quietly go to AWS and the cache leg reports "no objects"
- the cache set is sized so the per-blobber shard share exceeds per-blobber RAM
  (below that the "hit" is served from blobber page cache and nothing
  client-side reveals it)
- mlperf runs accel=1 below a `*.4xlarge`, because extra ranks starve the
  mount-s3 daemon (it sits outside the dlio memory cgroup and gets OOM-killed)
- dlio's `<value> (<stddev>)` is parsed to the value
- **the kit never shells into the Blimp node** — it is HTTP-only, and the suite
  fails if any `ssh`/`aws ssm`/`docker exec` against the cluster reappears
