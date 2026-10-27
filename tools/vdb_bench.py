#!/usr/bin/env python3
"""VectorDBBench-style search benchmark through the zs3 gateway.

VectorDBBench's performance cases (Performance768D1M: Cohere 1M x 768, cosine)
report recall@k against the dataset's ground truth, serial p99 latency and
QPS. This runs the same measurement with the gateway as the engine: each test
vector becomes

  SELECT id FROM <ns>.<table> ORDER BY array_cosine_distance(emb, [..]::FLOAT[dim]) LIMIT k

sent to /admin/query/run (no_mv: a nearest-neighbour search has no MV to
reuse; persist_result keeps each answer so recall is computed after the
timing). Latency is the request's wall time.

  --queries N       how many test vectors (default 100; VectorDBBench uses 1000)
  --levels 1,5,10   concurrency levels for QPS; each runs --duration seconds
                    over the test vectors (VectorDBBench: 30 s per level, the
                    reported QPS is the best level)

The search is an exact scan (no vector index in the gateway): recall is 1.0 by
construction, unlike the ANN engines VectorDBBench usually ranks.
"""
import argparse, json, os, statistics, sys, threading, time, urllib.error, urllib.request


def run(api, token, source, sql, label, persist):
    body = json.dumps({"original_sql": sql, "source": source, "label": label, "no_mv": True,
                       "persist_result": persist}).encode()
    req = urllib.request.Request(api + "/admin/query/run", data=body, method="POST",
                                 headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"})
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=3600) as r:
            d = json.load(r)
    except (urllib.error.URLError, OSError, ValueError) as e:
        d = {"status": "error", "err": str(e)}
    return time.time() - t0, d


def p99(xs):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(0.99 * len(xs)))] if xs else float("nan")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--env", required=True, help="blimp env file (CLUSTER_TOKEN, S3_*)")
    ap.add_argument("--dataset-dir", required=True, help="local dir with test.parquet and neighbors.parquet")
    ap.add_argument("--namespace", default="vdb"); ap.add_argument("--table", default="cohere_1m")
    ap.add_argument("--k", type=int, default=100); ap.add_argument("--queries", type=int, default=100)
    ap.add_argument("--levels", default="1,5,10,20"); ap.add_argument("--duration", type=float, default=30)
    ap.add_argument("--api", default="http://127.0.0.1:9000"); ap.add_argument("--source", default="")
    ap.add_argument("--out", default="")
    a = ap.parse_args()
    env = {}
    for l in open(a.env):
        if "=" in l and not l.lstrip().startswith("#"):
            k, v = l.strip().split("=", 1); env[k] = v.strip().strip('"').strip("'")
    token = env.get("CLUSTER_TOKEN") or os.environ.get("CLUSTER_TOKEN", "")
    source = a.source or env.get("SOURCE", "customer")
    import duckdb
    con = duckdb.connect()
    tests = con.execute(f"SELECT id, emb FROM read_parquet('{a.dataset_dir}/test.parquet') ORDER BY id LIMIT {a.queries}").fetchall()
    truth = {r[0]: set(r[1][:a.k]) for r in con.execute(f"SELECT id, neighbors_id FROM read_parquet('{a.dataset_dir}/neighbors.parquet')").fetchall()}
    dim = len(tests[0][1])

    def sql(v):
        lit = "[" + ",".join(f"{x:.9g}" for x in v) + f"]::FLOAT[{dim}]"
        # the column reads back as FLOAT[] (parquet stores a list)
        return f"SELECT id FROM {a.namespace}.{a.table} ORDER BY array_cosine_distance(emb::FLOAT[{dim}], {lit}), id LIMIT {a.k}"

    # results are read from the gateway's result bucket after the timing
    import s3fs
    fs = s3fs.S3FileSystem(key=env.get("MV_S3_KEY") or env.get("GW_AK") or env.get("S3_KEY"),
                           secret=env.get("MV_S3_SECRET") or env.get("GW_SK") or env.get("S3_SECRET"),
                           client_kwargs={"endpoint_url": env.get("MV_S3_ENDPOINT") or a.api, "region_name": "us-east-1"})
    import pyarrow.parquet as pq
    lat, errs, pending = [], 0, []
    print(f"== serial: {len(tests)} queries, k={a.k}, dim={dim}")
    for qid, v in tests:
        t0 = time.time()
        dt, d = run(a.api, token, source, sql(v), f"vdb:{a.table}:q{qid}", True)
        if d.get("status") != "ok":
            errs += 1; print(f"   q{qid}: {d.get('status')} {str(d.get('err'))[:200]}"); continue
        lat.append(dt)
        if d.get("result_bucket") and d.get("result_sig"):
            pending.append((qid, f"{d['result_bucket']}/qresults/{d['result_sig']}/data.parquet", t0))
    if lat:
        print(f"   serial latency: p50 {statistics.median(lat):.3f}s p99 {p99(lat):.3f}s mean {statistics.mean(lat):.3f}s, errors {errs}")
    recalls, unread = [], 0
    for qid, path, t0 in pending:
        got = None
        for _ in range(12):  # the result upload runs behind the response
            try:
                lm = fs.info(path).get("LastModified")
                if lm is not None and lm.timestamp() >= t0 - 2:
                    got = [r for r in pq.read_table(fs.open(path, "rb")).column(0).to_pylist()]
                    break
            except Exception:
                pass
            time.sleep(5)
        if got is None:
            unread += 1; continue
        recalls.append(len(set(got[:a.k]) & truth.get(qid, set())) / a.k)
    if recalls:
        print(f"   recall@{a.k}: {statistics.mean(recalls):.4f} over {len(recalls)} queries ({unread} result(s) unreadable)")
    elif pending:
        print(f"   recall: no result readable ({unread} unreadable)")
    best, levels = 0.0, {}
    for c in [int(x) for x in a.levels.split(",") if x.strip()]:
        lats, lock, stop = [], threading.Lock(), time.time() + a.duration
        def worker(i):
            j = i
            while time.time() < stop:
                qid, v = tests[j % len(tests)]; j += c
                dt, d = run(a.api, token, source, sql(v), f"vdb:{a.table}:c{qid}", False)
                if d.get("status") == "ok":
                    with lock:
                        lats.append(dt)
        t0 = time.time()
        ths = [threading.Thread(target=worker, args=(i,)) for i in range(c)]
        [t.start() for t in ths]; [t.join() for t in ths]
        wall = time.time() - t0
        qps = len(lats) / wall if wall > 0 else 0.0
        levels[c] = {"qps": qps, "p99": p99(lats), "n": len(lats)}
        best = max(best, qps)
        print(f"== concurrency {c}: QPS {qps:.2f} ({len(lats)} ok in {wall:.1f}s), p99 {p99(lats):.3f}s")
    print(f"== max QPS {best:.2f}")
    if a.out:
        json.dump({"serial_latency_s": lat, "recall": recalls, "levels": levels, "max_qps": best, "errors": errs}, open(a.out, "w"))


if __name__ == "__main__":
    main()
