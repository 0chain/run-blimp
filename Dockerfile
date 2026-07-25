# blimp kit — everything the client node needs to connect a Blimp cluster to
# your Iceberg + S3 data and validate it, baked into one image. No host prep:
# `docker compose run --rm blimp --setup` on any Docker host does it all.
#
# Bundled: python3 + pyiceberg[s3fs]/pyarrow/boto3 (register tables, CDC
# appends), aws CLI v2 (bucket create/grant, SSM), duckdb CLI (query suite),
# and the kit scripts. The Iceberg REST catalog runs as its own compose service
# (see docker-compose.yml), so this container never needs docker-in-docker.
FROM python:3.12-slim

# --- OS deps: aws CLI, duckdb CLI, and the small tools the scripts shell out to
RUN set -eux; \
    apt-get update -qq; \
    apt-get install -y -qq --no-install-recommends \
      curl unzip ca-certificates jq openssh-client bash coreutils; \
    # aws CLI v2 + duckdb CLI — architecture-aware (aws uses x86_64/aarch64,
    # duckdb uses amd64/aarch64), no bash-only string ops so /bin/sh is fine.
    case "$(uname -m)" in aarch64) A=aarch64; D=aarch64;; *) A=x86_64; D=amd64;; esac; \
    curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-${A}.zip" -o /tmp/awscli.zip; \
    unzip -q /tmp/awscli.zip -d /tmp; /tmp/aws/install; \
    curl -fsSL "https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-${D}.zip" -o /tmp/duckdb.zip; \
    unzip -q /tmp/duckdb.zip -d /usr/local/bin; chmod +x /usr/local/bin/duckdb; \
    rm -rf /tmp/aws /tmp/awscli.zip /tmp/duckdb.zip /var/lib/apt/lists/*

# --- python deps: Iceberg + Arrow + S3 + boto3 (register / seed / CDC)
RUN pip install --no-cache-dir "pyiceberg[s3fs]" pyarrow boto3 duckdb

# --- the kit
WORKDIR /kit
COPY blimp register_tpcds_tables.py seed_tpcds.py hookup_cluster_source.sh \
     test_query.sh bench_cdc.sh bench_incremental.sh test_cache.sh \
     run_cluster.sh run_router.sh test_router_freshness.sh /kit/
RUN chmod +x /kit/blimp /kit/*.sh || true; \
    ln -s /kit/blimp /usr/local/bin/blimp

# The image ALREADY has every prerequisite, so the CLI's own installer is a
# no-op here — skip it for a fast, offline-clean start.
ENV BLIMP_SKIP_DEPS=1 \
    BLIMP_PY=/usr/local/bin/python3 \
    BLIMP_ENV=/kit/state/.blimp_env

# Wiring + venv state persist in a mounted volume so --setup runs once and
# --query/--storage reuse it across `docker compose run` invocations.
VOLUME /kit/state

ENTRYPOINT ["blimp"]
CMD []
