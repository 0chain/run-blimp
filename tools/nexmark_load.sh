#!/bin/sh
# nexmark_load.sh <env file> <bucket> [namespace] [events] [base share %] [days]
#
# Generates the Nexmark event streams (person / auction / bid) as Iceberg tables
# for the batch-over-Iceberg form of the benchmark: the queries run through the
# gateway and each tick appends held-out events (seed_holdout.py), the way
# ClickBench is run. The reference generator is a Flink connector (Java); this
# is the same model in DuckDB SQL, following nexmark's GeneratorConfig:
#   - events numbered e = 0..N-1, spread evenly over [days] (default 3) from a
#     fixed base time, so time-of-day and per-day reports see several days;
#   - out of every 50 events: 1 person, 3 auctions, 46 bids;
#   - person / auction ids are 1000 + their own index; HOT KEYS as nexmark's
#     ratios: 1/2 of bids go to the hot auction (the latest id rounded down to
#     a hundred), 3/4 of bidders and sellers are the hot person (likewise);
#     the rest are drawn among the ones already generated, mostly recent;
#   - price = round(10^(u * 6) * 100), category = 10 + (u mod 5), auctions
#     expire after 1000-20000 event spacings; half the bids come from the four
#     hot channels (URL without channel_id), the rest from channel-N (URL with
#     channel_id=N); states / cities / names are nexmark's lists.
# The first <base share>% of the events (default 90) form the tables; the rest
# go to <table>_holdout/ for the ticks — later events, as a stream would bring,
# written in event order (seed_holdout.py appends them contiguously).
set -e
ENVF=$1; BUCKET=$2; NS=${3:-nexmark}; N=${4:-10000000}; BASE=${5:-90}; DAYS=${6:-3}
. "$ENVF"
HERE=$(cd "$(dirname "$0")/.." && pwd)
E=$(echo "$S3_ENDPOINT" | sed -E 's#https?://##')
case "$S3_ENDPOINT" in https:*) SSL=true;; *) SSL=false;; esac
CUT=$((N * BASE / 100))
# microseconds between events: N events cover DAYS days
SP=$((DAYS * 86400000000 / N))
# u(e, salt): a uniform [0,1) draw per event and purpose
U="((hash(e, %SALT%) % 1000000007)::DOUBLE / 1000000007)"
u() { echo "$U" | sed "s/%SALT%/$1/"; }
duckdb <<SQL
INSTALL httpfs; LOAD httpfs;
CREATE SECRET s (TYPE S3, KEY_ID '$S3_KEY', SECRET '$S3_SECRET', ENDPOINT '$E', URL_STYLE 'path', USE_SSL $SSL, REGION '${REGION:-us-east-1}');
SET threads=8; SET preserve_insertion_order=true;
CREATE TEMP TABLE ev AS
SELECT e, e // 50 AS epoch, e % 50 AS off,
       TIMESTAMP '2015-07-15 00:00:00' + to_microseconds(e * $SP) AS ts
FROM range(0, $N) t(e);
CREATE TEMP TABLE person AS
SELECT 1000 + epoch AS id,
       ['Peter','Paul','Luke','John','Saul','Vicky','Kate','Julie','Sarah','Deiter','Walter'][1 + (hash(e, 1) % 11)::INT] || ' ' ||
       ['Shultz','Abrams','Spencer','White','Bartels','Walton','Smith','Jones','Noris'][1 + (hash(e, 2) % 9)::INT] AS name,
       'u' || (hash(e, 3) % 100000)::VARCHAR || '@nexmark.com' AS emailAddress,
       lpad((hash(e, 4) % 10000)::VARCHAR, 4, '0') || ' ' || lpad((hash(e, 5) % 10000)::VARCHAR, 4, '0') || ' ' ||
       lpad((hash(e, 6) % 10000)::VARCHAR, 4, '0') || ' ' || lpad((hash(e, 7) % 10000)::VARCHAR, 4, '0') AS creditCard,
       ['Phoenix','Los Angeles','San Francisco','Boise','Portland','Bend','Redmond','Seattle','Kent','Cheyenne'][1 + (hash(e, 8) % 10)::INT] AS city,
       ['AZ','CA','ID','OR','WA','WY'][1 + (hash(e, 9) % 6)::INT] AS state,
       ts AS dateTime,
       md5(e::VARCHAR) AS extra,
       e
FROM ev WHERE off = 0;
CREATE TEMP TABLE auction AS
SELECT 1000 + epoch * 3 + (off - 1) AS id,
       'item' || (hash(e, 11) % 100000)::VARCHAR AS itemName,
       'desc' || (hash(e, 12) % 100000)::VARCHAR AS description,
       round(pow(10, $(u 13) * 6) * 100)::BIGINT AS initialBid,
       round(pow(10, $(u 14) * 6) * 100)::BIGINT AS reserve,
       ts AS dateTime,
       ts + to_microseconds((1000 + (hash(e, 15) % 19000))::BIGINT * $SP) AS expires,
       1000 + CASE WHEN $(u 18) < 0.75 THEN (epoch // 100) * 100
                   ELSE greatest(0, epoch - (hash(e, 16) % 100)::BIGINT) END AS seller,
       10 + (hash(e, 17) % 5)::BIGINT AS category,
       md5((e + 1)::VARCHAR) AS extra,
       e
FROM ev WHERE off BETWEEN 1 AND 3;
CREATE TEMP TABLE bid AS
SELECT 1000 + CASE WHEN $(u 20) < 0.5 THEN ((epoch * 3 + 2) // 100) * 100
                   WHEN $(u 21) < 0.9 THEN greatest(0, epoch * 3 + 2 - (hash(e, 22) % 100)::BIGINT)
                   ELSE (hash(e, 23) % (epoch * 3 + 3))::BIGINT END AS auction,
       1000 + CASE WHEN $(u 19) < 0.75 THEN (epoch // 100) * 100
                   WHEN $(u 24) < 0.9 THEN greatest(0, epoch - (hash(e, 25) % 100)::BIGINT)
                   ELSE (hash(e, 26) % (epoch + 1))::BIGINT END AS bidder,
       round(pow(10, $(u 27) * 6) * 100)::BIGINT AS price,
       CASE WHEN hash(e, 29) % 2 = 0 THEN ['Google','Facebook','Baidu','Apple'][1 + (hash(e, 28) % 4)::INT]
            ELSE 'channel-' || (hash(e, 33) % 10000)::VARCHAR END AS channel,
       'https://www.nexmark.com/' || (hash(e, 30) % 1000)::VARCHAR || '/' || (hash(e, 31) % 1000)::VARCHAR || '/' ||
       (hash(e, 32) % 1000)::VARCHAR || '/item.htm?query=1' ||
       CASE WHEN hash(e, 29) % 2 = 0 THEN '' ELSE '&channel_id=' || (hash(e, 33) % 10000)::VARCHAR END AS url,
       ts AS dateTime,
       md5((e + 2)::VARCHAR) AS extra,
       e
FROM ev WHERE off >= 4;
COPY (SELECT * EXCLUDE (e) FROM (SELECT * FROM person WHERE e < $CUT ORDER BY e)) TO 's3://$BUCKET/person/data.parquet' (FORMAT parquet, COMPRESSION zstd);
COPY (SELECT * EXCLUDE (e) FROM (SELECT * FROM person WHERE e >= $CUT ORDER BY e)) TO 's3://$BUCKET/person_holdout/data.parquet' (FORMAT parquet, COMPRESSION zstd);
COPY (SELECT * EXCLUDE (e) FROM (SELECT * FROM auction WHERE e < $CUT ORDER BY e)) TO 's3://$BUCKET/auction/data.parquet' (FORMAT parquet, COMPRESSION zstd);
COPY (SELECT * EXCLUDE (e) FROM (SELECT * FROM auction WHERE e >= $CUT ORDER BY e)) TO 's3://$BUCKET/auction_holdout/data.parquet' (FORMAT parquet, COMPRESSION zstd);
COPY (SELECT * EXCLUDE (e) FROM (SELECT * FROM bid WHERE e < $CUT ORDER BY e)) TO 's3://$BUCKET/bid/data.parquet' (FORMAT parquet, COMPRESSION zstd);
COPY (SELECT * EXCLUDE (e) FROM (SELECT * FROM bid WHERE e >= $CUT ORDER BY e)) TO 's3://$BUCKET/bid_holdout/data.parquet' (FORMAT parquet, COMPRESSION zstd);
SELECT 'person' t, count(*) FROM person UNION ALL SELECT 'auction', count(*) FROM auction UNION ALL SELECT 'bid', count(*) FROM bid;
SQL
AWS_ACCESS_KEY_ID=$S3_KEY AWS_SECRET_ACCESS_KEY=$S3_SECRET "${BLIMP_PY:-python3}" "$HERE/register_tpcds_tables.py" \
  --catalog "${ICEBERG_URL_LOCAL:-$ICEBERG_URL}" --warehouse "$WAREHOUSE" --source-bucket "$BUCKET" \
  --namespace "$NS" --tables person,auction,bid --tag-baseline --s3-endpoint "$S3_ENDPOINT" --region "${REGION:-us-east-1}"
