#!/bin/bash
# Streaming Core gate 14: Trino reads the GBFS Bronze and Silver Iceberg tables
# that Spark writes, with the same row counts, nested types and timestamps.
#
# Spark writes four test tables (same DDL and jobs as the real ones) from the
# deterministic fixtures in streaming/fixtures/gbfs, then Trino (catalog
# `iceberg`) reads them back:
#   * row counts in Trino == row counts in Spark, for all four tables;
#   * vehicle_types_available (ARRAY<ROW>) and rental_uris (ROW) are readable;
#   * collected_at / last_updated are `timestamp(6) with time zone` holding the
#     fixture instants (UTC).
#
# Runs only in an isolated Compose project (COMPOSE_PROJECT_NAME ending in -itest)
# with kafka, spark-master, spark-worker, iceberg-rest, minio and trino up:
#   export COMPOSE_PROJECT_NAME=citibike-itest
#   bash tests/integration/str_trino_read.sh
# Uses its own topics, tables and checkpoint, all removed at the end.
set -euo pipefail
cd "$(dirname "$0")/../.."

case "${COMPOSE_PROJECT_NAME:-}" in
  *-itest) ;;
  *) echo "refusing to run: set COMPOSE_PROJECT_NAME=<name>-itest (isolated stack)"; exit 2 ;;
esac

RUN="trinotest$(date +%s)"
TOPIC_S="gbfs.station_status.$RUN"
TOPIC_I="gbfs.station_information.$RUN"
B_S="bronze.gbfs_status_$RUN"
S_S="silver.gbfs_status_$RUN"
B_I="bronze.gbfs_station_information_$RUN"
S_I="silver.gbfs_station_information_$RUN"
CKPT="s3a://checkpoints/$RUN"
FIXTURES="streaming/fixtures/gbfs"
KAFKA_BIN="/opt/kafka/bin"
BS="--bootstrap-server kafka:29092"
LOG_DIR="$(mktemp -d)"
STATUS=0

spark_submit() {  # $1 = log name, rest = spark-submit args
  local log="$LOG_DIR/$1"; shift
  docker compose exec -T \
    -e GBFS_STATUS_TOPIC="$TOPIC_S" -e GBFS_INFORMATION_TOPIC="$TOPIC_I" \
    -e BRONZE_STATUS_TABLE="lakehouse.$B_S" -e SILVER_STATUS_TABLE="lakehouse.$S_S" \
    -e BRONZE_INFORMATION_TABLE="lakehouse.$B_I" -e SILVER_INFORMATION_TABLE="lakehouse.$S_I" \
    spark-master /opt/spark/bin/spark-submit "$@" >"$log" 2>&1
}

must() {
  spark_submit "$@" || { echo "[FAIL] spark-submit $1 failed (log: $LOG_DIR/$1)"; tail -15 "$LOG_DIR/$1"; exit 1; }
}

trino() {  # one query, TSV without header
  docker compose exec -T trino trino --output-format TSV --execute "$1"
}

cleanup() {
  for t in "$TOPIC_S" "$TOPIC_I"; do
    docker compose exec -T kafka $KAFKA_BIN/kafka-topics.sh $BS --delete --if-exists \
      --topic "$t" >/dev/null 2>&1 || true
  done
  spark_submit rm.log /opt/project/tests/integration/str09_silver_check.py "$CKPT" rm || true
  spark_submit drop.log /opt/project/tests/integration/str09_silver_check.py \
    "lakehouse.$B_S" "lakehouse.$S_S" "lakehouse.$B_I" "lakehouse.$S_I" drop || true
}
trap cleanup EXIT

produce() {  # $1 = topic; stdin: one JSON message per line, keyed by station_id
  python3 -c '
import json, sys
for line in sys.stdin:
    line = line.rstrip("\n")
    try:
        key = json.loads(line).get("station_id") or "none"
    except Exception:
        key = "none"
    print(f"{key}|{line}")' |
  docker compose exec -T kafka $KAFKA_BIN/kafka-console-producer.sh $BS --topic "$1" \
    --property parse.key=true --property key.separator='|' >/dev/null
}

check() {  # $1 = description, $2 = expected, $3 = actual
  if [ "$2" = "$3" ]; then
    echo "[PASS] $1: $3"
  else
    echo "[FAIL] $1: expected '$2', got '$3'"; STATUS=1
  fi
}

spark_rows() {  # $1 = table (without catalog) -> row count seen by Spark
  must "count-$1.log" /opt/project/tests/integration/str09_silver_check.py "lakehouse.$1" stats
  grep '^STR09_RESULT ' "$LOG_DIR/count-$1.log" | sed 's/^STR09_RESULT //' \
    | python3 -c 'import json, sys; print(json.load(sys.stdin)["rows"])'
}

for t in "$TOPIC_S" "$TOPIC_I"; do
  docker compose exec -T kafka $KAFKA_BIN/kafka-topics.sh $BS --create --topic "$t" \
    --partitions 3 --replication-factor 1 >/dev/null
done
{ cat "$FIXTURES/station_status.jsonl"; cat "$FIXTURES/station_status_bad.jsonl"; } | produce "$TOPIC_S"
produce "$TOPIC_I" < "$FIXTURES/station_information.jsonl"

# Spark: Kafka -> Bronze (both feeds), then Bronze -> Silver.
must bronze.log /opt/project/streaming/jobs/gbfs_history.py --sink bronze --feed all \
  --available-now --checkpoint-dir "$CKPT"
must silver-status.log /opt/project/streaming/jobs/silver_status.py --available-now \
  --checkpoint-dir "$CKPT"
must silver-information.log /opt/project/streaming/jobs/silver_information.py --available-now \
  --checkpoint-dir "$CKPT"

# Same row counts in Spark and Trino. Fixtures: status 2 valid + 10 invalid,
# information 2 valid; Silver keeps the valid ones.
for spec in "$B_S:12" "$S_S:2" "$B_I:2" "$S_I:2"; do
  table="${spec%%:*}"; expected="${spec##*:}"
  spark_count=$(spark_rows "$table")
  trino_count=$(trino "SELECT count(*) FROM iceberg.$table")
  check "Spark rows of $table" "$expected" "$spark_count"
  check "Trino rows of $table == Spark" "$spark_count" "$trino_count"
done

check "Trino Bronze status valid / invalid" "2	10" \
  "$(trino "SELECT count_if(dq_is_valid), count_if(NOT dq_is_valid) FROM iceberg.$B_S")"
check "Trino Bronze Kafka metadata present" "0" \
  "$(trino "SELECT count_if(kafka_topic IS NULL OR kafka_offset IS NULL OR kafka_timestamp IS NULL OR raw_value IS NULL) FROM iceberg.$B_S")"

# Nested types.
check "Trino reads vehicle_types_available (ARRAY<ROW>)" "def	1	2	2" \
  "$(trino "SELECT station_id, vehicle_types_available[1].vehicle_type_id, vehicle_types_available[1].\"count\", cardinality(vehicle_types_available) FROM iceberg.$S_S WHERE cardinality(vehicle_types_available) > 0")"
check "Trino reads rental_uris (ROW)" "abc	https://bkn.lft.to/lastmile_qr_scan	1" \
  "$(trino "SELECT station_id, rental_uris.ios, (SELECT count(*) FROM iceberg.$S_I WHERE rental_uris IS NULL) FROM iceberg.$S_I WHERE rental_uris IS NOT NULL")"

# Timestamps: Iceberg timestamptz, fixture instants in UTC.
check "Trino type of Silver collected_at" "timestamp(6) with time zone" \
  "$(trino "SELECT typeof(collected_at) FROM iceberg.$S_S LIMIT 1")"
check "Silver status collected_at / last_updated / last_reported (epoch s)" "1794796800	1794796800	1794796740" \
  "$(trino "SELECT CAST(to_unixtime(collected_at) AS bigint), CAST(to_unixtime(last_updated) AS bigint), CAST(to_unixtime(last_reported) AS bigint) FROM iceberg.$S_S WHERE station_id = 'abc'")"
check "Silver information collected_at (epoch s)" "1794787200" \
  "$(trino "SELECT DISTINCT CAST(to_unixtime(collected_at) AS bigint) FROM iceberg.$S_I")"

[ "$STATUS" = 0 ] && echo "trino read check: PASSED (logs in $LOG_DIR)" \
  || { echo "trino read check: FAILED (logs in $LOG_DIR)"; exit 1; }
