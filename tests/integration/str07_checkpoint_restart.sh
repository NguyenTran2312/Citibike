#!/bin/bash
# STR-07 integration check: Spark reads Kafka, splits valid/invalid, and resumes
# from its checkpoint after a restart (no message read twice, none skipped).
#
# Needs the running stack: kafka, spark-master, iceberg-rest (spark-defaults sets
# the Iceberg catalog as default, so every Spark query needs it reachable).
#   bash tests/integration/str07_checkpoint_restart.sh
#
# Uses its own topic + checkpoint, both deleted at the end; real topics are untouched.
set -euo pipefail
cd "$(dirname "$0")/../.."

TOPIC="gbfs.station_status.str07test"
CKPT="/tmp/str07-checkpoint-test"
FIXTURES="streaming/fixtures/gbfs"
KT="/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092"
LOG_DIR="$(mktemp -d)"

cleanup() {
  docker compose exec -T kafka $KT --delete --if-exists --topic "$TOPIC" >/dev/null 2>&1 || true
  docker compose exec -T spark-master rm -rf "$CKPT" || true
}
trap cleanup EXIT

produce() {  # stdin -> test topic
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:29092 --topic "$TOPIC" >/dev/null
}

run_job() {  # $1 = run name; prints "<valid> <invalid> <batches with rows>"
  docker compose exec -T -e GBFS_STATUS_TOPIC="$TOPIC" spark-master \
    /opt/spark/bin/spark-submit /opt/project/streaming/jobs/gbfs_history.py \
    --sink console --feed status --available-now --checkpoint-dir "$CKPT" >"$LOG_DIR/$1.log" 2>&1
  grep -E '^\[status\] batch [0-9]+:' "$LOG_DIR/$1.log" \
    | awk '{v+=$4; i+=$6; if ($4+$6>0) b++} END {printf "%d %d %d\n", v, i, b}'
}

expect() {  # $1 = run name, $2 = actual "v i b", $3 = expected "v i"
  local v i b
  read -r v i b <<<"$2"
  if [ "$v $i" = "$3" ]; then
    echo "[PASS] $1: $v valid, $i invalid ($b batch(es) with rows)"
  else
    echo "[FAIL] $1: got $v valid, $i invalid; expected $3 (log: $LOG_DIR/$1.log)"
    exit 1
  fi
}

cleanup
docker compose exec -T kafka $KT --create --topic "$TOPIC" --partitions 3 --replication-factor 1 >/dev/null

# Run 1: 2 valid + 10 invalid messages, fresh checkpoint -> reads everything.
cat "$FIXTURES/station_status.jsonl" "$FIXTURES/station_status_bad.jsonl" | produce
expect "run 1 (fresh checkpoint)" "$(run_job run1)" "2 10"

# Run 2: restart with the same checkpoint after 2 new messages -> only the new ones.
{
  head -n 1 "$FIXTURES/station_status.jsonl" | sed 's/"station_id": "abc"/"station_id": "ghi"/'
  echo 'not json'
} | produce
expect "run 2 (restart, 2 new messages)" "$(run_job run2)" "1 1"

# Run 3: restart again with nothing new -> nothing reprocessed.
expect "run 3 (restart, no new messages)" "$(run_job run3)" "0 0"

echo "checkpoint restart check: PASSED (logs in $LOG_DIR)"
