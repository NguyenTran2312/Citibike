#!/bin/bash
# STR-09 integration check: Bronze -> silver.gbfs_status on real Iceberg.
#   * grain (station_id, poll_id): a replayed message becomes one row;
#   * an unchanged station in a new poll is a new row;
#   * invalid messages stay out of Silver;
#   * re-running, and re-running after the Silver checkpoint is lost, adds no rows;
#   * the data-quality gate passes on Silver and fails on an injected duplicate.
#
# Needs the running stack: kafka, spark-master, iceberg-rest, minio.
#   bash tests/integration/str09_silver.sh
# Uses its own topic, tables and checkpoints, all removed at the end.
set -euo pipefail
cd "$(dirname "$0")/../.."

TOPIC="gbfs.station_status.str09test"
BRONZE="lakehouse.bronze.gbfs_status_str09test"
SILVER="lakehouse.silver.gbfs_status_str09test"
# On MinIO: the Iceberg streaming source (Silver) writes offsets with S3FileIO.
CKPT="s3a://checkpoints/str09-test-$(date +%s)"
FIXTURES="streaming/fixtures/gbfs"
KAFKA_BIN="/opt/kafka/bin"
BS="--bootstrap-server kafka:29092"
LOG_DIR="$(mktemp -d)"
STATUS=0

spark_submit() {  # $1 = log name, rest = spark-submit args; returns spark-submit's exit code
  local log="$LOG_DIR/$1"; shift
  docker compose exec -T -e GBFS_STATUS_TOPIC="$TOPIC" -e BRONZE_STATUS_TABLE="$BRONZE" \
    -e SILVER_STATUS_TABLE="$SILVER" spark-master /opt/spark/bin/spark-submit "$@" >"$log" 2>&1
}

must() {  # like spark_submit, but stop the test when it fails
  spark_submit "$@" || { echo "[FAIL] spark-submit $1 failed (log: $LOG_DIR/$1)"; tail -15 "$LOG_DIR/$1"; exit 1; }
}

cleanup() {
  docker compose exec -T kafka $KAFKA_BIN/kafka-topics.sh $BS --delete --if-exists \
    --topic "$TOPIC" >/dev/null 2>&1 || true
  spark_submit rm.log /opt/project/tests/integration/str09_silver_check.py "$CKPT" rm || true
  spark_submit drop.log /opt/project/tests/integration/str09_silver_check.py \
    "$BRONZE" "$SILVER" drop || true
}
trap cleanup EXIT

produce() {  # stdin: one JSON message per line, keyed by station_id
  python3 -c '
import json, sys
for line in sys.stdin:
    line = line.rstrip("\n")
    try:
        key = json.loads(line).get("station_id") or "none"
    except Exception:
        key = "none"
    print(f"{key}|{line}")' |
  docker compose exec -T kafka $KAFKA_BIN/kafka-console-producer.sh $BS --topic "$TOPIC" \
    --property parse.key=true --property key.separator='|' >/dev/null
}

bronze_then_silver() {  # $1 = step tag
  must "$1-bronze.log" /opt/project/streaming/jobs/gbfs_history.py \
    --sink bronze --feed status --available-now --checkpoint-dir "$CKPT"
  must "$1-silver.log" /opt/project/streaming/jobs/silver_status.py \
    --available-now --checkpoint-dir "$CKPT"
}

expect_silver() {  # $1 = step name, $2 = expected keys (space separated "station/poll")
  must "stats.log" /opt/project/tests/integration/str09_silver_check.py "$SILVER" stats
  local result
  result=$(grep '^STR09_RESULT ' "$LOG_DIR/stats.log" | sed 's/^STR09_RESULT //')
  if python3 - "$1" "$2" "$result" <<'EOF'
import json, sys
step, expected, r = sys.argv[1], sorted(sys.argv[2].split()), json.loads(sys.argv[3])
ok = r["keys"] == expected and r["rows"] == r["distinct_keys"] and r["null_collected_at"] == 0
print(f"[{'PASS' if ok else 'FAIL'}] {step}: {r['rows']} rows {r['keys']}, "
      f"{r['snapshots']} snapshot(s)" + ("" if ok else f"; expected {expected}"))
sys.exit(0 if ok else 1)
EOF
  then :; else STATUS=1; fi
}

expect_dq() {  # $1 = step name, $2 = expected exit code (0 pass, 1 fail)
  local code=0
  spark_submit dq.log /opt/project/streaming/jobs/dq_checks.py --layer silver_status \
    --bronze-status-table "$BRONZE" --silver-status-table "$SILVER" || code=$?
  local summary
  summary=$(grep -E '^\[(FAIL|WARN)\]' "$LOG_DIR/dq.log" | tr '\n' ' ')
  if [ "$code" = "$2" ]; then
    echo "[PASS] $1: dq exit $code ${summary:-(all checks pass)}"
  else
    echo "[FAIL] $1: dq exit $code, expected $2 (log: $LOG_DIR/dq.log)"; STATUS=1
  fi
}

P1="poll-20261116T024000Z"
cleanup
docker compose exec -T kafka $KAFKA_BIN/kafka-topics.sh $BS --create --topic "$TOPIC" \
  --partitions 3 --replication-factor 1 >/dev/null

# Run 1: abc + def (poll 1), the abc message replayed, abc unchanged in poll 2,
# and the 10 invalid fixture messages.
{
  cat "$FIXTURES/station_status.jsonl"
  head -n 1 "$FIXTURES/station_status.jsonl"
  head -n 1 "$FIXTURES/station_status.jsonl" | sed "s/$P1/poll-2/"
  cat "$FIXTURES/station_status_bad.jsonl"
} | produce
bronze_then_silver run1
expect_silver "run 1 (replay deduped, unchanged poll kept, invalid out)" "abc/$P1 abc/poll-2 def/$P1"
expect_dq "run 1 data-quality gate" 0

# Run 2: nothing new -> no new rows.
bronze_then_silver run2
expect_silver "run 2 (nothing new)" "abc/$P1 abc/poll-2 def/$P1"

# Run 3: the Silver checkpoint is lost -> Silver re-reads all of Bronze, MERGE adds nothing.
must rm-silver.log /opt/project/tests/integration/str09_silver_check.py "$CKPT/silver_status" rm
bronze_then_silver run3
expect_silver "run 3 (Silver checkpoint lost, full replay)" "abc/$P1 abc/poll-2 def/$P1"
expect_dq "run 3 data-quality gate" 0

# Run 4: new poll for def, plus the poll-2 message replayed again.
{
  sed -n 2p "$FIXTURES/station_status.jsonl" | sed "s/$P1/poll-3/"
  head -n 1 "$FIXTURES/station_status.jsonl" | sed "s/$P1/poll-2/"
} | produce
bronze_then_silver run4
expect_silver "run 4 (new poll + replay)" "abc/$P1 abc/poll-2 def/$P1 def/poll-3"

# Run 5 (review finding): the poll-2 message again, with a collected_at one minute
# later. Same (station_id, poll_id) -> still one row.
head -n 1 "$FIXTURES/station_status.jsonl" \
  | sed "s/$P1/poll-2/; s/2026-11-16T02:40:00Z/2026-11-16T02:41:00Z/" | produce
bronze_then_silver run5
expect_silver "run 5 (same key, collected_at one minute later)" "abc/$P1 abc/poll-2 def/$P1 def/poll-3"

# Run 6 (review finding): Silver checkpoint lost again and Bronze replayed with a
# non-UTC Spark session time zone -> no new rows.
must rm-silver6.log /opt/project/tests/integration/str09_silver_check.py "$CKPT/silver_status" rm
must run6-silver.log --conf spark.sql.session.timeZone=Asia/Ho_Chi_Minh \
  /opt/project/streaming/jobs/silver_status.py --available-now --checkpoint-dir "$CKPT"
expect_silver "run 6 (full replay, session time zone Asia/Ho_Chi_Minh)" "abc/$P1 abc/poll-2 def/$P1 def/poll-3"
expect_dq "run 6 data-quality gate" 0

# The gate must catch a broken Silver: inject a duplicate row.
must inject.log /opt/project/tests/integration/str09_silver_check.py "$SILVER" inject-duplicate
expect_dq "data-quality gate on an injected duplicate" 1

[ "$STATUS" = 0 ] && echo "silver check: PASSED (logs in $LOG_DIR)" || { echo "silver check: FAILED (logs in $LOG_DIR)"; exit 1; }
