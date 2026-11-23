#!/bin/bash
# STR-08 integration check: Spark appends every Kafka message to an Iceberg
# Bronze table, exactly once, and a restart from the checkpoint neither skips
# nor duplicates messages.
#
# Proof (rubric R2 style): for every partition, Bronze rows == Kafka end offset
# and max kafka_offset == end offset - 1; no (topic, partition, offset) twice.
#
# Needs the running stack: kafka, spark-master, iceberg-rest, minio.
#   bash tests/integration/str08_bronze.sh
#
# Uses its own topic, table and checkpoint, all removed at the end.
set -euo pipefail
cd "$(dirname "$0")/../.."

TOPIC="gbfs.station_status.str08test"
TABLE="lakehouse.bronze.gbfs_status_str08test"
CKPT="/tmp/str08-checkpoint-test"
FIXTURES="streaming/fixtures/gbfs"
KAFKA_BIN="/opt/kafka/bin"
BS="--bootstrap-server kafka:29092"
LOG_DIR="$(mktemp -d)"

spark_submit() {  # $1 = log name, rest = spark-submit args
  local log="$LOG_DIR/$1"; shift
  docker compose exec -T -e GBFS_STATUS_TOPIC="$TOPIC" -e BRONZE_STATUS_TABLE="$TABLE" \
    spark-master /opt/spark/bin/spark-submit "$@" >"$log" 2>&1 \
    || { echo "[FAIL] spark-submit failed (log: $log)"; tail -20 "$log"; exit 1; }
}

cleanup() {
  docker compose exec -T kafka $KAFKA_BIN/kafka-topics.sh $BS --delete --if-exists \
    --topic "$TOPIC" >/dev/null 2>&1 || true
  docker compose exec -T spark-master rm -rf "$CKPT" || true
  spark_submit drop.log /opt/project/tests/integration/str08_bronze_check.py "$TABLE" --drop \
    || true
}
trap cleanup EXIT

produce() {  # stdin: "<key>|<value>" per line
  docker compose exec -T kafka $KAFKA_BIN/kafka-console-producer.sh $BS --topic "$TOPIC" \
    --property parse.key=true --property key.separator='|' >/dev/null
}

end_offsets() {  # "partition:offset" per line
  docker compose exec -T kafka $KAFKA_BIN/kafka-get-offsets.sh $BS --topic "$TOPIC" \
    --time -1 | awk -F: '{print $2":"$3}' | sort
}

run_job() {
  spark_submit "$1.log" /opt/project/streaming/jobs/gbfs_history.py \
    --sink bronze --feed status --available-now --checkpoint-dir "$CKPT"
}

check() {  # $1 = step name, $2 = expected rows, $3 = expected valid
  spark_submit "$1-check.log" /opt/project/tests/integration/str08_bronze_check.py "$TABLE"
  local result
  result=$(grep '^STR08_RESULT ' "$LOG_DIR/$1-check.log" | sed 's/^STR08_RESULT //')
  python3 - "$1" "$2" "$3" "$result" "$(end_offsets)" <<'EOF'
import json, sys
step, rows, valid, result, offsets = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), \
    json.loads(sys.argv[4]), sys.argv[5].split()
errors = []
if result["rows"] != rows:
    errors.append(f"rows {result['rows']} != {rows}")
if result["valid"] != valid:
    errors.append(f"valid {result['valid']} != {valid}")
if result["duplicate_offsets"]:
    errors.append(f"{result['duplicate_offsets']} duplicate offsets")
for item in offsets:
    partition, end = item.split(":")
    got = result["per_partition"].get(f"gbfs.station_status.str08test:{partition}",
                                      {"rows": 0, "max_offset": -1})
    if got["rows"] != int(end) or got["max_offset"] != int(end) - 1:
        errors.append(f"partition {partition}: bronze {got} vs kafka end offset {end}")
summary = (f"{result['rows']} rows, {result['valid']} valid, offsets {' '.join(offsets)}, "
           f"{result['snapshots']} snapshot(s)")
print(f"[{'FAIL' if errors else 'PASS'}] {step}: {summary}" + ("; " + "; ".join(errors) if errors else ""))
sys.exit(1 if errors else 0)
EOF
}

keyed() {  # prefix each JSON line with its station_id (or "none") as the Kafka key
  python3 -c '
import json, sys
for line in sys.stdin:
    line = line.rstrip("\n")
    try:
        key = json.loads(line).get("station_id") or "none"
    except Exception:
        key = "none"
    print(f"{key}|{line}")'
}

cleanup
docker compose exec -T kafka $KAFKA_BIN/kafka-topics.sh $BS --create --topic "$TOPIC" \
  --partitions 3 --replication-factor 1 >/dev/null

# Run 1: 2 valid + 10 invalid messages -> all 12 land in Bronze (nothing dropped).
cat "$FIXTURES/station_status.jsonl" "$FIXTURES/station_status_bad.jsonl" | keyed | produce
run_job run1
check "run 1 (fresh checkpoint)" 12 2

# Run 2: restart after 3 new messages -> only those are appended.
{
  head -n 1 "$FIXTURES/station_status.jsonl" | sed 's/"station_id": "abc"/"station_id": "ghi"/'
  head -n 1 "$FIXTURES/station_status.jsonl" | sed 's/"station_id": "abc"/"station_id": "jkl"/'
  echo 'not json'
} | keyed | produce
run_job run2
check "run 2 (restart, 3 new messages)" 15 4

# Run 3: restart with nothing new -> no new rows, no new snapshot.
before=$(grep '^STR08_RESULT ' "$LOG_DIR/run 2 (restart, 3 new messages)-check.log" \
  | sed 's/^STR08_RESULT //' | python3 -c 'import json,sys; print(json.load(sys.stdin)["snapshots"])')
run_job run3
check "run 3 (restart, nothing new)" 15 4
after=$(grep '^STR08_RESULT ' "$LOG_DIR/run 3 (restart, nothing new)-check.log" \
  | sed 's/^STR08_RESULT //' | python3 -c 'import json,sys; print(json.load(sys.stdin)["snapshots"])')
[ "$before" = "$after" ] || { echo "[FAIL] run 3 committed a new snapshot ($before -> $after)"; exit 1; }

echo "bronze check: PASSED (logs in $LOG_DIR)"
