#!/bin/bash
# STR-10 integration check: Bronze -> silver.gbfs_station_information on real Iceberg.
#   * every source field (incl. region_id, rental_uris, is_charging) reaches Silver;
#   * grain (station_id, poll_id): a replayed message becomes one row;
#   * every poll is a snapshot: an unchanged or renamed station in a new poll is a new row;
#   * invalid messages stay out of Silver;
#   * re-running, and re-running after the Silver checkpoint is lost, adds no rows;
#   * the data-quality gate passes on Silver and fails on an injected duplicate.
#
# Needs the running stack: kafka, spark-master, iceberg-rest, minio.
#   bash tests/integration/str10_silver_information.sh
# Uses its own topic, tables and checkpoints, all removed at the end.
set -euo pipefail
cd "$(dirname "$0")/../.."

TOPIC="gbfs.station_information.str10test"
BRONZE="lakehouse.bronze.gbfs_station_information_str10test"
SILVER="lakehouse.silver.gbfs_station_information_str10test"
# On MinIO: the Iceberg streaming source (Silver) writes offsets with S3FileIO.
CKPT="s3a://checkpoints/str10-test-$(date +%s)"
FIXTURES="streaming/fixtures/gbfs"
KAFKA_BIN="/opt/kafka/bin"
BS="--bootstrap-server kafka:29092"
LOG_DIR="$(mktemp -d)"
STATUS=0

spark_submit() {  # $1 = log name, rest = spark-submit args; returns spark-submit's exit code
  local log="$LOG_DIR/$1"; shift
  docker compose exec -T -e GBFS_INFORMATION_TOPIC="$TOPIC" -e BRONZE_INFORMATION_TABLE="$BRONZE" \
    -e SILVER_INFORMATION_TABLE="$SILVER" spark-master /opt/spark/bin/spark-submit "$@" >"$log" 2>&1
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
    --sink bronze --feed information --available-now --checkpoint-dir "$CKPT"
  must "$1-silver.log" /opt/project/streaming/jobs/silver_information.py \
    --available-now --checkpoint-dir "$CKPT"
}

expect_silver() {  # $1 = step name, $2 = expected keys (space separated "station/poll")
  must "stats.log" /opt/project/tests/integration/str09_silver_check.py "$SILVER" stats
  local result
  result=$(grep '^STR09_RESULT ' "$LOG_DIR/stats.log" | sed 's/^STR09_RESULT //')
  if python3 - "$1" "$2" "$result" <<'EOF'
import json, sys
step, expected, r = sys.argv[1], sorted(sys.argv[2].split()), json.loads(sys.argv[3])
# Every source field reached Silver: abc carries region_id / rental_uris / is_charging.
full = all(r["non_null"][c] > 0 for c in ("region_id", "rental_uris", "is_charging", "short_name"))
ok = (r["keys"] == expected and r["rows"] == r["distinct_keys"]
      and r["null_collected_at"] == 0 and full)
print(f"[{'PASS' if ok else 'FAIL'}] {step}: {r['rows']} rows {r['keys']}, "
      f"{r['snapshots']} snapshot(s)" + ("" if ok else f"; expected {expected}"))
sys.exit(0 if ok else 1)
EOF
  then :; else STATUS=1; fi
}

expect_dq() {  # $1 = step name, $2 = expected exit code (0 pass, 1 fail)
  local code=0
  spark_submit dq.log /opt/project/streaming/jobs/dq_checks.py --layer silver_information \
    --bronze-information-table "$BRONZE" --silver-information-table "$SILVER" || code=$?
  local summary
  summary=$(grep -E '^\[(FAIL|WARN)\]' "$LOG_DIR/dq.log" | tr '\n' ' ')
  if [ "$code" = "$2" ]; then
    echo "[PASS] $1: dq exit $code ${summary:-(all checks pass)}"
  else
    echo "[FAIL] $1: dq exit $code, expected $2 (log: $LOG_DIR/dq.log)"; STATUS=1
  fi
}

P1="station_information-20261116T000000Z"
cleanup
docker compose exec -T kafka $KAFKA_BIN/kafka-topics.sh $BS --create --topic "$TOPIC" \
  --partitions 3 --replication-factor 1 >/dev/null

# Run 1: abc + def (snapshot 1), the abc message replayed, abc renamed in
# snapshot 2, and invalid messages (bad latitude, no poll_id, not JSON).
INFO="$FIXTURES/station_information.jsonl"
{
  cat "$INFO"
  head -n 1 "$INFO"
  head -n 1 "$INFO" | sed "s/$P1/poll-2/; s/Sample Station A/Renamed Station A/"
  head -n 1 "$INFO" | sed 's/"lat": 40.7128/"lat": 95.0/; s/"abc"/"bad-lat"/'
  head -n 1 "$INFO" | sed "s/\"poll_id\": \"$P1\", //; s/\"abc\"/\"no-poll\"/"
  echo 'not json'
} | produce
bronze_then_silver run1
expect_silver "run 1 (all fields, replay deduped, renamed snapshot kept, invalid out)" "abc/$P1 abc/poll-2 def/$P1"
expect_dq "run 1 data-quality gate" 0

# Run 2: nothing new -> no new rows.
bronze_then_silver run2
expect_silver "run 2 (nothing new)" "abc/$P1 abc/poll-2 def/$P1"

# Run 3: the Silver checkpoint is lost -> Silver re-reads all of Bronze, MERGE adds nothing.
must rm-silver.log /opt/project/tests/integration/str09_silver_check.py "$CKPT/silver_information" rm
bronze_then_silver run3
expect_silver "run 3 (Silver checkpoint lost, full replay)" "abc/$P1 abc/poll-2 def/$P1"
expect_dq "run 3 data-quality gate" 0

# Run 4: new snapshot for def, plus the poll-2 message replayed again.
{
  sed -n 2p "$INFO" | sed "s/$P1/poll-3/"
  head -n 1 "$INFO" | sed "s/$P1/poll-2/; s/Sample Station A/Renamed Station A/"
} | produce
bronze_then_silver run4
expect_silver "run 4 (new snapshot + replay)" "abc/$P1 abc/poll-2 def/$P1 def/poll-3"

# The gate must catch a broken Silver: inject a duplicate row.
must inject.log /opt/project/tests/integration/str09_silver_check.py "$SILVER" inject-duplicate
expect_dq "data-quality gate on an injected duplicate" 1

[ "$STATUS" = 0 ] && echo "silver information check: PASSED (logs in $LOG_DIR)" || { echo "silver information check: FAILED (logs in $LOG_DIR)"; exit 1; }
