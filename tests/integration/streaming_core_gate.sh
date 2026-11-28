#!/bin/bash
# Streaming Core test gate: runs the real pipeline in an isolated Compose project
#   gbfs-poller -> Kafka -> latest-state-consumer -> PostgreSQL
#   gbfs-poller -> Kafka -> spark-streaming (4 queries) -> Iceberg Bronze -> Silver
# and checks the gates that need live data (numbers as in the integration plan):
#   2  pipeline containers keep running (no restart)
#   3  Python dependencies and Spark connector jars
#   5  poll IDs: format, one collected_at per poll, status/information disjoint
#   7  PostgreSQL latest state == latest Silver observation, every field
#      (tests/integration/latest_state_reconcile.sh)
# A query that fails or returns nothing fails the run (pseudo-gate "queries"),
# and so does a catch-up timeout (pseudo-gate "catchup").
#   10 Bronze rows == Kafka end offsets per partition, Kafka metadata present
#   11 Silver grain unique + data-quality gate (dq_checks.py)
#   12 four queries with four distinct checkpoints
#   13 restarting spark-streaming adds no rows and no duplicates
# With RUN_EXTRA=1 it also runs gate 15 (tests/smoke/infra_smoke.sh), gate 14
# (tests/integration/str_trino_read.sh) and gates 8/9
# (tests/integration/test_latest_state_recovery.py, needs confluent-kafka and
# psycopg on the host).
#
# Source of GBFS data:
#   GATE_SOURCE=mock (default)  deterministic: tests/integration/gbfs_mock.py,
#                               2519 stations, via poller-shutdown.compose.yml
#   GATE_SOURCE=real            the real GBFS API (needs internet)
#
# Isolation: refuses to run unless COMPOSE_PROJECT_NAME ends in -itest and no
# container of the development project (citibike) is running. Containers,
# network and named volumes are then prefixed with the itest project name, so
# topics, Iceberg tables, MinIO objects and checkpoints stay inside it.
#   export COMPOSE_PROJECT_NAME=citibike-itest
#   bash tests/integration/streaming_core_gate.sh
# Nothing is deleted at the end; remove the itest stack yourself with
#   docker compose -p citibike-itest down -v
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 2

PROJECT="${COMPOSE_PROJECT_NAME:-}"
case "$PROJECT" in
  *-itest) ;;
  *) echo "refusing to run: set COMPOSE_PROJECT_NAME=<name>-itest (isolated stack)"; exit 2 ;;
esac
if [ -n "$(docker ps -q --filter label=com.docker.compose.project=citibike)" ]; then
  echo "refusing to run: the development project 'citibike' is running (same host ports)."
  echo "Stop it first, without removing volumes:  docker compose -p citibike stop"
  exit 2
fi

GATE_SOURCE="${GATE_SOURCE:-mock}"
RUN_SECONDS="${RUN_SECONDS:-330}"          # >= 5 status polls (60 s each)
CATCHUP_TIMEOUT="${CATCHUP_TIMEOUT:-600}"
export STREAM_TRIGGER_INTERVAL="${STREAM_TRIGGER_INTERVAL:-30 seconds}"
LOG_DIR="$(mktemp -d)"
declare -A RESULT
ORDER=()

# Docker Desktop without WSL integration runs docker.exe through a wrapper that
# forwards only a few variables (COMPOSE_PROJECT_NAME, COMPOSE_PROFILES, ...).
# Forward the ones this script interpolates, and pass compose files with -f
# (COMPOSE_FILE would be neither forwarded nor split on ':' by docker.exe).
export WSLENV="${WSLENV:+$WSLENV:}COMPOSE_PROJECT_NAME:STREAM_TRIGGER_INTERVAL:GBFS_MOCK_CONTROL_DIR"
COMPOSE_ARGS=(-f docker-compose.yml)
if [ "$GATE_SOURCE" = mock ]; then
  COMPOSE_ARGS+=(-f tests/integration/poller-shutdown.compose.yml)
  export GBFS_MOCK_CONTROL_DIR="${GBFS_MOCK_CONTROL_DIR:-$(mktemp -d)}"
  echo '{}' > "$GBFS_MOCK_CONTROL_DIR/behavior.json"
elif [ "$GATE_SOURCE" != real ]; then
  echo "GATE_SOURCE must be mock or real"; exit 2
fi

gate() {  # $1 = gate id, $2 = PASS|FAIL, $3 = evidence
  local id="$1"
  [ -z "${RESULT[$id]+x}" ] && ORDER+=("$id")
  # A gate fails if any of its checks fails.
  if [ "${RESULT[$id]:-PASS}" = PASS ] || [ "$2" = FAIL ]; then RESULT[$id]="$2"; fi
  echo "  [$2] gate $id: $3"
}
expect() {  # $1 = gate, $2 = description, $3 = expected, $4 = actual
  if [ "$3" = "$4" ]; then gate "$1" PASS "$2 = $4"; else gate "$1" FAIL "$2: expected '$3', got '$4'"; fi
}
dc() { docker compose "${COMPOSE_ARGS[@]}" "$@"; }
# Every query goes through checked(): a non-zero exit or an empty result is
# logged to $QUERY_FAILURES (which fails the run) and replaced by a marker that
# is unique per call, so two failed queries never compare equal.
QUERY_FAILURES="$LOG_DIR/query-failures.log"
checked() {
  local out rc=0
  out=$("$@" 2>>"$LOG_DIR/query.err") || rc=$?
  if [ "$rc" -ne 0 ] || [ -z "$out" ]; then
    printf 'exit=%s output=%s: %.300s\n' "$rc" "$([ -z "$out" ] && echo empty || echo present)" "$*" >>"$QUERY_FAILURES"
    echo "QUERY_FAILED_$(date +%s%N)_$RANDOM"
    return 0
  fi
  printf '%s\n' "$out"
}
trino() { checked dc exec -T trino trino --output-format TSV --execute "$1"; }
kafka() { checked dc exec -T kafka "/opt/kafka/bin/$1" --bootstrap-server kafka:29092 "${@:2}"; }
end_offsets_total() { kafka kafka-get-offsets.sh --topic "$1" | awk -F: '{s += $3} END {print s + 0}'; }
step() { echo; echo "== $*"; }

step "Isolation (project $PROJECT, source $GATE_SOURCE, trigger $STREAM_TRIGGER_INTERVAL)"
dc config --volumes | sed "s/^/  volume: ${PROJECT}_/"
echo "  network: ${PROJECT}_citibike; logs: $LOG_DIR"

step "Start the stack and the pipeline"
dc up -d --build >"$LOG_DIR/up.log" 2>&1 || { echo "docker compose up failed (see $LOG_DIR/up.log)"; exit 1; }
if [ "${RUN_EXTRA:-0}" = 1 ]; then
  if bash tests/smoke/infra_smoke.sh >"$LOG_DIR/smoke.log" 2>&1; then
    gate 15 PASS "infra_smoke.sh: $(grep -c '\[PASS\]' "$LOG_DIR/smoke.log") checks passed"
  else
    gate 15 FAIL "infra_smoke.sh failed (see $LOG_DIR/smoke.log)"
  fi
fi
dc --profile pipeline up -d --build gbfs-poller latest-state-consumer spark-streaming \
  >"$LOG_DIR/pipeline-up.log" 2>&1 || { echo "pipeline up failed (see $LOG_DIR/pipeline-up.log)"; exit 1; }
STARTED=$(date +%s)

step "Gate 3: dependencies"
pin() { grep -E "^$1==" infra/python/requirements.txt | sed 's/.*==//'; }
want="$(pin boto3) $(pin pydantic) $(pin confluent-kafka) $(pin 'psycopg\[binary\]')"
# The image the pipeline services run (built by `up --build` above); plain
# docker run, because `compose run` of a build service prints the build log.
got=$(docker run --rm --entrypoint sh citibike/python:local -c 'pip check >/dev/null && python -c "
import boto3, confluent_kafka, psycopg, pydantic
v = {\"pydantic\": pydantic.VERSION, \"confluent-kafka\": confluent_kafka.__version__,
     \"psycopg\": psycopg.__version__, \"boto3\": boto3.__version__}
print(\" \".join(v[k] for k in (\"boto3\", \"pydantic\", \"confluent-kafka\", \"psycopg\")))"' 2>"$LOG_DIR/deps.err")
expect 3 "python image (boto3 pydantic confluent-kafka psycopg), pip check" "$want" "$got"

echo "  waiting ${RUN_SECONDS}s for polls and micro-batches..."
sleep "$RUN_SECONDS"

jars=$(dc exec -T spark-streaming sh -c 'ls /root/.ivy2/jars' 2>/dev/null)
for jar in iceberg-spark-runtime-3.5_2.12-1.9.2.jar iceberg-aws-bundle-1.9.2.jar \
           spark-sql-kafka-0-10_2.12-3.5.6.jar hadoop-aws-3.3.4.jar; do
  # Ivy names resolved jars <groupId>_<artifact>-<version>.jar
  if grep -qE "(^|_)${jar//./\\.}\$" <<<"$jars"; then gate 3 PASS "spark jar $jar"
  else gate 3 FAIL "spark jar $jar missing"; fi
done
echo "  spark python: $(dc exec -T spark-streaming python3 --version 2>&1)"

step "Gate 2: pipeline containers keep running"
for svc in gbfs-poller latest-state-consumer spark-streaming; do
  cid=$(dc ps -q "$svc")
  state=$(docker inspect -f '{{.State.Status}} restarts={{.RestartCount}}' "$cid" 2>/dev/null)
  expect 2 "$svc after $(( $(date +%s) - STARTED ))s" "running restarts=0" "$state"
done

step "Gate 12: four queries, four checkpoints"
dc logs --no-color spark-streaming > "$LOG_DIR/spark-streaming.log" 2>&1
plans=$(grep -o '\[plan\] [a-z_]*: checkpoint=[^ ]*' "$LOG_DIR/spark-streaming.log" | sort -u)
echo "$plans" | sed 's/^/  /'
expect 12 "distinct queries in [plan]" "4" "$(echo "$plans" | awk '{print $2}' | sort -u | grep -c .)"
expect 12 "distinct checkpoints in [plan]" "4" "$(echo "$plans" | sed 's/.*checkpoint=//' | sort -u | grep -c .)"
listing=$(dc run --rm --no-deps --entrypoint sh minio-init -c \
  'mc alias set l http://minio:9000 "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY" >/dev/null && mc ls l/checkpoints/' 2>/dev/null)
for dir in status-history information-history silver-status silver-information; do
  if grep -q " $dir/\$" <<<"$listing"; then gate 12 PASS "s3://checkpoints/$dir/ exists"
  else gate 12 FAIL "s3://checkpoints/$dir/ missing"; fi
done

step "Quiesce: stop the poller, wait for the consumers to catch up"
dc stop gbfs-poller >/dev/null 2>&1
deadline=$(( $(date +%s) + CATCHUP_TIMEOUT ))
while :; do
  lag=$(kafka kafka-consumer-groups.sh --describe --group citibike-postgres-latest 2>/dev/null \
        | awk '$2 == "gbfs.station_status" && $6 ~ /^[0-9]+$/ {s += $6} END {print s + 0}')
  ends=$(end_offsets_total gbfs.station_status)
  bronze=$(trino "SELECT count(*) FROM iceberg.bronze.gbfs_status")
  silver=$(trino "SELECT count(*) FROM iceberg.silver.gbfs_status")
  keys=$(trino "SELECT count(*) FROM (SELECT DISTINCT station_id, poll_id FROM iceberg.bronze.gbfs_status WHERE dq_is_valid)")
  if [ "$lag" = 0 ] && [ "$bronze" = "$ends" ] && [ "$silver" = "$keys" ] \
     && [ "${ends:-0}" -gt 0 ] 2>/dev/null; then
    gate catchup PASS "consumers caught up: lag 0, Bronze = end offsets, Silver = valid keys"
    break
  fi
  if [ "$(date +%s)" -ge "$deadline" ]; then
    gate catchup FAIL "not caught up after ${CATCHUP_TIMEOUT}s: lag=$lag bronze=$bronze/$ends silver=$silver/$keys"
    break
  fi
  sleep 15
done
echo "  consumer lag=$lag, bronze=$bronze (Kafka end offsets $ends), silver=$silver (valid keys $keys)"

step "Gate 10: Bronze keeps every Kafka message with its metadata"
for topic in gbfs.station_status gbfs.station_information; do
  table=$([ "$topic" = gbfs.station_status ] && echo gbfs_status || echo gbfs_station_information)
  kafka_side=$(kafka kafka-get-offsets.sh --topic "$topic" | awk -F: '{print $2 " " $3 " " $3 - 1}' | sort -n)
  bronze_side=$(trino "SELECT kafka_partition, count(*), max(kafka_offset) FROM iceberg.bronze.$table GROUP BY 1 ORDER BY 1" | tr '\t' ' ')
  expect 10 "$table per partition (rows, max offset) vs Kafka end offsets" "$kafka_side" "$bronze_side"
  expect 10 "$table rows missing Kafka metadata or raw_value" "0" \
    "$(trino "SELECT count_if(kafka_topic IS NULL OR kafka_partition IS NULL OR kafka_offset IS NULL OR kafka_timestamp IS NULL OR raw_value IS NULL OR ingested_at IS NULL) FROM iceberg.bronze.$table")"
  expect 10 "$table valid rows whose Kafka key is not station_id" "0" \
    "$(trino "SELECT count_if(kafka_key IS DISTINCT FROM station_id) FROM iceberg.bronze.$table WHERE dq_is_valid")"
  expect 10 "$table duplicate Kafka messages" "0" \
    "$(trino "SELECT count(*) FROM (SELECT 1 FROM iceberg.bronze.$table GROUP BY kafka_topic, kafka_partition, kafka_offset, kafka_timestamp HAVING count(*) > 1)")"
done

step "Gate 5: poll IDs"
UUID='[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}'
for spec in "gbfs_status:station_status:2" "gbfs_station_information:station_information:1"; do
  IFS=: read -r table prefix min_polls <<<"$spec"
  expect 5 "$table valid rows with poll_id not '$prefix-<uuid4>'" "0" \
    "$(trino "SELECT count_if(NOT regexp_like(poll_id, '^$prefix-$UUID\$')) FROM iceberg.bronze.$table WHERE dq_is_valid")"
  expect 5 "$table polls with more than one collected_at" "0" \
    "$(trino "SELECT count(*) FROM (SELECT poll_id FROM iceberg.silver.$table GROUP BY 1 HAVING count(DISTINCT collected_at) > 1)")"
  polls=$(trino "SELECT count(DISTINCT poll_id) FROM iceberg.silver.$table")
  if [ "${polls:-0}" -ge "$min_polls" ]; then gate 5 PASS "$table has $polls poll(s) (>= $min_polls)"
  else gate 5 FAIL "$table has ${polls:-0} poll(s), need >= $min_polls"; fi
done
expect 5 "poll_id shared by status and information" "0" \
  "$(trino "SELECT count(*) FROM (SELECT poll_id FROM iceberg.silver.gbfs_status INTERSECT SELECT poll_id FROM iceberg.silver.gbfs_station_information)")"

step "Gate 7: PostgreSQL latest state == latest Silver observation (every field)"
if bash tests/integration/latest_state_reconcile.sh "$LOG_DIR/gate7" >"$LOG_DIR/gate7.log" 2>&1; then
  gate 7 PASS "$(grep '^\[PASS\]' "$LOG_DIR/gate7.log" | tail -1)"
else
  gate 7 FAIL "$(grep '^\[FAIL\]' "$LOG_DIR/gate7.log" | head -5 | tr '\n' ' ') (see $LOG_DIR/gate7.log)"
fi

step "Gate 13: restarting spark-streaming adds nothing"
before_b=$(trino "SELECT count(*) FROM iceberg.bronze.gbfs_status")
before_s=$(trino "SELECT count(*) FROM iceberg.silver.gbfs_status")
dc restart spark-streaming >/dev/null 2>&1
sleep 150    # restart + Ivy resolution + at least two 30 s triggers
expect 13 "bronze.gbfs_status rows after restart" "$before_b" "$(trino "SELECT count(*) FROM iceberg.bronze.gbfs_status")"
expect 13 "silver.gbfs_status rows after restart" "$before_s" "$(trino "SELECT count(*) FROM iceberg.silver.gbfs_status")"
expect 13 "duplicate Kafka messages in bronze.gbfs_status" "0" \
  "$(trino "SELECT count(*) FROM (SELECT 1 FROM iceberg.bronze.gbfs_status GROUP BY kafka_topic, kafka_partition, kafka_offset, kafka_timestamp HAVING count(*) > 1)")"
state=$(docker inspect -f '{{.State.Status}}' "$(dc ps -q spark-streaming)" 2>/dev/null)
expect 13 "spark-streaming after restart" "running" "$state"

step "Gate 11: Silver grain and data-quality gate"
for table in gbfs_status gbfs_station_information; do
  expect 11 "silver.$table duplicate (station_id, poll_id)" "0" \
    "$(trino "SELECT count(*) FROM (SELECT 1 FROM iceberg.silver.$table GROUP BY station_id, poll_id HAVING count(*) > 1)")"
done
dc stop spark-streaming >/dev/null 2>&1   # one writer per table; dq reads a quiet Bronze
for layer in silver_status silver_information; do
  if dc exec -T spark-master /opt/spark/bin/spark-submit /opt/project/streaming/jobs/dq_checks.py \
       --layer "$layer" >"$LOG_DIR/dq-$layer.log" 2>&1; then
    gate 11 PASS "dq_checks --layer $layer exit 0"
  else
    gate 11 FAIL "dq_checks --layer $layer failed: $(grep -E '^\[FAIL\]' "$LOG_DIR/dq-$layer.log" | tr '\n' ' ')"
  fi
done

if [ "${RUN_EXTRA:-0}" = 1 ]; then
  step "Gate 14: Trino reads Bronze and Silver"
  if bash tests/integration/str_trino_read.sh >"$LOG_DIR/trino-read.log" 2>&1; then
    gate 14 PASS "str_trino_read.sh ($(grep -c '\[PASS\]' "$LOG_DIR/trino-read.log") checks)"
  else
    gate 14 FAIL "str_trino_read.sh: $(grep '\[FAIL\]' "$LOG_DIR/trino-read.log" | head -3 | tr '\n' ' ')"
  fi
  step "Gates 8, 9: latest-state recovery"
  dc --profile pipeline start latest-state-consumer >/dev/null 2>&1
  if python3 -m unittest tests.integration.test_latest_state_recovery -v >"$LOG_DIR/recovery.log" 2>&1; then
    gate 8 PASS "test_latest_state_recovery test_1/test_2"; gate 9 PASS "test_latest_state_recovery test_3/test_4"
  else
    gate 8 FAIL "test_latest_state_recovery failed (see $LOG_DIR/recovery.log)"; gate 9 FAIL "see gate 8"
  fi
fi

if [ -s "$QUERY_FAILURES" ]; then
  gate queries FAIL "$(wc -l <"$QUERY_FAILURES") query(ies) failed or returned nothing (see $QUERY_FAILURES)"
else
  gate queries PASS "every query succeeded with output"
fi

echo
echo "== Summary (logs in $LOG_DIR)"
status=0
for id in "${ORDER[@]}"; do
  echo "  gate $id: ${RESULT[$id]}"
  [ "${RESULT[$id]}" = PASS ] || status=1
done
exit $status
