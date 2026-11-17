#!/bin/bash
# Infrastructure smoke test (INF-07). Run after `docker compose up -d --build`:
#   bash tests/smoke/infra_smoke.sh
#
# Checks every P0 component and the Spark -> Iceberg -> Trino milestone:
#   1. containers healthy / init jobs exited 0       (INF-01)
#   2. MinIO buckets raw, warehouse, checkpoints, quarantine   (INF-02)
#   3. Kafka topics, partitions and retention        (INF-03)
#   4. Iceberg REST namespaces + PostgreSQL serving tables     (INF-05)
#   5. Spark -> MinIO (S3A) and Spark -> Iceberg     (INF-04)
#   6. Trino reads the rows Spark just wrote         (end-to-end)
#
# Set KEEP_SMOKE_TABLE=1 to keep iceberg.smoke.e2e for inspection.
set -uo pipefail
cd "$(dirname "$0")/../.."

PASS=0
FAIL=0
ok()   { echo "  [PASS] $*"; PASS=$((PASS + 1)); }
bad()  { echo "  [FAIL] $*"; FAIL=$((FAIL + 1)); }
step() { echo; echo "== $*"; }
dc()   { docker compose "$@"; }

RUN_ID="smoke_$(date -u +%Y%m%dT%H%M%S)"
ROWS=100

# Right after `up -d`, init jobs may still be running and health checks starting.
echo "Waiting for the stack to settle (max ${SETTLE_TIMEOUT:-180}s)..."
deadline=$((SECONDS + ${SETTLE_TIMEOUT:-180}))
while [ $SECONDS -lt $deadline ]; do
  busy=$(dc ps -a --format '{{.Service}} {{.Status}}' 2>/dev/null \
    | grep -E 'health: starting|(minio-init|kafka-init|catalog-init|catalog-volume-init) Up' || true)
  [ -z "$busy" ] && break
  sleep 3
done
[ -n "${busy:-}" ] && echo "Still not settled: $busy"

step "1. Containers (INF-01)"
for svc in minio iceberg-rest kafka postgres spark-master trino grafana; do
  cid=$(dc ps -q "$svc" 2>/dev/null)
  st=$([ -n "$cid" ] && docker inspect -f '{{.State.Status}}/{{if .State.Health}}{{.State.Health.Status}}{{end}}' "$cid")
  [ "$st" = "running/healthy" ] && ok "$svc healthy" || bad "$svc: ${st:-not created}"
done
cid=$(dc ps -q spark-worker 2>/dev/null)
st=$([ -n "$cid" ] && docker inspect -f '{{.State.Status}}' "$cid")
[ "$st" = "running" ] && ok "spark-worker running" || bad "spark-worker: ${st:-not created}"
for svc in catalog-volume-init minio-init kafka-init catalog-init; do
  cid=$(dc ps -aq "$svc" 2>/dev/null)
  st=$([ -n "$cid" ] && docker inspect -f '{{.State.Status}}/{{.State.ExitCode}}' "$cid")
  [ "$st" = "exited/0" ] && ok "$svc completed" || bad "$svc: ${st:-not created}"
done
workers=$(dc exec -T spark-master bash -c 'curl -s http://localhost:8080/json/ 2>/dev/null' \
  | grep -o '"aliveworkers" *: *[0-9]*' | grep -o '[0-9]*$')
[ "${workers:-0}" -ge 1 ] && ok "Spark master sees $workers alive worker(s)" || bad "Spark master sees no alive worker"

step "2. MinIO buckets (INF-02)"
buckets=$(dc exec -T minio sh -c 'export MC_CONFIG_DIR=/tmp/.mc;
  mc alias set local http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc ls local' 2>&1)
for b in raw warehouse checkpoints quarantine; do
  grep -q " $b/\$" <<<"$buckets" && ok "bucket $b" || bad "bucket $b missing"
done

step "3. Kafka topics (INF-03)"
for t in gbfs.station_status gbfs.station_information gbfs.invalid; do
  desc=$(dc exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic "$t" 2>&1 | head -1)
  cfg=$(dc exec -T kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:29092 \
    --entity-type topics --entity-name "$t" --describe 2>&1)
  if grep -q 'PartitionCount: 3' <<<"$desc" && grep -q 'retention.ms=604800000' <<<"$cfg"; then
    ok "topic $t (3 partitions, retention 7d)"
  else
    bad "topic $t: ${desc:-missing} | ${cfg}"
  fi
done

step "4. Iceberg REST catalog + PostgreSQL (INF-05)"
ns=$(dc exec -T iceberg-rest curl -s http://localhost:8181/v1/namespaces 2>&1)
for n in bronze silver gold; do
  grep -q "\[\"$n\"\]" <<<"$ns" && ok "namespace $n" || bad "namespace $n missing ($ns)"
done
tables=$(dc exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d citibike -tA' 2>&1 <<'SQL'
SELECT string_agg(table_name, ',' ORDER BY table_name) FROM information_schema.tables
WHERE table_name IN ('station_status_latest', 'active_alerts');
SQL
)
[ "$tables" = "active_alerts,station_status_latest" ] && ok "postgres tables: $tables" || bad "postgres tables: $tables"

step "5. Spark -> MinIO (S3A) and Spark -> Iceberg (INF-04)"
spark_log=$(mktemp)
if dc exec -T spark-master /opt/spark/bin/spark-submit \
     --master spark://spark-master:7077 --conf spark.driver.host=spark-master \
     /opt/project/tests/smoke/spark_iceberg_smoke.py "$RUN_ID" "$ROWS" >"$spark_log" 2>&1 \
   && grep -q '^SMOKE_OK' "$spark_log"; then
  grep '^\[smoke\]' "$spark_log" | sed 's/^/  /'
  ok "Spark wrote $ROWS rows to lakehouse.smoke.e2e (run_id=$RUN_ID)"
else
  bad "Spark smoke job failed, last lines of $spark_log:"
  grep -E 'Error|Exception|\[smoke\]' "$spark_log" | tail -15 | sed 's/^/      /'
fi

step "6. Trino reads Spark's rows (Spark -> Iceberg -> Trino)"
trino() { dc exec -T trino trino --output-format TSV --execute "$1" 2>&1 | grep -v '^WARNING'; }
schemas=$(trino "SHOW SCHEMAS FROM iceberg")
for n in bronze silver gold; do
  grep -qx "$n" <<<"$schemas" && ok "Trino sees schema iceberg.$n" || bad "Trino missing iceberg.$n"
done
count=$(trino "SELECT count(*) FROM iceberg.smoke.e2e WHERE run_id = '$RUN_ID'")
[ "$count" = "$ROWS" ] && ok "Trino counted $count rows written by Spark" || bad "Trino count: $count (expected $ROWS)"

if [ "${KEEP_SMOKE_TABLE:-0}" != "1" ]; then
  trino "DROP TABLE IF EXISTS iceberg.smoke.e2e" >/dev/null
  trino "DROP SCHEMA IF EXISTS iceberg.smoke" >/dev/null
fi
[ "$FAIL" -eq 0 ] && rm -f "$spark_log"

echo
echo "Smoke test: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
