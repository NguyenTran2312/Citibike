#!/bin/bash
# Gate 7: PostgreSQL latest state == latest Silver observation of every station.
#
#   bash tests/integration/latest_state_reconcile.sh <output dir>
#
# Exports public.station_status_latest (PostgreSQL) and, per station, the latest
# row of silver.gbfs_status (Trino; max collected_at, ties by max poll_id) as CSV
# with full timestamp precision, then compares every shared field with
# tests/integration/latest_state_compare.py. Exit 0 only if both exports succeed,
# both are non-empty and every station matches; the CSVs and export errors stay
# in <output dir>.
#
# Run against a quiet pipeline (poller stopped, consumer and Silver caught up),
# in the isolated Compose project (COMPOSE_PROJECT_NAME=<name>-itest).
#
# For tests, the two exports can be replaced by shell commands that write CSV to
# stdout: PG_EXPORT_CMD and SILVER_EXPORT_CMD.
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 2
OUT="${1:?usage: latest_state_reconcile.sh <output dir>}"
mkdir -p "$OUT" || exit 2

PG_SQL="COPY (SELECT station_id, poll_id, num_bikes_available, num_docks_available,
  is_installed, is_renting, is_returning,
  to_char(last_updated AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') AS last_updated,
  to_char(last_reported AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') AS last_reported,
  to_char(collected_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') AS collected_at
  FROM public.station_status_latest ORDER BY station_id) TO STDOUT WITH (FORMAT csv, HEADER true)"

SILVER_SQL="SELECT station_id, poll_id, num_bikes_available, num_docks_available,
  is_installed, is_renting, is_returning,
  to_iso8601(last_updated AT TIME ZONE 'UTC') AS last_updated,
  to_iso8601(last_reported AT TIME ZONE 'UTC') AS last_reported,
  to_iso8601(collected_at AT TIME ZONE 'UTC') AS collected_at
FROM (SELECT *, row_number() OVER (PARTITION BY station_id
                                   ORDER BY collected_at DESC, poll_id DESC) AS rn
      FROM iceberg.silver.gbfs_status)
WHERE rn = 1 ORDER BY station_id"

export_pg() {
  if [ -n "${PG_EXPORT_CMD:-}" ]; then bash -c "$PG_EXPORT_CMD"; return; fi
  docker compose exec -T postgres psql -U "${POSTGRES_USER:-citibike}" -d citibike \
    -v ON_ERROR_STOP=1 -X -q -c "$PG_SQL"
}

export_silver() {
  if [ -n "${SILVER_EXPORT_CMD:-}" ]; then bash -c "$SILVER_EXPORT_CMD"; return; fi
  docker compose exec -T trino trino --output-format CSV_HEADER --execute "$SILVER_SQL"
}

status=0
run_export() {  # $1 = label, $2 = function, $3 = output csv
  local rc=0
  "$2" >"$3" 2>"$3.err" || rc=$?
  if [ "$rc" -ne 0 ]; then
    echo "[FAIL] $1 export failed with exit $rc: $(head -c 300 "$3.err" | tr '\n' ' ')"
    status=1
  elif [ ! -s "$3" ]; then
    echo "[FAIL] $1 export returned no output"
    status=1
  fi
}
run_export PostgreSQL export_pg "$OUT/pg_latest.csv"
run_export Silver export_silver "$OUT/silver_latest.csv"
[ "$status" = 0 ] || exit 1

python3 tests/integration/latest_state_compare.py "$OUT/pg_latest.csv" "$OUT/silver_latest.csv"
