"""Data-quality checks for the streaming lakehouse tables.

Every check is a SQL query that returns the number of BAD rows (0 = pass), as in
the course capstone (spark/jobs/dq_checks.py). Exit code 1 when an "error" check
fails, so an orchestrator stops the steps after it.

    spark-submit /opt/project/streaming/jobs/dq_checks.py --layer silver_status

Run reconciliation checks after Silver has caught up with Bronze (for example
right after silver_status.py --available-now); while the streaming query is
running, Silver lags Bronze by up to one trigger.
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import Dict, List, Tuple

from pyspark.sql import SparkSession

BRONZE_STATUS = os.environ.get("BRONZE_STATUS_TABLE", "lakehouse.bronze.gbfs_status")
SILVER_STATUS = os.environ.get("SILVER_STATUS_TABLE", "lakehouse.silver.gbfs_status")

Check = Tuple[str, str, str]  # (name, severity, SQL returning the number of bad rows)


def checks(bronze_status: str, silver_status: str) -> Dict[str, List[Check]]:
    b, s = bronze_status, silver_status
    return {
        "bronze_status": [
            ("bronze_status_no_duplicate_offsets", "error", f"""
                SELECT count(*) FROM (
                  SELECT kafka_topic, kafka_partition, kafka_offset FROM {b}
                  GROUP BY 1, 2, 3 HAVING count(*) > 1)"""),
            ("bronze_status_raw_value_kept", "error",
             f"SELECT count(*) FROM {b} WHERE raw_value IS NULL AND dq_error_type IS NULL"),
        ],
        "silver_status": [
            ("silver_status_grain_unique", "error", f"""
                SELECT count(*) FROM (
                  SELECT station_id, poll_id FROM {s} GROUP BY 1, 2 HAVING count(*) > 1)"""),
            ("silver_status_required_not_null", "error", f"""
                SELECT count(*) FROM {s}
                WHERE station_id IS NULL OR poll_id IS NULL OR collected_at IS NULL
                   OR last_updated IS NULL OR last_reported IS NULL
                   OR num_bikes_available IS NULL OR num_docks_available IS NULL
                   OR is_installed IS NULL OR is_renting IS NULL OR is_returning IS NULL"""),
            ("silver_status_counts_non_negative", "error",
             f"SELECT count(*) FROM {s} WHERE num_bikes_available < 0 OR num_docks_available < 0"),
            # Every valid Bronze key is in Silver (nothing dropped) ...
            ("silver_status_has_every_valid_bronze_key", "error", f"""
                SELECT count(*) FROM (
                  SELECT DISTINCT station_id, poll_id FROM {b} WHERE dq_is_valid) v
                LEFT ANTI JOIN {s} t ON t.station_id = v.station_id AND t.poll_id = v.poll_id"""),
            # ... and Silver has nothing that Bronze did not accept as valid.
            ("silver_status_only_valid_bronze_keys", "error", f"""
                SELECT count(*) FROM {s} t
                LEFT ANTI JOIN (
                  SELECT DISTINCT station_id, poll_id FROM {b} WHERE dq_is_valid) v
                ON t.station_id = v.station_id AND t.poll_id = v.poll_id"""),
            # A poll with far fewer stations than the largest poll is suspicious.
            ("silver_status_poll_completeness", "warn", f"""
                WITH polls AS (SELECT poll_id, count(*) AS n FROM {s} GROUP BY poll_id)
                SELECT count(*) FROM polls
                WHERE n < 0.9 * (SELECT max(n) FROM polls)"""),
            ("silver_status_last_reported_not_after_collected", "warn",
             f"SELECT count(*) FROM {s} WHERE last_reported > collected_at"),
        ],
    }


def run(spark: SparkSession, layer: str, bronze_status: str, silver_status: str) -> bool:
    ok = True
    for name, severity, sql in checks(bronze_status, silver_status)[layer]:
        bad = spark.sql(sql).collect()[0][0]
        status = "PASS" if bad == 0 else ("FAIL" if severity == "error" else "WARN")
        print(f"[{status}] {name}: {bad} bad rows", flush=True)
        if bad and severity == "error":
            ok = False
    return ok


def main() -> int:
    parser = argparse.ArgumentParser(description="Streaming lakehouse data-quality checks")
    parser.add_argument("--layer", choices=["bronze_status", "silver_status"], required=True)
    parser.add_argument("--bronze-status-table", default=BRONZE_STATUS)
    parser.add_argument("--silver-status-table", default=SILVER_STATUS)
    args = parser.parse_args()

    spark = SparkSession.builder.appName(f"dq-{args.layer}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    ok = run(spark, args.layer, args.bronze_status_table, args.silver_status_table)
    print(f"dq {args.layer}: {'PASSED' if ok else 'FAILED'}", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
