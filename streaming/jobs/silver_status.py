"""Silver station_status (STR-09): Bronze -> silver.gbfs_status.

Grain: one row = one station in one poll, key (station_id, poll_id). Every poll
is kept, even when nothing changed at the station. How rows are validated,
deduplicated and merged: see streaming/jobs/silver.py.

Run:
  # incremental catch-up from Bronze, then stop (manual, Airflow, tests)
  spark-submit /opt/project/streaming/jobs/silver_status.py --available-now

  # continuous; normally started by gbfs_history.py next to the Bronze queries
  spark-submit /opt/project/streaming/jobs/silver_status.py

Environment:
  SILVER_STATUS_TABLE                default lakehouse.silver.gbfs_status
  SILVER_STATUS_CHECKPOINT_LOCATION  checkpoint of the Bronze -> Silver query (on S3)
  BRONZE_STATUS_TABLE                source (see gbfs_history.py)
  STREAM_TRIGGER_INTERVAL            default "5 minutes"
"""
from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from streaming.jobs.gbfs_history import BRONZE_TABLES, build_spark
from streaming.jobs.silver import first_per_key, rebuild, start_silver_query, valid_messages

SILVER_STATUS_TABLE = os.environ.get("SILVER_STATUS_TABLE", "lakehouse.silver.gbfs_status")
SILVER_STATUS_CHECKPOINT = os.environ.get("SILVER_STATUS_CHECKPOINT_LOCATION")

SILVER_STATUS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    station_id              STRING    COMMENT 'grain: (station_id, poll_id)',
    poll_id                 STRING    COMMENT 'station_status-<uuid4>, one per poll',
    collected_at            TIMESTAMP COMMENT 'when the poller received the feed (UTC)',
    last_updated            TIMESTAMP COMMENT 'feed-level last_updated (epoch seconds -> UTC)',
    last_reported           TIMESTAMP COMMENT 'station last_reported (epoch seconds -> UTC)',
    num_bikes_available     INT,
    num_docks_available     INT,
    is_installed            BOOLEAN,
    is_renting              BOOLEAN,
    is_returning            BOOLEAN,
    vehicle_types_available ARRAY<STRUCT<vehicle_type_id: STRING, count: INT>>,
    schema_version          INT,
    kafka_partition         INT       COMMENT 'lineage: Bronze row kept for this key',
    kafka_offset            BIGINT,
    bronze_ingested_at      TIMESTAMP,
    silver_processed_at     TIMESTAMP
) USING iceberg
PARTITIONED BY (days(collected_at))
TBLPROPERTIES ('format-version' = '2')
"""
SILVER_STATUS_COLUMNS = [
    "station_id", "poll_id", "collected_at", "last_updated", "last_reported",
    "num_bikes_available", "num_docks_available", "is_installed", "is_renting",
    "is_returning", "vehicle_types_available", "schema_version",
    "kafka_partition", "kafka_offset", "bronze_ingested_at", "silver_processed_at",
]


def silver_status_candidates(bronze: DataFrame) -> DataFrame:
    """Bronze rows -> every valid message, typed, NOT deduplicated (no I/O). The
    streaming MERGE checks the poll's collected_at before deduplicating."""
    return valid_messages(bronze, "status").select(
        "station_id",
        "poll_id",
        F.to_timestamp("collected_at").alias("collected_at"),
        F.timestamp_seconds("last_updated").alias("last_updated"),
        F.timestamp_seconds("last_reported").alias("last_reported"),
        "num_bikes_available",
        "num_docks_available",
        "is_installed",
        "is_renting",
        "is_returning",
        "vehicle_types_available",
        "schema_version",
        "kafka_partition",
        "kafka_offset",
        "bronze_ingested_at",
        F.current_timestamp().alias("silver_processed_at"),
    )


def silver_status_rows(bronze: DataFrame) -> DataFrame:
    """Bronze rows -> valid, typed, one row per (station_id, poll_id) (no I/O)."""
    return first_per_key(silver_status_candidates(bronze))


def start_silver_status_query(spark: SparkSession, checkpoint: str | None = None,
                              available_now: bool = False,
                              from_timestamp_ms: int | None = None):
    return start_silver_query(spark, "status", SILVER_STATUS_TABLE, SILVER_STATUS_DDL,
                              silver_status_candidates, checkpoint or SILVER_STATUS_CHECKPOINT,
                              available_now, from_timestamp_ms)


def main() -> int:
    parser = argparse.ArgumentParser(description="Bronze -> silver.gbfs_status")
    parser.add_argument("--available-now", action="store_true",
                        help="process the Bronze rows not merged yet, then stop")
    parser.add_argument("--rebuild", action="store_true",
                        help="MERGE all of Bronze (current snapshot) into Silver, then stream "
                             "later Bronze commits with a NEW checkpoint (after expired "
                             "snapshots or a lost checkpoint)")
    parser.add_argument("--checkpoint-dir",
                        help="checkpoint root (uses <dir>/silver_status); "
                             "default SILVER_STATUS_CHECKPOINT_LOCATION")
    args = parser.parse_args()

    spark = build_spark("gbfs-silver-status")
    checkpoint = args.checkpoint_dir and f"{args.checkpoint_dir.rstrip('/')}/silver_status"
    from_ms = None
    if args.rebuild:
        checkpoint = checkpoint or SILVER_STATUS_CHECKPOINT
        if not checkpoint:
            raise SystemExit("--rebuild needs a new checkpoint (--checkpoint-dir or "
                             "SILVER_STATUS_CHECKPOINT_LOCATION)")
        from_ms = rebuild(spark, "status", SILVER_STATUS_TABLE, SILVER_STATUS_DDL, silver_status_candidates, checkpoint)
    query = start_silver_status_query(spark, checkpoint, args.available_now, from_ms)
    print(f"started silver: {BRONZE_TABLES['status']} -> {SILVER_STATUS_TABLE}", flush=True)
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
