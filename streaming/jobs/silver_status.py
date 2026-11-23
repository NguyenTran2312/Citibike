"""Silver station_status (STR-09): Bronze -> silver.gbfs_status.

Grain: one row = one station in one poll, key (station_id, poll_id).

  * Reads bronze.gbfs_status incrementally (Iceberg streaming source; Bronze is
    append-only).
  * Re-parses and re-validates `raw_value` with the same schema and rules as
    STR-07, so Silver can be rebuilt from Bronze when the rules change. Messages
    that fail stay in Bronze only.
  * Keeps every poll, even when nothing changed at the station.
  * Deduplicates: a message replayed for the same (station_id, poll_id) - producer
    retry or --replay - becomes one row (the lowest Kafka offset wins).
  * MERGE ... WHEN NOT MATCHED THEN INSERT: an observation never changes, so a
    replayed micro-batch, or a lost Silver checkpoint, adds no duplicate rows.

Run:
  # incremental catch-up from Bronze, then stop (manual, Airflow, tests)
  spark-submit /opt/project/streaming/jobs/silver_status.py --available-now

  # continuous; normally started by gbfs_history.py next to the Bronze queries
  spark-submit /opt/project/streaming/jobs/silver_status.py

Environment:
  SILVER_STATUS_TABLE                default lakehouse.silver.gbfs_status
  SILVER_STATUS_CHECKPOINT_LOCATION  checkpoint of the Bronze -> Silver query
  BRONZE_STATUS_TABLE                source (see gbfs_history.py)
  STREAM_TRIGGER_INTERVAL            default "5 minutes"
"""
from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from streaming.jobs.gbfs_history import (
    BRONZE_TABLES,
    TRIGGER_INTERVAL,
    build_spark,
    ensure_bronze_table,
    parse_value,
    validate,
)
from streaming.utils.schemas import STATION_STATUS_SCHEMA

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


def silver_status_rows(bronze: DataFrame) -> DataFrame:
    """Bronze rows -> valid, typed, deduplicated Silver rows (no I/O)."""
    as_kafka = bronze.select(
        F.col("raw_value").alias("value"),
        F.col("kafka_topic").alias("topic"),
        F.col("kafka_partition").alias("partition"),
        F.col("kafka_offset").alias("offset"),
        F.col("kafka_timestamp").alias("timestamp"),
        F.col("kafka_key").alias("key"),
        F.col("ingested_at").alias("bronze_ingested_at"),
    )
    valid = validate(parse_value(as_kafka, STATION_STATUS_SCHEMA, keep=("bronze_ingested_at",)),
                     "status").filter("dq_is_valid")

    first = Window.partitionBy("station_id", "poll_id").orderBy("kafka_partition", "kafka_offset")
    deduped = (valid.withColumn("_rank", F.row_number().over(first))
               .filter("_rank = 1").drop("_rank"))

    return deduped.select(
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


def ensure_silver_status_table(spark: SparkSession) -> str:
    spark.sql(SILVER_STATUS_DDL.format(table=SILVER_STATUS_TABLE))
    return SILVER_STATUS_TABLE


def merge_into_silver(batch: DataFrame, batch_id: int) -> None:
    """foreachBatch: insert keys Silver does not have yet. Safe to replay."""
    rows = silver_status_rows(batch).cache()
    try:
        bounds = rows.agg(F.min("collected_at").alias("lo"),
                          F.max("collected_at").alias("hi")).first()
        if bounds["lo"] is None:
            return  # nothing valid in this batch
        view = f"silver_status_batch_{batch_id}"
        rows.createOrReplaceTempView(view)
        # Same key always has the same collected_at, so the literal range lets
        # Iceberg prune target partitions (days(collected_at)) instead of a full scan.
        rows.sparkSession.sql(f"""
            MERGE INTO {SILVER_STATUS_TABLE} t
            USING {view} s
            ON  t.station_id = s.station_id
            AND t.poll_id = s.poll_id
            AND t.collected_at = s.collected_at
            AND t.collected_at BETWEEN TIMESTAMP '{bounds["lo"].isoformat()}'
                                   AND TIMESTAMP '{bounds["hi"].isoformat()}'
            WHEN NOT MATCHED THEN INSERT *
        """)
        rows.sparkSession.catalog.dropTempView(view)
    finally:
        rows.unpersist()


def start_silver_status_query(spark: SparkSession, checkpoint: str | None = None,
                              available_now: bool = False):
    checkpoint = checkpoint or SILVER_STATUS_CHECKPOINT
    if not checkpoint:
        raise SystemExit("no checkpoint: set SILVER_STATUS_CHECKPOINT_LOCATION or --checkpoint-dir")
    bronze_table = ensure_bronze_table(spark, "status")
    ensure_silver_status_table(spark)
    source = spark.readStream.format("iceberg").load(bronze_table)
    writer = (source.writeStream.queryName("gbfs_status_silver")
              .foreachBatch(merge_into_silver)
              .option("checkpointLocation", checkpoint))
    writer = (writer.trigger(availableNow=True) if available_now
              else writer.trigger(processingTime=TRIGGER_INTERVAL))
    return writer.start()


def main() -> int:
    parser = argparse.ArgumentParser(description="Bronze -> silver.gbfs_status")
    parser.add_argument("--available-now", action="store_true",
                        help="process the Bronze rows not merged yet, then stop")
    parser.add_argument("--checkpoint-dir",
                        help="checkpoint root (uses <dir>/silver_status); "
                             "default SILVER_STATUS_CHECKPOINT_LOCATION")
    args = parser.parse_args()

    spark = build_spark("gbfs-silver-status")
    checkpoint = args.checkpoint_dir and f"{args.checkpoint_dir.rstrip('/')}/silver_status"
    query = start_silver_status_query(spark, checkpoint, args.available_now)
    print(f"started silver: {BRONZE_TABLES['status']} -> {SILVER_STATUS_TABLE}", flush=True)
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
