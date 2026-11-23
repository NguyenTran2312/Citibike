"""Shared Bronze -> Silver machinery (STR-09 status, STR-10 station information).

Both Silver tables have grain (station_id, poll_id) and are built the same way:

  * read the Bronze table incrementally (Iceberg streaming source; Bronze is
    append-only);
  * re-parse and re-validate `raw_value` with the STR-07 schema and rules, so
    Silver can be rebuilt from Bronze when the rules change; invalid messages
    stay in Bronze only;
  * keep one row per (station_id, poll_id): a replayed message (producer retry,
    poller --replay) is dropped, the lowest Kafka offset wins;
  * MERGE ... WHEN NOT MATCHED THEN INSERT: an observation never changes, so a
    replayed micro-batch or a lost Silver checkpoint adds no duplicate rows.

The Silver checkpoint must be on S3 (MinIO): the Iceberg streaming source writes
its offsets with the table's S3FileIO.
"""
from __future__ import annotations

from typing import Callable

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from streaming.jobs.gbfs_history import (
    FEEDS,
    TRIGGER_INTERVAL,
    ensure_bronze_table,
    parse_value,
    validate,
)


def valid_messages(bronze: DataFrame, feed: str) -> DataFrame:
    """Bronze rows -> parsed messages that pass validation, with Kafka lineage."""
    as_kafka = bronze.select(
        F.col("raw_value").alias("value"),
        F.col("kafka_topic").alias("topic"),
        F.col("kafka_partition").alias("partition"),
        F.col("kafka_offset").alias("offset"),
        F.col("kafka_timestamp").alias("timestamp"),
        F.col("kafka_key").alias("key"),
        F.col("ingested_at").alias("bronze_ingested_at"),
    )
    parsed = parse_value(as_kafka, FEEDS[feed].schema, keep=("bronze_ingested_at",))
    return validate(parsed, feed).filter("dq_is_valid")


def first_per_key(df: DataFrame) -> DataFrame:
    """One row per (station_id, poll_id): the lowest Kafka position wins."""
    first = Window.partitionBy("station_id", "poll_id").orderBy("kafka_partition", "kafka_offset")
    return df.withColumn("_rank", F.row_number().over(first)).filter("_rank = 1").drop("_rank")


def merge_new_keys(rows: DataFrame, table: str, view: str) -> None:
    """Insert the (station_id, poll_id) keys `table` does not have yet."""
    rows = rows.cache()
    try:
        bounds = rows.agg(F.min("collected_at").alias("lo"),
                          F.max("collected_at").alias("hi")).first()
        if bounds["lo"] is None:
            return  # nothing valid in this batch
        rows.createOrReplaceTempView(view)
        # A key always has the same collected_at, so the literal range lets Iceberg
        # prune target partitions (partitioned by collected_at) instead of a full scan.
        rows.sparkSession.sql(f"""
            MERGE INTO {table} t
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


def start_silver_query(spark: SparkSession, feed: str, table: str, ddl: str,
                       to_silver: Callable[[DataFrame], DataFrame],
                       checkpoint: str | None, available_now: bool):
    """Bronze(feed) -> Silver(table) streaming query with insert-only MERGE."""
    if not checkpoint:
        raise SystemExit(f"no checkpoint for silver {feed}: set "
                         f"SILVER_{feed.upper()}_CHECKPOINT_LOCATION or --checkpoint-dir")
    bronze_table = ensure_bronze_table(spark, feed)
    spark.sql(ddl.format(table=table))

    def merge_batch(batch: DataFrame, batch_id: int) -> None:
        merge_new_keys(to_silver(batch), table, f"silver_{feed}_batch_{batch_id}")

    writer = (spark.readStream.format("iceberg").load(bronze_table)
              .writeStream.queryName(f"gbfs_{feed}_silver")
              .foreachBatch(merge_batch)
              .option("checkpointLocation", checkpoint))
    writer = (writer.trigger(availableNow=True) if available_now
              else writer.trigger(processingTime=TRIGGER_INTERVAL))
    return writer.start()
