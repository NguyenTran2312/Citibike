"""Shared Bronze -> Silver machinery (STR-09 status, STR-10 station information).

Both Silver tables have grain (station_id, poll_id) and are built the same way:

  * read the Bronze table incrementally (Iceberg streaming source; Bronze is
    append-only);
  * re-parse and re-validate `raw_value` with the STR-07 schema and rules, so
    Silver can be rebuilt from Bronze when the rules change; invalid messages
    stay in Bronze only;
  * keep one row per (station_id, poll_id): a replayed message (producer retry,
    poller --replay) is dropped, the lowest Kafka offset wins;
  * MERGE ... ON (station_id, poll_id) WHEN NOT MATCHED THEN INSERT: an
    observation never changes, so a replayed micro-batch or a lost Silver
    checkpoint adds no duplicate rows.

The Silver checkpoint must be on S3 (MinIO): the Iceberg streaming source writes
its offsets with the table's S3FileIO. The source needs every Bronze snapshot
after the one in its checkpoint: expiring them stops the query, and the way back
is rebuild() below (see streaming/README.md, "Giữ snapshot và dựng lại Silver").
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

# A poll's messages all carry the poll's collected_at, so an existing row for a
# key lies within a day of the incoming one. The window only prunes partitions;
# matching is on (station_id, poll_id) alone. A key replayed with a collected_at
# more than a day away (corrupt data) would get a second row, which the DQ check
# *_grain_unique reports.
PRUNE_MARGIN_MICROS = 24 * 3600 * 1_000_000


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


def merge_new_keys(rows: DataFrame, table: str, view: str) -> int:
    """Insert the (station_id, poll_id) keys `table` does not have yet; returns the
    number of candidate rows."""
    rows = rows.cache()
    try:
        # Epoch microseconds, not datetime literals: independent of the driver's
        # and the session's time zone.
        bounds = rows.agg(F.count(F.lit(1)).alias("n"),
                          F.min(F.unix_micros("collected_at")).alias("lo"),
                          F.max(F.unix_micros("collected_at")).alias("hi")).first()
        if not bounds["n"]:
            return 0  # nothing valid in this batch
        print(f"[silver] merge {view} into {table}: {bounds['n']} rows", flush=True)
        rows.createOrReplaceTempView(view)
        rows.sparkSession.sql(f"""
            MERGE INTO {table} t
            USING {view} s
            ON  t.station_id = s.station_id
            AND t.poll_id = s.poll_id
            AND t.collected_at BETWEEN timestamp_micros({bounds["lo"] - PRUNE_MARGIN_MICROS})
                                   AND timestamp_micros({bounds["hi"] + PRUNE_MARGIN_MICROS})
            WHEN NOT MATCHED THEN INSERT *
        """)
        rows.sparkSession.catalog.dropTempView(view)
        return bounds["n"]
    finally:
        rows.unpersist()


def _iceberg_table(spark: SparkSession, table: str):
    return spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, table)


def checkpoint_exists(spark: SparkSession, checkpoint: str) -> bool:
    path = spark._jvm.org.apache.hadoop.fs.Path(checkpoint)
    return path.getFileSystem(spark._jsc.hadoopConfiguration()).exists(path)


def start_silver_query(spark: SparkSession, feed: str, table: str, ddl: str,
                       to_silver: Callable[[DataFrame], DataFrame],
                       checkpoint: str | None, available_now: bool,
                       from_timestamp_ms: int | None = None):
    """Bronze(feed) -> Silver(table) streaming query with insert-only MERGE.

    `from_timestamp_ms` only applies to a new checkpoint: the query starts with
    the first Bronze snapshot committed after that time (used after rebuild()).
    """
    if not checkpoint:
        raise SystemExit(f"no checkpoint for silver {feed}: set "
                         f"SILVER_{feed.upper()}_CHECKPOINT_LOCATION or --checkpoint-dir")
    bronze_table = ensure_bronze_table(spark, feed)
    spark.sql(ddl.format(table=table))

    def merge_batch(batch: DataFrame, batch_id: int) -> None:
        merge_new_keys(to_silver(batch), table, f"silver_{feed}_batch_{batch_id}")

    reader = spark.readStream.format("iceberg")
    if from_timestamp_ms is not None:
        reader = reader.option("stream-from-timestamp", str(from_timestamp_ms))
    writer = (reader.load(bronze_table)
              .writeStream.queryName(f"gbfs_{feed}_silver")
              .foreachBatch(merge_batch)
              .option("checkpointLocation", checkpoint))
    writer = (writer.trigger(availableNow=True) if available_now
              else writer.trigger(processingTime=TRIGGER_INTERVAL))
    return writer.start()


def rebuild(spark: SparkSession, feed: str, table: str, ddl: str,
            to_silver: Callable[[DataFrame], DataFrame], new_checkpoint: str) -> int:
    """Recover Silver when its streaming source cannot continue (Bronze snapshots
    it needed were expired, checkpoint lost or corrupt):

    1. MERGE every Bronze row of the current snapshot into Silver (batch read;
       insert-only, so rows Silver already has are kept, not duplicated);
    2. return that snapshot's commit time; start_silver_query(..., new_checkpoint,
       from_timestamp_ms=<it> + 1) then continues with later Bronze commits only.

    `new_checkpoint` must not exist yet, so the old query state is never reused.
    """
    if checkpoint_exists(spark, new_checkpoint):
        raise SystemExit(f"rebuild needs a new checkpoint, {new_checkpoint} already exists")
    bronze_table = ensure_bronze_table(spark, feed)
    spark.sql(ddl.format(table=table))
    snapshot = _iceberg_table(spark, bronze_table).currentSnapshot()
    if snapshot is None:
        print(f"[silver] rebuild {table}: Bronze is empty", flush=True)
        return 0
    # Time travel in SQL: the REST catalog rejects .load(<table>) with a snapshot-id.
    bronze = spark.sql(f"SELECT * FROM {bronze_table} VERSION AS OF {snapshot.snapshotId()}")
    merged = merge_new_keys(to_silver(bronze), table, f"silver_{feed}_rebuild")
    print(f"[silver] rebuild {table} from {bronze_table} snapshot {snapshot.snapshotId()}: "
          f"{merged} candidate rows", flush=True)
    return snapshot.timestampMillis()
