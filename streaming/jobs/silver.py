"""Shared Bronze -> Silver machinery (STR-09 status, STR-10 station information).

Both Silver tables have grain (station_id, poll_id) and are built the same way:

  * read the Bronze table incrementally (Iceberg streaming source; Bronze is
    append-only);
  * re-parse and re-validate `raw_value` with the STR-07 schema and rules, so
    Silver can be rebuilt from Bronze when the rules change; invalid messages
    stay in Bronze only;
  * keep one row per (station_id, poll_id): a replayed message (producer retry,
    poller --replay) is dropped, the lowest Kafka offset wins;
  * one collected_at per poll: the side table <silver table>_polls records the
    collected_at of every poll that reached Silver; a message whose poll_id is
    known with another collected_at (corrupt producer) stays in Bronze only;
  * MERGE ... ON (station_id, poll_id) WHEN NOT MATCHED THEN INSERT: an
    observation never changes, so a replayed micro-batch or a lost Silver
    checkpoint adds no duplicate rows. Because a poll has a single collected_at,
    the MERGE only reads the Silver partitions of the batch's collected_at values.

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

# rebuild() hands over to the stream this long before the Bronze snapshot it read:
# Iceberg picks the start snapshot by commit time, and snapshots committed in the
# same millisecond (or with a slightly wrong clock) must not be skipped. Reading a
# few snapshots twice is harmless, the MERGE only inserts new keys.
REBUILD_HANDOFF_MARGIN_MS = 60_000

POLLS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    poll_id      STRING    COMMENT 'one row per poll that reached the Silver table',
    collected_at TIMESTAMP COMMENT 'collected_at of every Silver row of this poll'
) USING iceberg
TBLPROPERTIES ('format-version' = '2')
"""


def polls_table(silver_table: str) -> str:
    return f"{silver_table}_polls"


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


def ensure_polls_table(spark: SparkSession, silver_table: str) -> str:
    """Create <silver_table>_polls; filled from Silver the first time, so a Silver
    table built before the poll table existed keeps its guarantee."""
    polls = polls_table(silver_table)
    spark.sql(POLLS_DDL.format(table=polls))
    if (_iceberg_table(spark, polls).currentSnapshot() is None
            and _iceberg_table(spark, silver_table).currentSnapshot() is not None):
        spark.sql(f"INSERT INTO {polls} SELECT poll_id, min(collected_at) "
                  f"FROM {silver_table} GROUP BY poll_id")
    return polls


def split_by_poll_time(rows: DataFrame, polls: str):
    """(rows to merge, polls new to Silver, number of rows dropped).

    A poll's collected_at is the one already recorded in `polls`, or for a new poll
    the collected_at of its first message (lowest Kafka position). Rows with another
    collected_at are dropped: they contradict an earlier message of the same poll.
    """
    first = Window.partitionBy("poll_id").orderBy("kafka_partition", "kafka_offset")
    batch_polls = (rows.withColumn("_rank", F.row_number().over(first))
                   .filter("_rank = 1")
                   .select("poll_id", F.col("collected_at").alias("_first_at")))
    known = (rows.sparkSession.table(polls)
             .join(F.broadcast(batch_polls.select("poll_id")), "poll_id")
             .select("poll_id", F.col("collected_at").alias("_known_at")))
    poll_time = (batch_polls.join(known, "poll_id", "left")
                 .select("poll_id",
                         F.coalesce("_known_at", "_first_at").alias("_poll_at"),
                         F.col("_known_at").isNull().alias("_new")))
    joined = rows.join(F.broadcast(poll_time), "poll_id")
    accepted = joined.filter(F.col("collected_at") == F.col("_poll_at")).select(*rows.columns)
    dropped = joined.filter(F.col("collected_at") != F.col("_poll_at")).count()
    new_polls = poll_time.filter("_new").select("poll_id",
                                                F.col("_poll_at").alias("collected_at"))
    return accepted, new_polls, dropped


def merge_new_keys(rows: DataFrame, table: str, view: str) -> int:
    """Insert the (station_id, poll_id) keys `table` does not have yet; returns the
    number of candidate rows. ensure_polls_table(table) must have run."""
    spark = rows.sparkSession
    polls = polls_table(table)
    rows = rows.cache()
    accepted = new_polls = None
    try:
        accepted, new_polls, dropped = split_by_poll_time(rows, polls)
        accepted, new_polls = accepted.cache(), new_polls.cache()
        # Epoch microseconds, not datetime literals: independent of the driver's
        # and the session's time zone.
        bounds = accepted.agg(F.count(F.lit(1)).alias("n"),
                              F.min(F.unix_micros("collected_at")).alias("lo"),
                              F.max(F.unix_micros("collected_at")).alias("hi")).first()
        if dropped:
            print(f"[silver] {table}: {dropped} rows dropped, their poll_id is known with "
                  f"another collected_at", flush=True)
        if not bounds["n"]:
            return 0  # nothing valid in this batch
        # Record the polls first: a crash before the MERGE leaves polls Silver does
        # not have yet, which the replayed batch then finds with the same time.
        if not new_polls.isEmpty():
            new_polls.writeTo(polls).append()
        print(f"[silver] merge {view} into {table}: {bounds['n']} rows", flush=True)
        accepted.createOrReplaceTempView(view)
        # Every Silver row of a poll has the poll's collected_at, so equal keys have
        # equal collected_at and the range below only prunes partitions.
        spark.sql(f"""
            MERGE INTO {table} t
            USING {view} s
            ON  t.station_id = s.station_id
            AND t.poll_id = s.poll_id
            AND t.collected_at = s.collected_at
            AND t.collected_at BETWEEN timestamp_micros({bounds["lo"]})
                                   AND timestamp_micros({bounds["hi"]})
            WHEN NOT MATCHED THEN INSERT *
        """)
        spark.catalog.dropTempView(view)
        return bounds["n"]
    finally:
        for df in (rows, accepted, new_polls):
            if df is not None:
                df.unpersist()


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
    ensure_polls_table(spark, table)

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
    2. return a time REBUILD_HANDOFF_MARGIN_MS before that snapshot's commit;
       start_silver_query(..., new_checkpoint, from_timestamp_ms=<it>) continues
       from there, reading the last snapshots again rather than missing one.

    `new_checkpoint` must not exist yet, so the old query state is never reused.
    Stop the spark-streaming service first: one writer per Silver table.
    """
    if checkpoint_exists(spark, new_checkpoint):
        raise SystemExit(f"rebuild needs a new checkpoint, {new_checkpoint} already exists")
    bronze_table = ensure_bronze_table(spark, feed)
    spark.sql(ddl.format(table=table))
    ensure_polls_table(spark, table)
    snapshot = _iceberg_table(spark, bronze_table).currentSnapshot()
    if snapshot is None:
        print(f"[silver] rebuild {table}: Bronze is empty", flush=True)
        return 0  # stream from the first Bronze snapshot
    # Time travel in SQL: the REST catalog rejects .load(<table>) with a snapshot-id.
    bronze = spark.sql(f"SELECT * FROM {bronze_table} VERSION AS OF {snapshot.snapshotId()}")
    merged = merge_new_keys(to_silver(bronze), table, f"silver_{feed}_rebuild")
    print(f"[silver] rebuild {table} from {bronze_table} snapshot {snapshot.snapshotId()}: "
          f"{merged} candidate rows", flush=True)
    return snapshot.timestampMillis() - REBUILD_HANDOFF_MARGIN_MS
