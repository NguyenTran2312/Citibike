"""GBFS history job - Spark Structured Streaming: Kafka -> from_json -> validate -> sink.

Entry point of the `spark-streaming` service in docker-compose.yml.

Current scope (STR-07): Kafka source, from_json with the agreed schemas,
valid / invalid classification, fixture check, console sink, restart from a
test checkpoint. The Iceberg sink (dedup on (station_id, poll_id),
STATUS_/INFORMATION_CHECKPOINT_LOCATION) comes in a later issue.

Run inside the spark containers (see streaming/README.md):
  # parse + validate the fixture JSON (no Kafka needed)
  spark-submit /opt/project/streaming/jobs/gbfs_history.py --fixture

  # read Kafka, print valid and invalid rows of every micro-batch
  spark-submit /opt/project/streaming/jobs/gbfs_history.py --feed status \
      --checkpoint-dir /tmp/str07-checkpoints [--available-now]

Environment (names match docker-compose.yml):
  KAFKA_BOOTSTRAP_SERVERS   default kafka:29092
  KAFKA_GROUP_ID            used as groupIdPrefix (Spark adds a unique suffix per query)
  GBFS_STATUS_TOPIC         default gbfs.station_status
  GBFS_INFORMATION_TOPIC    default gbfs.station_information
  STREAM_TRIGGER_INTERVAL   default "5 minutes" (contract 2.3.3); use "30 seconds" to debug
  STARTING_OFFSETS          default earliest (first run only; then the checkpoint wins)
  MAX_OFFSETS_PER_TRIGGER   default 5000
"""
from __future__ import annotations  # apache/spark:3.5.6 ships Python 3.8

import argparse
import os
import sys
from pathlib import Path
from typing import Callable, Dict, List, NamedTuple, Tuple

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from streaming.utils.schemas import (
    INVALID_SCHEMA,
    STATION_INFORMATION_SCHEMA,
    STATION_STATUS_SCHEMA,
    TOPIC_INVALID,
    TOPIC_STATION_INFORMATION,
    TOPIC_STATION_STATUS,
)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "citibike-iceberg-history")
STATUS_TOPIC = os.environ.get("GBFS_STATUS_TOPIC", TOPIC_STATION_STATUS)
INFORMATION_TOPIC = os.environ.get("GBFS_INFORMATION_TOPIC", TOPIC_STATION_INFORMATION)
TRIGGER_INTERVAL = os.environ.get("STREAM_TRIGGER_INTERVAL", "5 minutes")
STARTING_OFFSETS = os.environ.get("STARTING_OFFSETS", "earliest")
MAX_OFFSETS_PER_TRIGGER = int(os.environ.get("MAX_OFFSETS_PER_TRIGGER", "5000"))

# error_type values, same vocabulary as the gbfs.invalid contract (docs/contracts.md 2.3.2)
PARSE_ERROR = "PARSE_ERROR"            # not JSON at all (or empty message)
SCHEMA_ERROR = "SCHEMA_ERROR"          # valid JSON, but not an object or a field has the wrong type
VALIDATION_ERROR = "VALIDATION_ERROR"  # parsed, but breaks a contract rule (missing field, range...)

CORRUPT_COL = "_corrupt_record"
KAFKA_META = ["kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp"]
# ISO-8601 with an explicit timezone: ...Z or ...+07:00 / -0500
TZ_SUFFIX = r"(Z|[+-][0-9]{2}:?[0-9]{2})$"

Rule = Tuple[Column, str]  # (condition that means "broken", message)


# --- Validation rules per feed (mirror ingestion/gbfs/json_schemas) ----------
def _required(*fields: str) -> List[Rule]:
    return [(F.col(f).isNull(), f"missing {f}") for f in fields]


def _non_empty(*fields: str) -> List[Rule]:
    return [(F.length(F.col(f)) == 0, f"empty {f}") for f in fields]


def _non_negative(*fields: str) -> List[Rule]:
    return [(F.col(f) < 0, f"negative {f}") for f in fields]


def _iso_datetime_with_tz(field: str) -> List[Rule]:
    c = F.col(field)
    return [
        (c.isNotNull() & ~c.rlike(TZ_SUFFIX), f"{field} has no timezone"),
        (c.isNotNull() & F.to_timestamp(c).isNull(), f"{field} is not an ISO-8601 datetime"),
    ]


def status_rules() -> List[Rule]:
    bad_vehicle = F.exists(
        F.col("vehicle_types_available"),
        lambda v: v["vehicle_type_id"].isNull() | (F.length(v["vehicle_type_id"]) == 0)
        | v["count"].isNull() | (v["count"] < 0),
    )
    return (
        _required("poll_id", "station_id", "num_bikes_available", "num_docks_available",
                  "is_installed", "is_renting", "is_returning", "last_reported",
                  "last_updated", "collected_at")
        + _non_empty("poll_id", "station_id")
        + _non_negative("num_bikes_available", "num_docks_available",
                        "last_reported", "last_updated")
        + _iso_datetime_with_tz("collected_at")
        + [(F.col("schema_version").isNotNull() & (F.col("schema_version") != 1),
            "schema_version must be 1"),
           (bad_vehicle, "invalid vehicle_types_available item")]
    )


def information_rules() -> List[Rule]:
    return (
        _required("last_updated", "collected_at", "station_id", "name", "lat", "lon")
        + _non_empty("station_id", "name")
        + _non_negative("last_updated", "capacity")
        + _iso_datetime_with_tz("collected_at")
        + [((F.col("lat") < -90) | (F.col("lat") > 90), "lat out of range [-90, 90]"),
           ((F.col("lon") < -180) | (F.col("lon") > 180), "lon out of range [-180, 180]")]
    )


def invalid_rules() -> List[Rule]:
    return (
        _required("source", "error_type", "error_message", "raw_payload", "detected_at")
        + _non_empty("source", "error_type", "error_message")
        + _iso_datetime_with_tz("detected_at")
    )


class Feed(NamedTuple):
    topic: str
    schema: StructType
    rules: Callable[[], List[Rule]]


FEEDS: Dict[str, Feed] = {
    "status": Feed(STATUS_TOPIC, STATION_STATUS_SCHEMA, status_rules),
    "information": Feed(INFORMATION_TOPIC, STATION_INFORMATION_SCHEMA, information_rules),
    # Not streamed by this job (the producer writes it); kept for the fixture/contract check.
    "invalid": Feed(TOPIC_INVALID, INVALID_SCHEMA, invalid_rules),
}
STREAM_FEEDS = ["status", "information"]


# --- Spark session and Kafka source ------------------------------------------
def build_spark(app_name: str = "gbfs-history") -> SparkSession:
    # Timezone, shuffle partitions, connectors and the Iceberg catalog come from
    # infra/spark/spark-defaults.conf.
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka(spark: SparkSession, topic: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("groupIdPrefix", KAFKA_GROUP_ID)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .option("failOnDataLoss", "false")
        .load()
    )


# --- Parse and validate (pure DataFrame -> DataFrame, also used by tests) -----
def parse_value(df: DataFrame, schema: StructType) -> DataFrame:
    """from_json on `value`. Works on Kafka (binary value) and text DataFrames.

    Output: Kafka metadata (if present), every schema field, `_corrupt_record`
    (raw text when from_json could not map it to the schema) and `raw_value`.
    """
    value_str = F.col("value").cast("string")
    parse_schema = StructType(schema.fields + [StructField(CORRUPT_COL, StringType(), True)])
    meta = []
    if "partition" in df.columns:  # Kafka source
        meta = [
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        ]
    parsed = df.select(
        *meta,
        F.from_json(value_str, parse_schema, {"columnNameOfCorruptRecord": CORRUPT_COL}).alias("e"),
        value_str.alias("raw_value"),
    )
    return parsed.select(*[c for c in KAFKA_META if c in parsed.columns], "e.*", "raw_value")


def validate(parsed: DataFrame, feed: str) -> DataFrame:
    """Add `dq_is_valid`, `dq_error_type`, `dq_error_message` to the output of parse_value.

    Prefixed so they never clash with schema fields (gbfs.invalid has its own error_type).
    """
    raw = F.col("raw_value")
    corrupt = F.col(CORRUPT_COL)
    empty = raw.isNull() | (F.length(F.trim(raw)) == 0)
    is_json = F.get_json_object(raw, "$").isNotNull()

    errors = F.filter(
        F.array(*[F.when(cond, F.lit(msg)) for cond, msg in FEEDS[feed].rules()]),
        lambda m: m.isNotNull(),
    )
    error_type = (
        F.when(empty, F.lit(PARSE_ERROR))
        .when(corrupt.isNotNull() & ~is_json, F.lit(PARSE_ERROR))
        .when(corrupt.isNotNull(), F.lit(SCHEMA_ERROR))
        .when(F.size(errors) > 0, F.lit(VALIDATION_ERROR))
    )
    error_message = (
        F.when(empty, F.lit("empty message"))
        .when(corrupt.isNotNull() & ~is_json, F.lit("payload is not valid JSON"))
        .when(corrupt.isNotNull(),
              F.lit("JSON does not match schema (not an object or a field has the wrong type)"))
        .when(F.size(errors) > 0, F.array_join(errors, "; "))
    )
    return (parsed
            .withColumn("dq_error_type", error_type)
            .withColumn("dq_error_message", error_message)
            .withColumn("dq_is_valid", F.col("dq_error_type").isNull()))


def valid_rows(validated: DataFrame, feed: str) -> DataFrame:
    """Valid messages with exactly the schema fields (+ Kafka metadata)."""
    meta = [c for c in KAFKA_META if c in validated.columns]
    return validated.filter("dq_is_valid").select(*meta, *FEEDS[feed].schema.fieldNames())


def invalid_rows(validated: DataFrame, feed: str) -> DataFrame:
    """Invalid messages in the gbfs.invalid contract shape (+ Kafka metadata)."""
    meta = [c for c in KAFKA_META if c in validated.columns]
    return validated.filter(~F.col("dq_is_valid")).select(
        *meta,
        F.lit(FEEDS[feed].topic).alias("source"),
        F.col("dq_error_type").alias("error_type"),
        F.col("dq_error_message").alias("error_message"),
        F.col("raw_value").alias("raw_payload"),
        F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("detected_at"),
    )


# --- Fixture check -------------------------------------------------------------
FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "gbfs"
# (feed, file, expect_valid)
FIXTURES = [
    ("status", "station_status.jsonl", True),
    ("information", "station_information.jsonl", True),
    ("invalid", "invalid.jsonl", True),
    ("status", "station_status_bad.jsonl", False),
]


def _count_valid(validated: DataFrame) -> Tuple[int, int]:
    """(total, valid) in one aggregation pass."""
    row = validated.agg(F.count(F.lit(1)).alias("total"),
                        F.count(F.when(F.col("dq_is_valid"), 1)).alias("valid")).first()
    return row["total"], row["valid"]


def run_fixture_check(spark: SparkSession) -> bool:
    ok = True
    for feed, filename, expect_valid in FIXTURES:
        raw = spark.read.text(str(FIXTURE_DIR / filename))  # one JSON message per line
        validated = validate(parse_value(raw, FEEDS[feed].schema), feed)
        total, n_valid = _count_valid(validated)
        wrong = total - n_valid if expect_valid else n_valid

        print(f"\n===== {filename} ({feed}, expect {'valid' if expect_valid else 'invalid'}) =====")
        validated.select(*FEEDS[feed].schema.fieldNames(),
                         "dq_is_valid", "dq_error_type", "dq_error_message") \
            .show(truncate=False)
        if total == 0 or wrong:
            ok = False
            print(f"[FAIL] {filename}: {wrong}/{total} rows not {'valid' if expect_valid else 'invalid'}")

    print("\nfixture check:", "PASSED" if ok else "FAILED")
    return ok


# --- Streaming -------------------------------------------------------------------
def make_console_batch_writer(feed: str):
    def write_batch(batch: DataFrame, batch_id: int) -> None:
        validated = validate(batch, feed).cache()  # avoid re-reading Kafka for each action
        total, n_valid = _count_valid(validated)
        n_invalid = total - n_valid
        print(f"\n[{feed}] batch {batch_id}: {n_valid} valid, {n_invalid} invalid", flush=True)
        if n_valid:
            valid_rows(validated, feed).orderBy("kafka_partition", "kafka_offset") \
                .show(50, truncate=False)
        if n_invalid:
            invalid_rows(validated, feed).orderBy("kafka_partition", "kafka_offset") \
                .show(50, truncate=False)
        validated.unpersist()
    return write_batch


def start_console_query(spark: SparkSession, feed: str, checkpoint_dir: str | None,
                        available_now: bool):
    parsed = parse_value(read_kafka(spark, FEEDS[feed].topic), FEEDS[feed].schema)
    writer = parsed.writeStream.queryName(f"gbfs_{feed}_console").foreachBatch(
        make_console_batch_writer(feed))
    # Test checkpoint only. Never reuse STATUS_/INFORMATION_CHECKPOINT_LOCATION here:
    # those belong to the Iceberg sink, and a console checkpoint would corrupt them.
    if checkpoint_dir:
        writer = writer.option("checkpointLocation", f"{checkpoint_dir.rstrip('/')}/{feed}")
    if available_now:
        writer = writer.trigger(availableNow=True)  # process what is in Kafka now, then stop
    else:
        writer = writer.trigger(processingTime=TRIGGER_INTERVAL)
    return writer.start()


def main() -> int:
    parser = argparse.ArgumentParser(description="GBFS Kafka -> Spark history job")
    parser.add_argument("--fixture", action="store_true",
                        help="parse + validate fixture JSON instead of reading Kafka")
    parser.add_argument("--sink", choices=["console"], default="console",
                        help="output sink; 'iceberg' will be added after STR-07")
    parser.add_argument("--feed", choices=STREAM_FEEDS + ["all"], default="all",
                        help="which feed to stream (default: %(default)s)")
    parser.add_argument("--checkpoint-dir",
                        help="test checkpoint root; each feed gets <dir>/<feed>. "
                             "Without it Spark uses a temp dir and restarts from STARTING_OFFSETS.")
    parser.add_argument("--available-now", action="store_true",
                        help="process the data available now, then stop (restart tests)")
    args = parser.parse_args()

    spark = build_spark()
    if args.fixture:
        ok = run_fixture_check(spark)
        spark.stop()
        return 0 if ok else 1

    feeds = STREAM_FEEDS if args.feed == "all" else [args.feed]
    for feed in feeds:
        start_console_query(spark, feed, args.checkpoint_dir, args.available_now)
        print(f"started {feed}: {FEEDS[feed].topic} -> {args.sink}", flush=True)

    if args.available_now:
        for q in spark.streams.active:
            q.awaitTermination()
    else:
        spark.streams.awaitAnyTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
