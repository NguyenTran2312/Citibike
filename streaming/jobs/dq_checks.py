"""Data-quality checks for the streaming lakehouse tables.

Every check is a SQL query that returns the number of BAD rows (0 = pass), as in
the course capstone (spark/jobs/dq_checks.py). Exit code 1 when an "error" check
fails, so an orchestrator stops the steps after it.

    spark-submit /opt/project/streaming/jobs/dq_checks.py --layer silver_status
    spark-submit /opt/project/streaming/jobs/dq_checks.py --layer silver_information

The Silver reconciliation checks compare Silver with the Bronze keys that pass
validation *today* (raw_value re-validated with the same code Silver uses, not the
dq_is_valid flag stored at ingest time). Messages whose poll_id Silver recorded with
another collected_at (<silver table>_polls) are left out of Silver on purpose; a
warn check counts them. Run them on a quiet Bronze: stop the
spark-streaming service (or wait for its Silver queries to catch up) and run
silver_*.py --available-now first; otherwise rows Bronze committed after Silver's
last batch show up as "missing".
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import Dict, List, Tuple

from pyspark.sql import SparkSession

DEFAULT_TABLES = {
    "bronze_status": os.environ.get("BRONZE_STATUS_TABLE", "lakehouse.bronze.gbfs_status"),
    "silver_status": os.environ.get("SILVER_STATUS_TABLE", "lakehouse.silver.gbfs_status"),
    "bronze_information": os.environ.get("BRONZE_INFORMATION_TABLE",
                                         "lakehouse.bronze.gbfs_station_information"),
    "silver_information": os.environ.get("SILVER_INFORMATION_TABLE",
                                         "lakehouse.silver.gbfs_station_information"),
}
# layer -> (bronze table key, silver table key)
LAYER_TABLES = {
    "bronze_status": ("bronze_status", "silver_status"),
    "silver_status": ("bronze_status", "silver_status"),
    "bronze_information": ("bronze_information", "silver_information"),
    "silver_information": ("bronze_information", "silver_information"),
}

Check = Tuple[str, str, str]  # (name, severity, SQL returning the number of bad rows)


def _bronze_checks(prefix: str, b: str) -> List[Check]:
    return [
        # A Kafka message is (topic, partition, offset, kafka_timestamp), the same key
        # the Bronze sink deduplicates on (gbfs_history.MESSAGE_KEY).
        (f"{prefix}_no_duplicate_messages", "error", f"""
            SELECT count(*) FROM (
              SELECT kafka_topic, kafka_partition, kafka_offset, kafka_timestamp FROM {b}
              GROUP BY 1, 2, 3, 4 HAVING count(*) > 1)"""),
        # The same offset with another timestamp: the topic was deleted and
        # recreated (offsets restarted). Expected after that operation only.
        (f"{prefix}_offset_reused", "warn", f"""
            SELECT count(*) FROM (
              SELECT kafka_topic, kafka_partition, kafka_offset FROM {b}
              GROUP BY 1, 2, 3 HAVING count(DISTINCT kafka_timestamp) > 1)"""),
        (f"{prefix}_raw_value_kept", "error",
         f"SELECT count(*) FROM {b} WHERE raw_value IS NULL AND dq_error_type IS NULL"),
    ]


VALID_KEYS_VIEW = "dq_valid_bronze_keys"
CONFLICT_VIEW = "dq_poll_time_conflicts"


def _silver_key_checks(prefix: str, b: str, s: str) -> List[Check]:
    """Grain and reconciliation with Bronze, shared by both Silver tables.
    VALID_KEYS_VIEW and CONFLICT_VIEW are created by run() from the Bronze table `b`."""
    valid_keys = VALID_KEYS_VIEW
    return [
        # Valid messages that contradict their poll's collected_at (not in Silver).
        (f"{prefix}_poll_time_conflicts", "warn", f"SELECT count(*) FROM {CONFLICT_VIEW}"),
        (f"{prefix}_grain_unique", "error", f"""
            SELECT count(*) FROM (
              SELECT station_id, poll_id FROM {s} GROUP BY 1, 2 HAVING count(*) > 1)"""),
        # The MERGE matches on the poll's collected_at: one per poll.
        (f"{prefix}_one_collected_at_per_poll", "error", f"""
            SELECT count(*) FROM (
              SELECT poll_id FROM {s} GROUP BY 1 HAVING count(DISTINCT collected_at) > 1)"""),
        # Every valid Bronze key is in Silver (nothing dropped) ...
        (f"{prefix}_has_every_valid_bronze_key", "error", f"""
            SELECT count(*) FROM {valid_keys} v
            LEFT ANTI JOIN {s} t ON t.station_id = v.station_id AND t.poll_id = v.poll_id"""),
        # ... and Silver has nothing that Bronze did not accept as valid.
        (f"{prefix}_only_valid_bronze_keys", "error", f"""
            SELECT count(*) FROM {s} t
            LEFT ANTI JOIN {valid_keys} v
            ON t.station_id = v.station_id AND t.poll_id = v.poll_id"""),
        # A poll with far fewer stations than the largest poll is suspicious.
        (f"{prefix}_poll_completeness", "warn", f"""
            WITH polls AS (SELECT poll_id, count(*) AS n FROM {s} GROUP BY poll_id)
            SELECT count(*) FROM polls WHERE n < 0.9 * (SELECT max(n) FROM polls)"""),
    ]


def checks(layer: str, bronze: str, silver: str) -> List[Check]:
    b, s = bronze, silver
    if layer == "bronze_status":
        return _bronze_checks("bronze_status", b)
    if layer == "bronze_information":
        return _bronze_checks("bronze_information", b)
    if layer == "silver_status":
        return _silver_key_checks("silver_status", b, s) + [
            ("silver_status_required_not_null", "error", f"""
                SELECT count(*) FROM {s}
                WHERE station_id IS NULL OR poll_id IS NULL OR collected_at IS NULL
                   OR last_updated IS NULL OR last_reported IS NULL
                   OR num_bikes_available IS NULL OR num_docks_available IS NULL
                   OR is_installed IS NULL OR is_renting IS NULL OR is_returning IS NULL"""),
            ("silver_status_counts_non_negative", "error",
             f"SELECT count(*) FROM {s} WHERE num_bikes_available < 0 OR num_docks_available < 0"),
            ("silver_status_last_reported_not_after_collected", "warn",
             f"SELECT count(*) FROM {s} WHERE last_reported > collected_at"),
        ]
    if layer == "silver_information":
        return _silver_key_checks("silver_information", b, s) + [
            ("silver_information_required_not_null", "error", f"""
                SELECT count(*) FROM {s}
                WHERE station_id IS NULL OR poll_id IS NULL OR collected_at IS NULL
                   OR last_updated IS NULL OR name IS NULL OR lat IS NULL OR lon IS NULL"""),
            ("silver_information_coordinates_in_range", "error", f"""
                SELECT count(*) FROM {s}
                WHERE lat NOT BETWEEN -90 AND 90 OR lon NOT BETWEEN -180 AND 180"""),
            ("silver_information_capacity_non_negative", "error",
             f"SELECT count(*) FROM {s} WHERE capacity < 0"),
            # Trip History mapping needs short_name (scope Slide 11).
            ("silver_information_short_name_present", "warn",
             f"SELECT count(*) FROM {s} WHERE short_name IS NULL OR short_name = ''"),
        ]
    raise ValueError(f"unknown layer {layer}")


def create_valid_keys_view(spark: SparkSession, layer: str, bronze: str, polls: str):
    """Bronze keys that pass validation now (the same code path Silver uses), minus
    the messages whose poll Silver recorded with another collected_at."""
    from pyspark.sql import functions as F
    from pyspark.sql.utils import AnalysisException
    from streaming.jobs.silver import valid_messages
    feed = layer.split("_", 1)[1]
    valid = (valid_messages(spark.table(bronze), feed)
             .select("station_id", "poll_id", F.to_timestamp("collected_at").alias("at")))
    try:
        known = spark.table(polls).select("poll_id", F.col("collected_at").alias("poll_at"))
        valid = valid.join(known, "poll_id", "left")
    except AnalysisException:  # Silver never ran: no poll recorded yet
        valid = valid.withColumn("poll_at", F.lit(None).cast("timestamp"))
    valid = valid.cache()
    conflict = F.col("poll_at").isNotNull() & (F.col("at") != F.col("poll_at"))
    valid.filter(conflict).createOrReplaceTempView(CONFLICT_VIEW)
    (valid.filter(~conflict).select("station_id", "poll_id").distinct()
     .createOrReplaceTempView(VALID_KEYS_VIEW))
    return valid   # cached: unpersist after the checks


def run(spark: SparkSession, layer: str, bronze: str, silver: str,
        polls: str | None = None) -> bool:
    """`polls`: the Silver table's poll table, default <silver>_polls."""
    cached = None
    if layer.startswith("silver_"):
        from streaming.jobs.silver import polls_table
        cached = create_valid_keys_view(spark, layer, bronze, polls or polls_table(silver))
    ok = True
    try:
        for name, severity, sql in checks(layer, bronze, silver):
            bad = spark.sql(sql).collect()[0][0]
            status = "PASS" if bad == 0 else ("FAIL" if severity == "error" else "WARN")
            print(f"[{status}] {name}: {bad} bad rows", flush=True)
            if bad and severity == "error":
                ok = False
    finally:
        if cached is not None:
            cached.unpersist()
    return ok


def main() -> int:
    parser = argparse.ArgumentParser(description="Streaming lakehouse data-quality checks")
    parser.add_argument("--layer", choices=list(LAYER_TABLES), required=True)
    for key, default in DEFAULT_TABLES.items():
        parser.add_argument(f"--{key.replace('_', '-')}-table", dest=key, default=default)
    args = parser.parse_args()

    bronze_key, silver_key = LAYER_TABLES[args.layer]
    spark = SparkSession.builder.appName(f"dq-{args.layer}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    ok = run(spark, args.layer, getattr(args, bronze_key), getattr(args, silver_key))
    print(f"dq {args.layer}: {'PASSED' if ok else 'FAILED'}", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
