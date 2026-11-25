"""Silver station information (STR-10): Bronze -> silver.gbfs_station_information.

Grain: one row = one station in one station_information poll, key
(station_id, poll_id). Each poll (every 24 h, plus one at every poller start) is
a full snapshot of the station list, kept as observed, so a station's name,
position, capacity or short_name can be read "as of" any poll. Every field of
the source feed is kept: station_id, name, short_name, lat, lon, capacity,
region_id, rental_uris, is_charging (and is_virtual_station, which the current
source does not send). Validation, dedup and MERGE: see streaming/jobs/silver.py.

Run:
  spark-submit /opt/project/streaming/jobs/silver_information.py --available-now
  # continuous: started by gbfs_history.py next to the Bronze queries

Environment:
  SILVER_INFORMATION_TABLE                default lakehouse.silver.gbfs_station_information
  SILVER_INFORMATION_CHECKPOINT_LOCATION  checkpoint of the Bronze -> Silver query (on S3)
  BRONZE_INFORMATION_TABLE                source (see gbfs_history.py)
"""
from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from streaming.jobs.gbfs_history import BRONZE_TABLES, build_spark
from streaming.jobs.silver import first_per_key, rebuild, start_silver_query, valid_messages

SILVER_INFORMATION_TABLE = os.environ.get("SILVER_INFORMATION_TABLE",
                                          "lakehouse.silver.gbfs_station_information")
SILVER_INFORMATION_CHECKPOINT = os.environ.get("SILVER_INFORMATION_CHECKPOINT_LOCATION")

SILVER_INFORMATION_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    station_id          STRING    COMMENT 'grain: (station_id, poll_id)',
    poll_id             STRING    COMMENT 'station_information-<uuid4>: one snapshot',
    collected_at        TIMESTAMP COMMENT 'when the poller received the feed (UTC)',
    last_updated        TIMESTAMP COMMENT 'feed-level last_updated (epoch seconds -> UTC)',
    name                STRING,
    short_name          STRING    COMMENT 'bridge to Trip History station ids',
    lat                 DOUBLE,
    lon                 DOUBLE,
    capacity            INT,
    region_id           STRING,
    is_virtual_station  BOOLEAN,
    is_charging         BOOLEAN,
    rental_uris         STRUCT<android: STRING, ios: STRING, web: STRING>,
    kafka_partition     INT       COMMENT 'lineage: Bronze row kept for this key',
    kafka_offset        BIGINT,
    bronze_ingested_at  TIMESTAMP,
    silver_processed_at TIMESTAMP
) USING iceberg
PARTITIONED BY (months(collected_at))
TBLPROPERTIES ('format-version' = '2')
"""
SILVER_INFORMATION_COLUMNS = [
    "station_id", "poll_id", "collected_at", "last_updated", "name", "short_name",
    "lat", "lon", "capacity", "region_id", "is_virtual_station", "is_charging",
    "rental_uris", "kafka_partition", "kafka_offset", "bronze_ingested_at",
    "silver_processed_at",
]


def silver_information_candidates(bronze: DataFrame) -> DataFrame:
    """Bronze rows -> every valid message, typed, NOT deduplicated (no I/O). The
    streaming MERGE checks the poll's collected_at before deduplicating."""
    return valid_messages(bronze, "information").select(
        "station_id",
        "poll_id",
        F.to_timestamp("collected_at").alias("collected_at"),
        F.timestamp_seconds("last_updated").alias("last_updated"),
        "name",
        "short_name",
        "lat",
        "lon",
        "capacity",
        "region_id",
        "is_virtual_station",
        "is_charging",
        "rental_uris",
        "kafka_partition",
        "kafka_offset",
        "bronze_ingested_at",
        F.current_timestamp().alias("silver_processed_at"),
    )


def silver_information_rows(bronze: DataFrame) -> DataFrame:
    """Bronze rows -> valid, typed, one row per (station_id, poll_id) (no I/O)."""
    return first_per_key(silver_information_candidates(bronze))


def start_silver_information_query(spark: SparkSession, checkpoint: str | None = None,
                                   available_now: bool = False,
                                   from_timestamp_ms: int | None = None):
    return start_silver_query(spark, "information", SILVER_INFORMATION_TABLE,
                              SILVER_INFORMATION_DDL, silver_information_candidates,
                              checkpoint or SILVER_INFORMATION_CHECKPOINT, available_now,
                              from_timestamp_ms)


def main() -> int:
    parser = argparse.ArgumentParser(description="Bronze -> silver.gbfs_station_information")
    parser.add_argument("--available-now", action="store_true",
                        help="process the Bronze rows not merged yet, then stop")
    parser.add_argument("--rebuild", action="store_true",
                        help="MERGE all of Bronze (current snapshot) into Silver, then stream "
                             "later Bronze commits with a NEW checkpoint (after expired "
                             "snapshots or a lost checkpoint)")
    parser.add_argument("--checkpoint-dir",
                        help="checkpoint root (uses <dir>/silver_information); "
                             "default SILVER_INFORMATION_CHECKPOINT_LOCATION")
    args = parser.parse_args()

    spark = build_spark("gbfs-silver-information")
    checkpoint = (args.checkpoint_dir
                  and f"{args.checkpoint_dir.rstrip('/')}/silver_information")
    from_ms = None
    if args.rebuild:
        checkpoint = checkpoint or SILVER_INFORMATION_CHECKPOINT
        if not checkpoint:
            raise SystemExit("--rebuild needs a new checkpoint (--checkpoint-dir or "
                             "SILVER_INFORMATION_CHECKPOINT_LOCATION)")
        from_ms = rebuild(spark, "information", SILVER_INFORMATION_TABLE, SILVER_INFORMATION_DDL, silver_information_candidates, checkpoint)
    query = start_silver_information_query(spark, checkpoint, args.available_now, from_ms)
    print(f"started silver: {BRONZE_TABLES['information']} -> {SILVER_INFORMATION_TABLE}",
          flush=True)
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
