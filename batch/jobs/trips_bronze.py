from __future__ import annotations

import argparse
import os
import re
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

CITI_BIKE_13_COLS = [
    "ride_id",
    "rideable_type",
    "started_at",
    "ended_at",
    "start_station_name",
    "start_station_id",
    "end_station_name",
    "end_station_id",
    "start_lat",
    "start_lng",
    "end_lat",
    "end_lng",
    "member_casual",
]


def _sanitize_identifier(raw: str | None, fallback: str) -> str:
    """Keep only safe SQL/URI identifier characters and strip control/CRLF chars."""
    if not raw:
        return fallback
    cleaned = re.sub(r"[^A-Za-z0-9_.]", "", raw.strip())
    return cleaned or fallback


def _sanitize_ym(raw_ym: str) -> str:
    cleaned = raw_ym.strip()
    if not re.fullmatch(r"\d{4}-\d{2}", cleaned):
        raise ValueError(f"Invalid YYYY-MM value: {raw_ym!r}")
    return cleaned


def build_bronze_schema() -> StructType:
    fields = [StructField(col, StringType(), nullable=True) for col in CITI_BIKE_13_COLS]
    fields.extend(
        [
            StructField("source_month", StringType(), nullable=False),
            StructField("source_file", StringType(), nullable=False),
            StructField("ingested_at", StringType(), nullable=False),
        ]
    )
    return StructType(fields)


def iter_months(start_ym: str, end_ym: str) -> list[str]:
    start_ym = _sanitize_ym(start_ym)
    end_ym = _sanitize_ym(end_ym)
    sy, sm = map(int, start_ym.split("-"))
    ey, em = map(int, end_ym.split("-"))
    if not (1 <= sm <= 12 and 1 <= em <= 12):
        raise ValueError(f"Month out of range in {start_ym} .. {end_ym}")
    if (sy, sm) > (ey, em):
        raise ValueError(f"Start month {start_ym} is after end month {end_ym}")

    months: list[str] = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        months.append(f"{y:04d}-{m:02d}")
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1
    return months


def create_spark_session(app_name: str = "citibike-trips-bronze") -> SparkSession:
    default_tmp = "D:/CitibikeTmp" if os.name == "nt" else "/tmp"
    local_tmp = (os.environ.get("TMP_DIR") or "").strip() or default_tmp
    os.makedirs(local_tmp, exist_ok=True)
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.local.dir", local_tmp)
        .config("spark.sql.iceberg.check-ordering", "false")
        .getOrCreate()
    )


def run_bronze(
    start_month: str,
    end_month: str,
    catalog: str = "lakehouse",
    table: str = "bronze.citibike_trips",
) -> None:
    clean_catalog = _sanitize_identifier(catalog, "lakehouse").split(".")[0] or "lakehouse"
    clean_table = _sanitize_identifier(table, "bronze.citibike_trips")
    namespace = clean_table.split(".", 1)[0] if "." in clean_table else "bronze"
    full_table = f"{clean_catalog}.{clean_table}"

    spark = create_spark_session()
    months = iter_months(start_month, end_month)
    ingested_ts = datetime.now(timezone.utc).isoformat()

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {clean_catalog}.{namespace}")

    for ym in months:
        staging_glob = f"s3a://raw/trip_history/staging/{ym}/*.csv"
        raw_df = (
            spark.read.option("header", "true")
            .option("inferSchema", "false")
            .option("multiLine", "false")
            .csv(staging_glob)
        )

        select_exprs = [
            F.col(c).cast("string").alias(c) if c in raw_df.columns else F.lit(None).cast("string").alias(c)
            for c in CITI_BIKE_13_COLS
        ]
        select_exprs.extend(
            [
                F.lit(ym).cast("string").alias("source_month"),
                F.regexp_extract(F.input_file_name(), r"([^/\\]+)$", 1).cast("string").alias("source_file"),
                F.lit(ingested_ts).cast("string").alias("ingested_at"),
            ]
        )

        bronze_df = raw_df.select(*select_exprs)

        if not spark.catalog.tableExists(full_table):
            (
                bronze_df.writeTo(full_table)
                .using("iceberg")
                .partitionedBy("source_month")
                .create()
            )
        else:
            bronze_df.createOrReplaceTempView("staged_month_bronze")
            spark.sql(
                f"""
                DELETE FROM {full_table}
                WHERE source_month = '{ym}'
                """
            )
            bronze_df.writeTo(full_table).using("iceberg").append()

    spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Citi Bike Bronze Iceberg table from staged CSVs.")
    parser.add_argument("--start-month", default=os.environ.get("TRIP_START_MONTH", "2026-01"))
    parser.add_argument("--end-month", default=os.environ.get("TRIP_END_MONTH", "2026-01"))
    parser.add_argument("--catalog", default=os.environ.get("ICEBERG_CATALOG", "lakehouse"))
    parser.add_argument("--table", default=os.environ.get("ICEBERG_BRONZE_TABLE", "bronze.citibike_trips"))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_bronze(
        start_month=args.start_month,
        end_month=args.end_month,
        catalog=args.catalog,
        table=args.table,
    )