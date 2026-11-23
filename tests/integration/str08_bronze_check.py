"""Đọc một bảng Bronze và in số liệu đối soát (một dòng JSON, tiền tố STR08_RESULT).

    spark-submit tests/integration/str08_bronze_check.py <table> [--drop]

Dùng cho tests/integration/str08_bronze.sh; cũng chạy được bằng tay để đối soát
Bronze với end offset của Kafka.
"""
import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def snapshot_count(spark: SparkSession, table: str) -> int:
    # The REST catalog fixture rejects metadata-table names (<table>.snapshots),
    # so ask the Iceberg table object directly.
    iceberg_table = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, table)
    return iceberg_table.history().size()


def main() -> None:
    table = sys.argv[1]
    spark = SparkSession.builder.appName("str08-bronze-check").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    if "--drop" in sys.argv:
        spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
        print("STR08_RESULT " + json.dumps({"dropped": table}))
        return

    df = spark.table(table)
    per_partition = {
        f"{r.kafka_topic}:{r.kafka_partition}": {"rows": r.rows, "max_offset": r.max_offset}
        for r in df.groupBy("kafka_topic", "kafka_partition")
        .agg(F.count(F.lit(1)).alias("rows"), F.max("kafka_offset").alias("max_offset"))
        .collect()
    }
    per_poll = (
        df.filter("dq_is_valid")
        .groupBy("poll_id")
        .agg(F.countDistinct("station_id").alias("stations"), F.count(F.lit(1)).alias("rows"))
        .collect()
    )
    result = {
        "rows": df.count(),
        "valid": df.filter("dq_is_valid").count(),
        "per_partition": per_partition,
        "duplicate_offsets": df.groupBy("kafka_topic", "kafka_partition", "kafka_offset")
        .count().filter("count > 1").count(),
        "null_raw_value": df.filter("raw_value IS NULL").count(),
        "polls": {r.poll_id: {"stations": r.stations, "rows": r.rows} for r in per_poll},
        "snapshots": snapshot_count(spark, table),
    }
    print("STR08_RESULT " + json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
