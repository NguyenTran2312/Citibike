"""Helper for tests/integration/str09_silver.sh and str10_silver_information.sh.

    str09_silver_check.py <silver_table> stats              -> STR09_RESULT {...}
    str09_silver_check.py <silver_table> inject-duplicate   -> re-insert one existing row
    str09_silver_check.py <table> [<table> ...] drop        -> DROP TABLE ... PURGE
    str09_silver_check.py <path> rm                         -> delete a checkpoint dir (s3a://...)
"""
import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    *tables, action = sys.argv[1:]
    spark = SparkSession.builder.appName("str09-silver-check").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    if action == "rm":
        # The Iceberg streaming source writes its offsets with the table FileIO, so
        # the Silver checkpoint lives on S3 (MinIO); delete it with Hadoop FS.
        jvm = spark._jvm
        path = jvm.org.apache.hadoop.fs.Path(tables[0])
        path.getFileSystem(spark._jsc.hadoopConfiguration()).delete(path, True)
        return

    if action == "drop":
        for table in tables:   # a Silver table's poll table goes with it
            for name in (table, f"{table}_polls"):
                spark.sql(f"DROP TABLE IF EXISTS {name} PURGE")
        return

    table = tables[0]
    if action == "inject-duplicate":
        spark.sql(f"INSERT INTO {table} SELECT * FROM {table} LIMIT 1")
        return

    df = spark.table(table)
    rows = df.orderBy("poll_id", "station_id").collect()
    history = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, table).history()
    print("STR09_RESULT " + json.dumps({
        "rows": len(rows),
        "keys": sorted(f"{r.station_id}/{r.poll_id}" for r in rows),
        "distinct_keys": df.select("station_id", "poll_id").distinct().count(),
        "null_collected_at": df.filter(F.col("collected_at").isNull()).count(),
        "non_null": {c: df.filter(F.col(c).isNotNull()).count() for c in df.columns},
        "snapshots": history.size(),
    }, sort_keys=True))


if __name__ == "__main__":
    main()
