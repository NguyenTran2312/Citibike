"""spark-submit helper for tests/integration/test_spark_recovery.py.

    str12_helper.py stats <bronze_table> <silver_table>   -> STR12_RESULT {...}
    str12_helper.py batches <checkpoint_dir>               -> STR12_RESULT {"offsets": [...], "commits": [...]}
    str12_helper.py rm <path>                              -> STR12_RESULT {"deleted": bool}
    str12_helper.py expire <table>                         -> expire all but the current snapshot
    str12_helper.py drop <table> [<table> ...]             -> DROP TABLE ... PURGE
"""
import json
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def emit(result) -> None:
    print("STR12_RESULT " + json.dumps(result, sort_keys=True), flush=True)


def snapshots(spark: SparkSession, table: str) -> int:
    # The REST catalog fixture rejects <table>.snapshots, so ask Iceberg directly.
    return spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, table).history().size()


def table_stats(spark: SparkSession, table: str, bronze: bool):
    if not spark.catalog.tableExists(table):
        return None
    df = spark.table(table)
    result = {"rows": df.count(), "snapshots": snapshots(spark, table)}
    if bronze:
        result["per_partition"] = {
            str(r.kafka_partition): r.n
            for r in df.groupBy("kafka_partition").agg(F.count(F.lit(1)).alias("n")).collect()}
        result["duplicate_offsets"] = (df.groupBy("kafka_topic", "kafka_partition", "kafka_offset")
                                       .count().filter("count > 1").count())
    else:
        result["distinct_keys"] = df.select("station_id", "poll_id").distinct().count()
    return result


def list_batches(spark: SparkSession, directory: str, sub: str):
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(f"{directory.rstrip('/')}/{sub}")
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if not fs.exists(path):
        return []
    names = [status.getPath().getName() for status in fs.listStatus(path)]
    return sorted(int(n) for n in names if n.isdigit())


def main() -> None:
    action, *args = sys.argv[1:]
    spark = SparkSession.builder.appName(f"str12-{action}").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    if action == "stats":
        emit({"bronze": table_stats(spark, args[0], bronze=True),
              "silver": table_stats(spark, args[1], bronze=False)})
    elif action == "batches":
        emit({"offsets": list_batches(spark, args[0], "offsets"),
              "commits": list_batches(spark, args[0], "commits")})
    elif action == "rm":
        path = spark._jvm.org.apache.hadoop.fs.Path(args[0])
        emit({"deleted": path.getFileSystem(spark._jsc.hadoopConfiguration()).delete(path, True)})
    elif action == "expire":
        catalog, name = args[0].split(".", 1)
        # CALL takes literals only; the timestamp is read in the same session time zone.
        cutoff = spark.sql("SELECT date_format(current_timestamp() + INTERVAL 1 MINUTE, "
                           "'yyyy-MM-dd HH:mm:ss')").first()[0]
        row = spark.sql(f"CALL {catalog}.system.expire_snapshots(table => '{name}', "
                        f"older_than => TIMESTAMP '{cutoff}', retain_last => 1)").first()
        emit({"deleted_manifest_lists": row["deleted_manifest_lists_count"],
              "snapshots": snapshots(spark, args[0])})
    elif action == "drop":
        for table in args:
            spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    else:
        raise SystemExit(f"unknown action {action}")


if __name__ == "__main__":
    main()
