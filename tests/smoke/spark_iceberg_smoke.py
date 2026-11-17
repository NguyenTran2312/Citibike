"""Smoke test Spark <-> MinIO (S3A) and Spark <-> Iceberg REST (S3FileIO).

Run inside the Spark image (see tests/smoke/infra_smoke.sh):
    spark-submit --master spark://spark-master:7077 \
        /opt/project/tests/smoke/spark_iceberg_smoke.py <run_id> <rows>

Writes <rows> rows tagged with <run_id> to lakehouse.smoke.e2e so Trino can
verify them afterwards. Prints "SMOKE_OK" on success.
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

run_id, rows = sys.argv[1], int(sys.argv[2])
spark = SparkSession.builder.appName(f"infra-smoke-{run_id}").getOrCreate()

# 1. Spark <-> MinIO through Hadoop S3A (the path used by streaming checkpoints).
s3a_path = f"s3a://checkpoints/_smoke/{run_id}"
spark.range(rows).write.mode("overwrite").parquet(s3a_path)
s3a_count = spark.read.parquet(s3a_path).count()
assert s3a_count == rows, f"S3A round trip: expected {rows}, got {s3a_count}"
print(f"[smoke] S3A write/read OK: {s3a_count} rows at {s3a_path}")

# 2. Spark <-> Iceberg REST catalog, data files on MinIO through S3FileIO.
spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.smoke")
spark.sql(
    "CREATE TABLE IF NOT EXISTS lakehouse.smoke.e2e "
    "(run_id STRING, id BIGINT, written_at TIMESTAMP) USING iceberg"
)
(
    spark.range(rows)
    .select(F.lit(run_id).alias("run_id"), F.col("id"), F.current_timestamp().alias("written_at"))
    .writeTo("lakehouse.smoke.e2e")
    .append()
)
ice_count = spark.table("lakehouse.smoke.e2e").where(F.col("run_id") == run_id).count()
assert ice_count == rows, f"Iceberg: expected {rows}, got {ice_count}"
location = spark.sql("DESCRIBE TABLE EXTENDED lakehouse.smoke.e2e").where("col_name = 'Location'").first()
print(f"[smoke] Iceberg append OK: {ice_count} rows, location {location.data_type if location else '?'}")

# The S3A scratch data is not needed after the check.
jvm = spark._jvm
hpath = jvm.org.apache.hadoop.fs.Path(s3a_path)
hpath.getFileSystem(spark._jsc.hadoopConfiguration()).delete(hpath, True)

spark.stop()
print("SMOKE_OK")
