"""Unit tests for Silver station information (STR-10) and its data-quality checks.

Runs on a local SparkSession without Iceberg (see tests/README.md for the command).
"""
import contextlib
import io
import json
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
except ImportError:  # pragma: no cover - pyspark is only in the Spark image
    SparkSession = None

FIXTURES = Path(__file__).resolve().parents[2] / "streaming" / "fixtures" / "gbfs"

if SparkSession is not None:
    from streaming.jobs import dq_checks
    from streaming.jobs.gbfs_history import bronze_rows, parse_value, validate
    from streaming.jobs.silver_information import (SILVER_INFORMATION_COLUMNS,
                                                   silver_information_rows)
    from streaming.utils.schemas import STATION_INFORMATION_SCHEMA


def information(line=0, **overrides):
    text = (FIXTURES / "station_information.jsonl").read_text(encoding="utf-8").splitlines()
    msg = json.loads(text[line])
    msg.update(overrides)
    return msg


@unittest.skipIf(SparkSession is None, "pyspark not installed")
class SilverInformationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._conf_dir = tempfile.TemporaryDirectory()
        os.environ["SPARK_CONF_DIR"] = cls._conf_dir.name
        cls.spark = (SparkSession.builder.master("local[1]").appName("str10-tests")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.sql.shuffle.partitions", "1")
                     .config("spark.ui.enabled", "false")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls._conf_dir.cleanup()

    def bronze(self, messages):
        rows = []
        for offset, msg in enumerate(messages):
            value = msg if isinstance(msg, (str, type(None))) else json.dumps(msg)
            rows.append((bytearray(value, "utf-8") if value is not None else None,
                         "gbfs.station_information", 0, offset))
        kafka = (self.spark.createDataFrame(
            rows, "value binary, topic string, partition int, offset long")
            .withColumn("timestamp", F.current_timestamp())
            .withColumn("key", F.lit("k").cast("binary")))
        return bronze_rows(validate(parse_value(kafka, STATION_INFORMATION_SCHEMA),
                                    "information"))

    def silver(self, messages):
        return silver_information_rows(self.bronze(messages)).orderBy("poll_id", "station_id")

    # --- every source field is kept ------------------------------------------------
    def test_every_source_field_is_kept(self):
        row = self.silver([information(0)]).collect()[0]
        self.assertEqual((row.station_id, row.name, row.short_name),
                         ("abc", "Sample Station A", "5000.01"))
        self.assertAlmostEqual(row.lat, 40.7128)
        self.assertEqual(row.capacity, 17)
        self.assertEqual(row.region_id, "71")
        self.assertIs(row.is_charging, False)
        self.assertEqual(row.rental_uris.android, "https://bkn.lft.to/lastmile_qr_scan")
        self.assertIsNone(row.rental_uris.web)
        self.assertEqual(row.collected_at, datetime(2026, 11, 16, 0, 0, 0))
        self.assertEqual(row.last_updated, datetime.utcfromtimestamp(information()["last_updated"]))

    def test_nullable_source_fields_stay_null(self):
        row = self.silver([information(1)]).collect()[0]
        self.assertIsNone(row.region_id)
        self.assertIsNone(row.rental_uris)
        self.assertIsNone(row.is_charging)
        self.assertIsNone(row.capacity)

    # --- snapshots, dedup, validation ---------------------------------------------------
    def test_each_poll_is_a_snapshot_even_when_nothing_changed(self):
        rows = self.silver([information(0, poll_id="info-1"),
                            information(0, poll_id="info-2")]).collect()
        self.assertEqual([r.poll_id for r in rows], ["info-1", "info-2"])
        self.assertEqual({r.name for r in rows}, {"Sample Station A"})

    def test_a_renamed_station_keeps_both_versions(self):
        rows = self.silver([information(0, poll_id="info-1"),
                            information(0, poll_id="info-2", name="Renamed A")]).collect()
        self.assertEqual([(r.poll_id, r.name) for r in rows],
                         [("info-1", "Sample Station A"), ("info-2", "Renamed A")])

    def test_replayed_message_becomes_one_row(self):
        rows = self.silver([information(0), information(0), information(1)]).collect()
        self.assertEqual([r.station_id for r in rows], ["abc", "def"])
        self.assertEqual(rows[0].kafka_offset, 0)

    def test_invalid_messages_stay_out(self):
        rows = self.silver([information(0), information(0, lat=95.0, station_id="x"),
                            information(0, poll_id=None, station_id="y"),
                            information(0, is_charging="no", station_id="z"),
                            "not json"]).collect()
        self.assertEqual([r.station_id for r in rows], ["abc"])

    def test_columns_and_types_match_the_table(self):
        schema = self.silver([information(0)]).schema
        self.assertEqual(schema.fieldNames(), SILVER_INFORMATION_COLUMNS)
        types = {f.name: f.dataType.simpleString() for f in schema.fields}
        self.assertEqual(types, {
            "station_id": "string", "poll_id": "string", "collected_at": "timestamp",
            "last_updated": "timestamp", "name": "string", "short_name": "string",
            "lat": "double", "lon": "double", "capacity": "int", "region_id": "string",
            "is_virtual_station": "boolean", "is_charging": "boolean",
            "rental_uris": "struct<android:string,ios:string,web:string>",
            "kafka_partition": "int", "kafka_offset": "bigint",
            "bronze_ingested_at": "timestamp", "silver_processed_at": "timestamp",
        })

    # --- data-quality checks ----------------------------------------------------------
    def run_dq(self, bronze, silver):
        bronze.createOrReplaceTempView("dq_info_bronze")
        silver.createOrReplaceTempView("dq_info_silver")
        with contextlib.redirect_stdout(io.StringIO()):
            return dq_checks.run(self.spark, "silver_information",
                                 "dq_info_bronze", "dq_info_silver")

    def test_dq_passes_on_clean_silver(self):
        bronze = self.bronze([information(0), information(1), information(0), "not json"])
        self.assertTrue(self.run_dq(bronze, silver_information_rows(bronze)))

    def test_dq_fails_on_duplicate_grain(self):
        bronze = self.bronze([information(0)])
        silver = silver_information_rows(bronze)
        self.assertFalse(self.run_dq(bronze, silver.unionByName(silver)))

    def test_dq_fails_when_a_snapshot_row_is_missing(self):
        bronze = self.bronze([information(0), information(1)])
        silver = silver_information_rows(bronze).filter("station_id = 'abc'")
        self.assertFalse(self.run_dq(bronze, silver))

    def test_dq_fails_on_out_of_range_coordinates(self):
        bronze = self.bronze([information(0)])
        silver = silver_information_rows(bronze).withColumn("lat", F.lit(99.0))
        self.assertFalse(self.run_dq(bronze, silver))


if __name__ == "__main__":
    unittest.main()
