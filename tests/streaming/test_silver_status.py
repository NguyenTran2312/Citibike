"""Unit tests for Silver station_status (STR-09) and its data-quality checks.

Runs on a local SparkSession without Iceberg (see tests/README.md for the command).
"""
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
    from streaming.jobs.silver_status import SILVER_STATUS_COLUMNS, silver_status_rows
    from streaming.utils.schemas import STATION_STATUS_SCHEMA


def status(**overrides):
    line = (FIXTURES / "station_status.jsonl").read_text(encoding="utf-8").splitlines()[0]
    msg = json.loads(line)
    msg.update(overrides)
    return msg


@unittest.skipIf(SparkSession is None, "pyspark not installed")
class SilverStatusTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._conf_dir = tempfile.TemporaryDirectory()
        os.environ["SPARK_CONF_DIR"] = cls._conf_dir.name
        cls.spark = (SparkSession.builder.master("local[1]").appName("str09-tests")
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
        """Kafka messages (dict, str or None) -> Bronze rows, offsets 0..n-1 on partition 0."""
        rows = []
        for offset, msg in enumerate(messages):
            value = msg if isinstance(msg, (str, type(None))) else json.dumps(msg)
            rows.append((bytearray(value, "utf-8") if value is not None else None,
                         "gbfs.station_status", 0, offset))
        kafka = (self.spark.createDataFrame(
            rows, "value binary, topic string, partition int, offset long")
            .withColumn("timestamp", F.current_timestamp())
            .withColumn("key", F.lit("k").cast("binary")))
        return bronze_rows(validate(parse_value(kafka, STATION_STATUS_SCHEMA), "status"))

    def silver(self, messages):
        return silver_status_rows(self.bronze(messages)).orderBy("poll_id", "station_id")

    # --- validation ------------------------------------------------------------
    def test_only_valid_messages_reach_silver(self):
        rows = self.silver([status(), "not json", status(num_docks_available=-1),
                            status(station_id=None), None]).collect()
        self.assertEqual([(r.station_id, r.poll_id) for r in rows],
                         [("abc", "poll-20261116T024000Z")])

    def test_silver_revalidates_raw_value_instead_of_trusting_bronze_flags(self):
        bronze = self.bronze([status(num_bikes_available=-5)]) \
            .withColumn("dq_is_valid", F.lit(True))   # pretend Bronze said valid
        self.assertEqual(silver_status_rows(bronze).count(), 0)

    # --- grain and dedup ---------------------------------------------------------
    def test_replayed_message_for_the_same_key_becomes_one_row(self):
        rows = self.silver([status(), status(), status()]).collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].kafka_offset, 0)   # lowest offset wins

    def test_unchanged_station_in_two_polls_is_two_observations(self):
        rows = self.silver([status(poll_id="poll-1"), status(poll_id="poll-2")]).collect()
        self.assertEqual([r.poll_id for r in rows], ["poll-1", "poll-2"])
        self.assertEqual({r.num_bikes_available for r in rows}, {5})

    def test_different_stations_in_one_poll_are_separate_rows(self):
        rows = self.silver([status(station_id="abc"), status(station_id="def")]).collect()
        self.assertEqual([r.station_id for r in rows], ["abc", "def"])

    # --- types ---------------------------------------------------------------------
    def test_timestamps_are_converted(self):
        row = self.silver([status()]).collect()[0]
        msg = status()
        self.assertEqual(row.collected_at, datetime(2026, 11, 16, 2, 40, 0))
        self.assertEqual(row.last_reported, datetime.utcfromtimestamp(msg["last_reported"]))
        self.assertEqual(row.last_updated, datetime.utcfromtimestamp(msg["last_updated"]))
        self.assertIsNotNone(row.bronze_ingested_at)
        self.assertIsNotNone(row.silver_processed_at)

    def test_vehicle_types_are_kept_as_structs(self):
        vehicles = [{"vehicle_type_id": "1", "count": 2}, {"vehicle_type_id": "2", "count": 1}]
        row = self.silver([status(vehicle_types_available=vehicles)]).collect()[0]
        self.assertEqual([(v.vehicle_type_id, v["count"]) for v in row.vehicle_types_available],
                         [("1", 2), ("2", 1)])

    def test_columns_and_types_match_the_table(self):
        schema = self.silver([status()]).schema
        self.assertEqual(schema.fieldNames(), SILVER_STATUS_COLUMNS)
        types = {f.name: f.dataType.simpleString() for f in schema.fields}
        self.assertEqual(types, {
            "station_id": "string", "poll_id": "string", "collected_at": "timestamp",
            "last_updated": "timestamp", "last_reported": "timestamp",
            "num_bikes_available": "int", "num_docks_available": "int",
            "is_installed": "boolean", "is_renting": "boolean", "is_returning": "boolean",
            "vehicle_types_available": "array<struct<vehicle_type_id:string,count:int>>",
            "schema_version": "int", "kafka_partition": "int", "kafka_offset": "bigint",
            "bronze_ingested_at": "timestamp", "silver_processed_at": "timestamp",
        })

    # --- data-quality checks ----------------------------------------------------------
    def run_dq(self, bronze, silver):
        bronze.createOrReplaceTempView("dq_bronze")
        silver.createOrReplaceTempView("dq_silver")
        with _quiet():
            return dq_checks.run(self.spark, "silver_status", "dq_bronze", "dq_silver")

    def test_dq_passes_on_clean_silver(self):
        bronze = self.bronze([status(), status(), status(poll_id="p2"), "not json"])
        self.assertTrue(self.run_dq(bronze, silver_status_rows(bronze)))

    def test_dq_fails_on_duplicate_grain(self):
        bronze = self.bronze([status()])
        silver = silver_status_rows(bronze)
        self.assertFalse(self.run_dq(bronze, silver.unionByName(silver)))

    def test_dq_fails_when_a_valid_bronze_key_is_missing(self):
        bronze = self.bronze([status(), status(poll_id="p2")])
        self.assertFalse(self.run_dq(bronze, silver_status_rows(bronze).filter("poll_id = 'p2'")))

    def test_dq_reconciles_with_current_rules_not_stored_bronze_flags(self):
        # Bronze flagged the message invalid when it was ingested; today's rules
        # (the ones Silver applies) accept it, so it is a valid key for the DQ too.
        bronze = self.bronze([status()]).withColumn("dq_is_valid", F.lit(False))
        silver = silver_status_rows(bronze)
        self.assertEqual(silver.count(), 1)
        self.assertTrue(self.run_dq(bronze, silver))

    def test_epoch_overflow_stays_out_of_silver(self):
        rows = self.silver([status(last_reported=1794787200000000), status(poll_id="p2")])
        self.assertEqual([r.poll_id for r in rows.collect()], ["p2"])

    def test_bronze_dq_fails_on_a_duplicated_message(self):
        bronze = self.bronze([status()])
        bronze.unionByName(bronze).createOrReplaceTempView("dq_bronze")
        with _quiet():
            self.assertFalse(dq_checks.run(self.spark, "bronze_status", "dq_bronze", "dq_silver"))
        bronze.createOrReplaceTempView("dq_bronze")
        with _quiet():
            self.assertTrue(dq_checks.run(self.spark, "bronze_status", "dq_bronze", "dq_silver"))

    def test_dq_fails_on_rows_bronze_did_not_accept(self):
        bronze = self.bronze([status()])
        extra = silver_status_rows(self.bronze([status(poll_id="never-in-bronze")]))
        self.assertFalse(self.run_dq(bronze, silver_status_rows(bronze).unionByName(extra)))


class _quiet:
    """Silence the PASS/FAIL lines printed by dq_checks.run during tests."""

    def __enter__(self):
        import contextlib
        import io
        self._redirect = contextlib.redirect_stdout(io.StringIO())
        self._redirect.__enter__()

    def __exit__(self, *exc):
        return self._redirect.__exit__(*exc)


if __name__ == "__main__":
    unittest.main()
