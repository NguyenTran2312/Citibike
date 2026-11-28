"""STR-07 unit + contract tests for the GBFS Spark parsing/validation.

Needs pyspark + Java; run inside the Spark image (see streaming/README.md):
    python3 -m unittest discover -s tests/streaming -v
"""
import json
import os
import tempfile
import unittest
from pathlib import Path

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (ArrayType, BooleanType, DoubleType, IntegerType,
                                   LongType, StringType)
except ImportError:  # pragma: no cover - environments without Spark
    SparkSession = None

REPO = Path(__file__).resolve().parents[2]
FIXTURES = REPO / "streaming" / "fixtures" / "gbfs"
PRODUCER_SCHEMAS = REPO / "ingestion" / "gbfs" / "json_schemas"

if SparkSession is not None:
    from streaming.jobs.gbfs_history import (
        BRONZE_COLUMNS, PARSE_ERROR, SCHEMA_ERROR, VALIDATION_ERROR,
        bronze_rows, invalid_rows, parse_value, valid_rows, validate)
    from streaming.utils.schemas import (
        INVALID_SCHEMA, STATION_INFORMATION_SCHEMA, STATION_STATUS_SCHEMA)


def _fixture_lines(name):
    return (FIXTURES / name).read_text(encoding="utf-8").splitlines()


def _valid_status(**overrides):
    msg = json.loads(_fixture_lines("station_status.jsonl")[0])
    msg.update(overrides)
    return msg


def _valid_information(**overrides):
    msg = json.loads(_fixture_lines("station_information.jsonl")[0])
    msg.update(overrides)
    return msg


@unittest.skipIf(SparkSession is None, "pyspark not installed")
class ContractSchemaTest(unittest.TestCase):
    """The Spark StructTypes are the agreed contract (docs/contracts.md 1.4, 1.5, 2.3.2)."""

    def assertFieldTypes(self, schema, expected):
        actual = {f.name: f.dataType for f in schema.fields}
        self.assertEqual(set(actual), set(expected), "field names differ from the contract")
        for name, dtype in expected.items():
            self.assertEqual(actual[name], dtype, f"wrong type for {name}")
        # from_json cannot enforce NOT NULL; required checks live in validate().
        self.assertTrue(all(f.nullable for f in schema.fields))

    def test_station_status_fields(self):
        self.assertFieldTypes(STATION_STATUS_SCHEMA, {
            "poll_id": StringType(), "last_updated": LongType(), "collected_at": StringType(),
            "station_id": StringType(), "num_bikes_available": IntegerType(),
            "num_docks_available": IntegerType(), "is_installed": BooleanType(),
            "is_renting": BooleanType(), "is_returning": BooleanType(),
            "last_reported": LongType(), "schema_version": IntegerType(),
            "vehicle_types_available": STATION_STATUS_SCHEMA["vehicle_types_available"].dataType,
        })
        vta = STATION_STATUS_SCHEMA["vehicle_types_available"].dataType
        self.assertIsInstance(vta, ArrayType)
        self.assertEqual(vta.elementType["vehicle_type_id"].dataType, StringType())
        self.assertEqual(vta.elementType["count"].dataType, IntegerType())

    def test_station_information_fields(self):
        self.assertFieldTypes(STATION_INFORMATION_SCHEMA, {
            "poll_id": StringType(),
            "last_updated": LongType(), "collected_at": StringType(), "station_id": StringType(),
            "name": StringType(), "lat": DoubleType(), "lon": DoubleType(),
            "short_name": StringType(), "capacity": IntegerType(),
            "is_virtual_station": BooleanType(), "region_id": StringType(),
            "rental_uris": STATION_INFORMATION_SCHEMA["rental_uris"].dataType,
            "is_charging": BooleanType(),
        })
        uris = STATION_INFORMATION_SCHEMA["rental_uris"].dataType
        self.assertEqual(uris.fieldNames(), ["android", "ios", "web"])

    def test_invalid_fields(self):
        self.assertFieldTypes(INVALID_SCHEMA, {
            "source": StringType(), "error_type": StringType(), "error_message": StringType(),
            "raw_payload": StringType(), "detected_at": StringType(),
        })

    def test_matches_producer_json_schemas(self):
        """Same field names as the producer's JSON Schemas (ingestion/gbfs/json_schemas)."""
        pairs = {
            "station_status.schema.json": STATION_STATUS_SCHEMA,
            "station_information.schema.json": STATION_INFORMATION_SCHEMA,
            "invalid.schema.json": INVALID_SCHEMA,
        }
        if not PRODUCER_SCHEMAS.is_dir():
            self.skipTest(f"{PRODUCER_SCHEMAS} not on this branch yet")
        for filename, spark_schema in pairs.items():
            with self.subTest(filename):
                props = json.loads((PRODUCER_SCHEMAS / filename).read_text())["properties"]
                self.assertEqual(set(props), set(spark_schema.fieldNames()))

    def test_types_match_producer_json_schemas(self):
        """Every producer field maps to a compatible Spark type, nested objects included."""
        if not PRODUCER_SCHEMAS.is_dir():
            self.skipTest(f"{PRODUCER_SCHEMAS} not on this branch yet")
        for filename, spark_schema in PRODUCER_SCHEMA_FILES.items():
            with self.subTest(filename):
                doc = json.loads((PRODUCER_SCHEMAS / filename).read_text())
                _assert_struct_matches(self, doc, doc, spark_schema, filename)


# JSON Schema type -> Spark types that from_json maps it to without loss.
JSON_TO_SPARK = {
    "integer": ("IntegerType", "LongType"),
    "number": ("DoubleType",),
    "string": ("StringType",),
    "boolean": ("BooleanType",),
}

PRODUCER_SCHEMA_FILES = {} if SparkSession is None else {
    "station_status.schema.json": STATION_STATUS_SCHEMA,
    "station_information.schema.json": STATION_INFORMATION_SCHEMA,
    "invalid.schema.json": INVALID_SCHEMA,
}


def _resolve(doc, node):
    """Follow a local $ref and drop the `null` branch of `anyOf: [X, null]`."""
    if "$ref" in node:
        name = node["$ref"].rsplit("/", 1)[-1]
        return _resolve(doc, doc["$defs"][name])
    branches = [b for b in node.get("anyOf", []) if b.get("type") != "null"]
    if len(branches) == 1:
        return _resolve(doc, branches[0])
    return node


def _assert_struct_matches(test, doc, node, spark_struct, where):
    props = node["properties"]
    test.assertEqual(set(props), set(spark_struct.fieldNames()), f"{where}: field names")
    for name, prop in props.items():
        _assert_type_matches(test, doc, _resolve(doc, prop),
                             spark_struct[name].dataType, f"{where}.{name}")


def _assert_type_matches(test, doc, node, spark_type, where):
    json_type = node.get("type")
    spark_name = type(spark_type).__name__
    if json_type is None:
        # raw_payload is "any JSON value": Spark keeps its JSON text as a string.
        test.assertEqual(spark_name, "StringType", f"{where}: any-typed field must be a string")
    elif json_type == "array":
        test.assertEqual(spark_name, "ArrayType", where)
        _assert_type_matches(test, doc, _resolve(doc, node["items"]),
                             spark_type.elementType, f"{where}[]")
    elif json_type == "object":
        test.assertEqual(spark_name, "StructType", where)
        _assert_struct_matches(test, doc, node, spark_type, where)
    else:
        test.assertIn(spark_name, JSON_TO_SPARK[json_type],
                      f"{where}: JSON {json_type} read as Spark {spark_name}")


@unittest.skipIf(SparkSession is None, "pyspark not installed")
class ParseValidateTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Empty conf dir: skip spark-defaults.conf (connector downloads, Iceberg catalog).
        cls._conf_dir = tempfile.TemporaryDirectory()
        os.environ["SPARK_CONF_DIR"] = cls._conf_dir.name
        cls.spark = (SparkSession.builder.master("local[1]").appName("str07-tests")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.sql.shuffle.partitions", "1")
                     .config("spark.ui.enabled", "false")
                     .getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls._conf_dir.cleanup()

    def _text_df(self, lines):
        return self.spark.createDataFrame([(line,) for line in lines], "value string")

    def _validate(self, lines, feed, schema):
        return validate(parse_value(self._text_df(lines), schema), feed)

    def _one(self, msg, feed="status", schema=None):
        schema = schema or STATION_STATUS_SCHEMA
        line = msg if isinstance(msg, str) else json.dumps(msg)
        return self._validate([line], feed, schema).collect()[0]

    # --- valid messages are parsed with the right values -------------------
    def test_status_fixture_parsed_and_valid(self):
        rows = self._validate(_fixture_lines("station_status.jsonl"), "status",
                              STATION_STATUS_SCHEMA).orderBy("station_id").collect()
        self.assertEqual([r.dq_is_valid for r in rows], [True, True])
        first, second = rows
        self.assertEqual(first.station_id, "abc")
        self.assertEqual(first.poll_id, "poll-20261116T024000Z")
        self.assertEqual(first.num_bikes_available, 5)
        self.assertEqual(first.num_docks_available, 12)
        self.assertIs(first.is_renting, True)
        self.assertEqual(first.last_reported, 1794796740)
        self.assertEqual(first.collected_at, "2026-11-16T02:40:00Z")
        self.assertIsNone(first.vehicle_types_available)
        self.assertEqual([(v.vehicle_type_id, v["count"]) for v in second.vehicle_types_available],
                         [("1", 2), ("2", 1)])

    def test_information_fixture_parsed_and_valid(self):
        rows = self._validate(_fixture_lines("station_information.jsonl"), "information",
                              STATION_INFORMATION_SCHEMA).orderBy("station_id").collect()
        self.assertEqual([r.dq_is_valid for r in rows], [True, True])
        self.assertEqual(rows[0].poll_id, "station_information-20261116T000000Z")
        self.assertAlmostEqual(rows[0].lat, 40.7128)
        self.assertEqual(rows[0].capacity, 17)
        self.assertIsNone(rows[1].short_name)  # nullable fields may be null
        self.assertIsNone(rows[1].capacity)
        self.assertEqual(rows[0].region_id, "71")
        self.assertIs(rows[0].is_charging, False)
        self.assertEqual(rows[0].rental_uris.ios, "https://bkn.lft.to/lastmile_qr_scan")
        self.assertIsNone(rows[1].rental_uris)

    def test_invalid_fixture_keeps_raw_payload_as_json_text(self):
        rows = self._validate(_fixture_lines("invalid.jsonl"), "invalid", INVALID_SCHEMA).collect()
        self.assertTrue(all(r.dq_is_valid for r in rows))
        # the message's own error_type must survive validation (no column clash)
        payloads = {r.error_type: r.raw_payload for r in rows}
        self.assertEqual(json.loads(payloads["VALIDATION_ERROR"]), {"num_bikes_available": 5})
        self.assertEqual(payloads["PARSE_ERROR"], '{"station_id": "abc", ')

    def test_required_fields_are_enforced_by_spark_rules(self):
        """Nullability contract with the producer's JSON Schema:
        * required field absent or null -> invalid in Spark (VALIDATION_ERROR);
        * optional field absent -> still valid; optional field null -> still valid
          when the JSON Schema allows null (anyOf [..., null]).
        Not asserted: null in an optional field the producer never sends as null
        (schema_version). from_json cannot tell an absent value from null, so the
        any-typed raw_payload is required to be non-null in Spark (gbfs.invalid is
        not streamed; see docs/contracts.md 2.3.2)."""
        if not PRODUCER_SCHEMAS.is_dir():
            self.skipTest(f"{PRODUCER_SCHEMAS} not on this branch yet")
        feeds = {
            "station_status.schema.json": ("status", STATION_STATUS_SCHEMA, _valid_status),
            "station_information.schema.json": ("information", STATION_INFORMATION_SCHEMA,
                                                _valid_information),
            "invalid.schema.json": ("invalid", INVALID_SCHEMA,
                                    lambda: json.loads(_fixture_lines("invalid.jsonl")[0])),
        }
        for filename, (feed, schema, valid) in feeds.items():
            doc = json.loads((PRODUCER_SCHEMAS / filename).read_text())
            required = set(doc.get("required", []))
            self.assertTrue(self._one(valid(), feed, schema).dq_is_valid, f"{filename}: base")
            for name, prop in doc["properties"].items():
                allows_null = any(b.get("type") == "null" for b in prop.get("anyOf", []))
                for how in ("absent", "null"):
                    if how == "null" and name not in required and not allows_null:
                        continue
                    msg = valid()
                    if how == "absent":
                        msg.pop(name, None)
                    else:
                        msg[name] = None
                    with self.subTest(feed=feed, field=name, value=how):
                        row = self._one(msg, feed, schema)
                        if name in required:
                            self.assertFalse(row.dq_is_valid, f"{name} {how} must be invalid")
                            self.assertEqual(row.dq_error_type, VALIDATION_ERROR)
                            self.assertIn(f"missing {name}", row.dq_error_message)
                        else:
                            self.assertTrue(row.dq_is_valid,
                                            f"optional {name} {how} must stay valid: "
                                            f"{row.dq_error_message}")

    def test_schema_version_may_be_absent(self):
        msg = _valid_status()
        del msg["schema_version"]
        self.assertTrue(self._one(msg).dq_is_valid)

    # --- invalid messages: (message, expected error_type, text in error_message)
    def test_status_invalid_cases(self):
        cases = [
            ("not json at all", PARSE_ERROR, "not valid JSON"),
            ('{"station_id": "abc", ', PARSE_ERROR, "not valid JSON"),
            ("", PARSE_ERROR, "empty message"),
            ('[{"station_id": "abc"}]', SCHEMA_ERROR, "does not match schema"),
            (_valid_status(num_bikes_available="five"), SCHEMA_ERROR, "does not match schema"),
            (_valid_status(is_renting="true"), SCHEMA_ERROR, "does not match schema"),
            (_valid_status(station_id=None), VALIDATION_ERROR, "missing station_id"),
            (_valid_status(num_bikes_available=None), VALIDATION_ERROR,
             "missing num_bikes_available"),
            (_valid_status(poll_id=""), VALIDATION_ERROR, "empty poll_id"),
            (_valid_status(num_docks_available=-1), VALIDATION_ERROR,
             "negative num_docks_available"),
            (_valid_status(schema_version=2), VALIDATION_ERROR, "schema_version must be 1"),
            (_valid_status(collected_at="2026-11-16T02:40:00"), VALIDATION_ERROR,
             "collected_at has no timezone"),
            (_valid_status(collected_at="yesterday Z"), VALIDATION_ERROR,
             "collected_at is not an ISO-8601 datetime"),
            (_valid_status(vehicle_types_available=[{"vehicle_type_id": "1", "count": -1}]),
             VALIDATION_ERROR, "invalid vehicle_types_available item"),
        ]
        for msg, error_type, text in cases:
            with self.subTest(msg=msg):
                row = self._one(msg)
                self.assertFalse(row.dq_is_valid)
                self.assertEqual(row.dq_error_type, error_type)
                self.assertIn(text, row.dq_error_message)

    def test_all_errors_are_reported_together(self):
        row = self._one(_valid_status(station_id=None, num_docks_available=-3))
        self.assertEqual(row.dq_error_message, "missing station_id; negative num_docks_available")

    def test_escaped_quotes_in_values_are_accepted(self):
        info = _valid_information(name='Station "A"')
        self.assertTrue(self._one(info, "information", STATION_INFORMATION_SCHEMA).dq_is_valid)

    def test_strings_with_spaces_around_the_colon_are_accepted(self):
        line = json.dumps(_valid_status()).replace('"station_id": "abc"', '"station_id" :  "abc"')
        self.assertTrue(self._one(line).dq_is_valid)

    def test_epoch_in_microseconds_is_rejected(self):
        for field in ("last_reported", "last_updated"):
            with self.subTest(field=field):
                row = self._one(_valid_status(**{field: 1794787200000000}))
                self.assertFalse(row.dq_is_valid)
                self.assertIn(f"{field} is not epoch seconds", row.dq_error_message)
        row = self._one(_valid_information(last_updated=1794787200000), "information",
                        STATION_INFORMATION_SCHEMA)
        self.assertIn("last_updated is not epoch seconds", row.dq_error_message)

    def test_information_invalid_cases(self):
        cases = [
            (_valid_information(name=None), "missing name"),
            (_valid_information(poll_id=None), "missing poll_id"),
            (_valid_information(poll_id=""), "empty poll_id"),
            (_valid_information(lat=91.0), "lat out of range"),
            (_valid_information(lon=-181.0), "lon out of range"),
            (_valid_information(capacity=-1), "negative capacity"),
        ]
        for msg, text in cases:
            with self.subTest(msg=msg):
                row = self._one(msg, "information", STATION_INFORMATION_SCHEMA)
                self.assertEqual(row.dq_error_type, VALIDATION_ERROR)
                self.assertIn(text, row.dq_error_message)

    def test_bad_fixture_is_all_invalid(self):
        rows = self._validate(_fixture_lines("station_status_bad.jsonl"), "status",
                              STATION_STATUS_SCHEMA).collect()
        self.assertEqual(len(rows), 10)
        self.assertFalse(any(r.dq_is_valid for r in rows))

    # --- outputs ------------------------------------------------------------
    def test_valid_and_invalid_split(self):
        lines = _fixture_lines("station_status.jsonl") + _fixture_lines("station_status_bad.jsonl")
        validated = validate(parse_value(self._text_df(lines), STATION_STATUS_SCHEMA), "status")

        valid = valid_rows(validated, "status")
        self.assertEqual(valid.columns, STATION_STATUS_SCHEMA.fieldNames())
        self.assertEqual(valid.count(), 2)

        invalid = invalid_rows(validated, "status")
        self.assertEqual(invalid.columns, INVALID_SCHEMA.fieldNames())  # gbfs.invalid contract
        self.assertEqual(invalid.count(), 10)
        sample = invalid.first()
        self.assertEqual(sample.source, "gbfs.station_status")
        self.assertRegex(sample.detected_at, r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
        self.assertIsNotNone(sample.raw_payload)

    def test_kafka_shaped_input_keeps_metadata(self):
        kafka_df = self.spark.createDataFrame(
            [(bytearray(_fixture_lines("station_status.jsonl")[0], "utf-8"),
              "gbfs.station_status", 2, 41)],
            "value binary, topic string, partition int, offset long",
        ).withColumn("timestamp", F.current_timestamp())
        validated = validate(parse_value(kafka_df, STATION_STATUS_SCHEMA), "status")
        row = valid_rows(validated, "status").collect()[0]
        self.assertEqual((row.kafka_topic, row.kafka_partition, row.kafka_offset),
                         ("gbfs.station_status", 2, 41))
        self.assertEqual(row.station_id, "abc")

    # --- Bronze (STR-08) ------------------------------------------------------
    def _kafka_df(self, values, topic="gbfs.station_status"):
        rows = [(bytearray(v, "utf-8") if v is not None else None, key, topic, 0, offset)
                for offset, (key, v) in enumerate(values)]
        return self.spark.createDataFrame(
            rows, "value binary, key string, topic string, partition int, offset long"
        ).withColumn("key", F.col("key").cast("binary")) \
         .withColumn("timestamp", F.current_timestamp())

    def _bronze(self, values, feed="status", schema=None):
        schema = schema or STATION_STATUS_SCHEMA
        df = self._kafka_df(values)
        return bronze_rows(validate(parse_value(df, schema), feed))

    def test_bronze_keeps_every_message_as_received(self):
        valid = json.dumps(_valid_status())
        broken = json.dumps(_valid_status(num_docks_available=-1))
        values = [("abc", valid), ("abc", "not json"), ("abc", broken), (None, None)]

        rows = self._bronze(values).orderBy("kafka_offset").collect()

        self.assertEqual(len(rows), 4)   # nothing is dropped
        self.assertEqual([r.raw_value for r in rows], [valid, "not json", broken, None])
        self.assertEqual([r.dq_is_valid for r in rows], [True, False, False, False])
        self.assertEqual([r.dq_error_type for r in rows],
                         [None, PARSE_ERROR, VALIDATION_ERROR, PARSE_ERROR])
        self.assertEqual([r.kafka_offset for r in rows], [0, 1, 2, 3])
        self.assertEqual([r.kafka_key for r in rows], ["abc", "abc", "abc", None])
        self.assertTrue(all(r.ingested_at is not None for r in rows))

    def test_bronze_copies_identity_fields_out_of_the_message(self):
        row = self._bronze([("abc", json.dumps(_valid_status()))]).collect()[0]

        self.assertEqual(row.poll_id, "poll-20261116T024000Z")
        self.assertEqual(row.station_id, "abc")
        self.assertEqual(row.collected_at.isoformat(), "2026-11-16T02:40:00")  # UTC session
        self.assertEqual(row.kafka_topic, "gbfs.station_status")

        unparsed = self._bronze([("abc", "not json")]).collect()[0]
        self.assertIsNone(unparsed.station_id)
        self.assertIsNone(unparsed.collected_at)

    def test_bronze_information_rows(self):
        msg = _valid_information()
        df = self._kafka_df([(msg["station_id"], json.dumps(msg))],
                            topic="gbfs.station_information")
        row = bronze_rows(validate(parse_value(df, STATION_INFORMATION_SCHEMA),
                                   "information")).collect()[0]
        self.assertTrue(row.dq_is_valid)
        self.assertEqual(row.poll_id, msg["poll_id"])
        self.assertEqual(row.kafka_topic, "gbfs.station_information")

    def test_bronze_columns_and_types_match_the_table(self):
        schema = self._bronze([("abc", json.dumps(_valid_status()))]).schema
        self.assertEqual(schema.fieldNames(), BRONZE_COLUMNS)
        types = {f.name: f.dataType.simpleString() for f in schema.fields}
        self.assertEqual(types, {
            "kafka_topic": "string", "kafka_partition": "int", "kafka_offset": "bigint",
            "kafka_timestamp": "timestamp", "kafka_key": "string", "raw_value": "string",
            "poll_id": "string", "station_id": "string", "collected_at": "timestamp",
            "dq_is_valid": "boolean", "dq_error_type": "string",
            "dq_error_message": "string", "ingested_at": "timestamp",
        })

    def test_bronze_from_text_input_has_typed_null_kafka_columns(self):
        rows = bronze_rows(self._validate(_fixture_lines("station_status.jsonl"), "status",
                                          STATION_STATUS_SCHEMA))
        self.assertEqual(rows.schema["kafka_offset"].dataType.simpleString(), "bigint")
        self.assertTrue(all(r.kafka_offset is None for r in rows.collect()))


if __name__ == "__main__":
    unittest.main()
