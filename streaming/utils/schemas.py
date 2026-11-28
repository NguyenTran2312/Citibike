"""Spark StructType schemas for the GBFS Kafka topics (STR-07).

Each schema describes the JSON message in the Kafka `value`, and is meant for
    F.from_json(F.col("value").cast("string"), SCHEMA)

Conventions (see docs/contracts.md, sections 1.4, 1.5 and 2.3.2):
  - Unix timestamps (last_updated, last_reported) -> LongType (epoch seconds).
  - ISO-8601 datetimes (collected_at, detected_at) stay StringType at parse time
    so the raw value is kept; convert later with F.to_timestamp(...).
  - Every field is nullable=True: from_json never enforces NOT NULL, so required
    fields (station_id, name, lat, lon, ...) must be checked by validation logic.
"""
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# Topic names (docs/contracts.md section 2.3.1)
TOPIC_STATION_STATUS = "gbfs.station_status"
TOPIC_STATION_INFORMATION = "gbfs.station_information"
TOPIC_INVALID = "gbfs.invalid"

# vehicle_types_available element (GBFS v2.3): {"vehicle_type_id": "...", "count": n}
VEHICLE_TYPE_AVAILABLE_SCHEMA = StructType([
    StructField("vehicle_type_id", StringType(), True),
    StructField("count", IntegerType(), True),
])

# ---------------------------------------------------------------------------
# gbfs.station_status  -- grain: (station_id, poll_id)
# ---------------------------------------------------------------------------
STATION_STATUS_SCHEMA = StructType([
    StructField("poll_id", StringType(), True),                 # ID of the poll run
    StructField("last_updated", LongType(), True),              # feed update time, epoch seconds
    StructField("collected_at", StringType(), True),            # ISO-8601 datetime, e.g. 2026-11-16T02:40:00Z
    StructField("station_id", StringType(), True),
    StructField("num_bikes_available", IntegerType(), True),
    StructField("num_docks_available", IntegerType(), True),
    StructField("is_installed", BooleanType(), True),
    StructField("is_renting", BooleanType(), True),
    StructField("is_returning", BooleanType(), True),
    StructField("last_reported", LongType(), True),             # station report time, epoch seconds
    StructField("schema_version", IntegerType(), True),         # always 1
    StructField("vehicle_types_available",                      # array or null
                ArrayType(VEHICLE_TYPE_AVAILABLE_SCHEMA, containsNull=True), True),
])

# ---------------------------------------------------------------------------
# gbfs.station_information  -- polled every 24h, own poll_id space
# ---------------------------------------------------------------------------
# rental_uris (GBFS v2.3): deep links to rent at this station
RENTAL_URIS_SCHEMA = StructType([
    StructField("android", StringType(), True),
    StructField("ios", StringType(), True),
    StructField("web", StringType(), True),
])

STATION_INFORMATION_SCHEMA = StructType([
    StructField("poll_id", StringType(), True),                 # required; station_information-<uuid4>
    StructField("last_updated", LongType(), True),              # feed update time, epoch seconds
    StructField("collected_at", StringType(), True),            # ISO-8601 datetime
    StructField("station_id", StringType(), True),              # required
    StructField("name", StringType(), True),                    # required
    StructField("lat", DoubleType(), True),                     # required
    StructField("lon", DoubleType(), True),                     # required
    StructField("short_name", StringType(), True),              # nullable
    StructField("capacity", IntegerType(), True),               # nullable
    StructField("is_virtual_station", BooleanType(), True),     # nullable
    StructField("region_id", StringType(), True),               # nullable (source field)
    StructField("rental_uris", RENTAL_URIS_SCHEMA, True),       # nullable (source field)
    StructField("is_charging", BooleanType(), True),            # nullable (source field)
])

# ---------------------------------------------------------------------------
# gbfs.invalid  -- messages that failed parse/validation
# ---------------------------------------------------------------------------
INVALID_SCHEMA = StructType([
    StructField("source", StringType(), True),                  # e.g. gbfs.station_status
    StructField("error_type", StringType(), True),              # e.g. VALIDATION_ERROR
    StructField("error_message", StringType(), True),
    # raw_payload is "Any" (object, array, string, number...). With StringType,
    # from_json keeps the original JSON text of whatever value is there, e.g.
    # {"num_bikes_available": 5} -> '{"num_bikes_available":5}'.
    StructField("raw_payload", StringType(), True),
    StructField("detected_at", StringType(), True),             # ISO-8601 with timezone, e.g. 2026-11-16T08:00:00Z
])

# Topic -> schema, so a job can pick the schema from the topic it subscribes to.
SCHEMAS_BY_TOPIC = {
    TOPIC_STATION_STATUS: STATION_STATUS_SCHEMA,
    TOPIC_STATION_INFORMATION: STATION_INFORMATION_SCHEMA,
    TOPIC_INVALID: INVALID_SCHEMA,
}
