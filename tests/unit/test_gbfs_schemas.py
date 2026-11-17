import unittest

from pydantic import ValidationError

from ingestion.gbfs.schemas import (
    InvalidMessage,
    StationInformation,
    StationStatus,
    StationStatusMessage,
)


class TestGBFSSchemas(unittest.TestCase):
    def setUp(self):
        # Dữ liệu giả lập cố định, không phụ thuộc API.
        self.status = {
            "station_id": "station-test-01",
            "num_bikes_available": 5,
            "num_docks_available": 10,
            "is_installed": 1,
            "is_renting": 1,
            "is_returning": 0,
            "last_reported": 1794886000,
            "vehicle_types_available": [
                {"vehicle_type_id": "1", "count": 5}
            ],
        }

        self.information = {
            "station_id": "station-test-01",
            "name": "Test Station",
            "lat": 40.75,
            "lon": -73.98,
        }

        self.metadata = {
            "schema_version": 1,
            "poll_id": "status-test-poll-01",
            "last_updated": 1794886078,
            "collected_at": "2026-11-17T03:00:00Z",
        }

    def test_valid_status_normalizes_flags(self):
        result = StationStatus.model_validate(self.status)

        self.assertIs(result.is_installed, True)
        self.assertIs(result.is_renting, True)
        self.assertIs(result.is_returning, False)

    def test_missing_station_id_is_rejected(self):
        del self.status["station_id"]

        with self.assertRaises(ValidationError):
            StationStatus.model_validate(self.status)

    def test_invalid_bike_count_is_rejected(self):
        # null bị reject theo chính sách bản nháp, chờ nhóm chốt.
        for value in (-1, None, "5", True):
            with self.subTest(value=value):
                row = {**self.status, "num_bikes_available": value}

                with self.assertRaises(ValidationError):
                    StationStatus.model_validate(row)

    def test_invalid_flags_are_rejected(self):
        for value in (2, -1, "yes", "1", None):
            with self.subTest(value=value):
                row = {**self.status, "is_renting": value}

                with self.assertRaises(ValidationError):
                    StationStatus.model_validate(row)

    def test_nested_vehicle_data_is_validated(self):
        invalid_items = [
            {"vehicle_type_id": "1", "count": -1},
            {"vehicle_type_id": "1"},
            {"count": 5},
        ]

        for item in invalid_items:
            with self.subTest(item=item):
                row = {
                    **self.status,
                    "vehicle_types_available": [item],
                }

                with self.assertRaises(ValidationError):
                    StationStatus.model_validate(row)

    def test_missing_scooter_count_is_not_filled_with_zero(self):
        result = StationStatus.model_validate(self.status)
        exported = result.model_dump(exclude_unset=True)

        self.assertNotIn("num_scooters_available", exported)
        self.assertNotIn("num_scooters_unavailable", exported)

    def test_unknown_source_field_is_preserved(self):
        row = {**self.status, "future_source_field": {"value": 7}}
        result = StationStatus.model_validate(row)

        self.assertEqual(
            result.model_dump()["future_source_field"],
            {"value": 7},
        )

    def test_old_last_reported_is_preserved(self):
        row = {**self.status, "last_reported": 86400}
        result = StationStatus.model_validate(row)

        self.assertEqual(result.last_reported, 86400)

    def test_information_nullable_fields(self):
        row = {
            **self.information,
            "short_name": None,
            "capacity": None,
            "is_virtual_station": None,
        }
        result = StationInformation.model_validate(row)

        self.assertIsNone(result.short_name)
        self.assertIsNone(result.capacity)

    def test_invalid_coordinates_are_rejected(self):
        for field, value in (
            ("lat", 91),
            ("lon", -181),
            ("lat", float("nan")),
        ):
            with self.subTest(field=field, value=value):
                row = {**self.information, field: value}

                with self.assertRaises(ValidationError):
                    StationInformation.model_validate(row)

    def test_message_is_flat_and_json_roundtrip_works(self):
        result = StationStatusMessage.model_validate({
            **self.status,
            **self.metadata,
        })

        exported = result.model_dump(mode="json", exclude_unset=True)

        self.assertEqual(exported["station_id"], "station-test-01")
        self.assertEqual(exported["poll_id"], "status-test-poll-01")
        self.assertNotIn("station", exported)

        restored = StationStatusMessage.model_validate_json(
            result.model_dump_json(exclude_unset=True)
        )
        self.assertEqual(restored, result)

    def test_timestamp_without_timezone_is_rejected(self):
        row = {
            **self.status,
            **self.metadata,
            "collected_at": "2026-11-17T03:00:00",
        }

        with self.assertRaises(ValidationError):
            StationStatusMessage.model_validate(row)

    def test_missing_poll_id_is_rejected(self):
        row = {**self.status, **self.metadata}
        del row["poll_id"]

        with self.assertRaises(ValidationError):
            StationStatusMessage.model_validate(row)

    def test_invalid_message_contract(self):
        row = {
            "source": "gbfs.station_status",
            "error_type": "VALIDATION_ERROR",
            "error_message": "Missing station_id",
            "raw_payload": {"num_bikes_available": 5},
            "detected_at": "2026-11-17T03:00:00Z",
        }

        result = InvalidMessage.model_validate(row)
        self.assertEqual(result.raw_payload, row["raw_payload"])

        del row["error_type"]

        with self.assertRaises(ValidationError):
            InvalidMessage.model_validate(row)


if __name__ == "__main__":
    unittest.main()
