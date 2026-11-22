import json
import unittest
from unittest.mock import MagicMock, Mock, patch

import psycopg

from serving.latest_state_consumer import (
    Quarantine,
    decode_payload,
    process_message,
)


VALID_STATUS = {
    "station_id": "test-station",
    "poll_id": "test-poll",
    "schema_version": 1,
    "last_updated": 1794891057,
    "collected_at": "2026-11-17T05:00:00Z",
    "num_bikes_available": 5,
    "num_docks_available": 10,
    "is_installed": True,
    "is_renting": True,
    "is_returning": True,
    "last_reported": 1794891000,
}


class FakeMessage:
    def __init__(self, value, key=b"test-station"):
        self._value = value
        self._key = key

    def value(self):
        return self._value

    def key(self):
        return self._key

    def topic(self):
        return "gbfs.station_status"

    def partition(self):
        return 2

    def offset(self):
        return 41


def make_connection():
    connection = MagicMock()
    # Không nuốt exception khi thoát khối transaction, như psycopg thật.
    connection.transaction.return_value.__exit__.return_value = False
    return connection


class TestProcessMessage(unittest.TestCase):
    def setUp(self):
        self.connection = make_connection()
        self.quarantine = Mock()

    def process(self, value):
        return process_message(FakeMessage(value), self.connection, self.quarantine)

    def test_valid_message_is_upserted(self):
        result = self.process(json.dumps(VALID_STATUS).encode())

        self.assertEqual(result, "upserted")
        self.connection.execute.assert_called_once()
        self.quarantine.send.assert_not_called()

    def test_schema_violation_is_quarantined_not_raised(self):
        row = {k: v for k, v in VALID_STATUS.items() if k != "station_id"}

        result = self.process(json.dumps(row).encode())

        self.assertEqual(result, "quarantined")
        self.connection.execute.assert_not_called()
        self.assertEqual(
            self.quarantine.send.call_args.args[1], "CONSUMER_VALIDATION_ERROR"
        )

    def test_unparseable_and_empty_values_are_quarantined(self):
        for value in (b"not json", None):
            with self.subTest(value=value):
                self.quarantine.reset_mock()

                self.assertEqual(self.process(value), "quarantined")
                self.quarantine.send.assert_called_once()

    def test_row_rejected_by_database_is_quarantined(self):
        self.connection.execute.side_effect = psycopg.DataError("bad value")

        result = self.process(json.dumps(VALID_STATUS).encode())

        self.assertEqual(result, "quarantined")
        self.assertEqual(self.quarantine.send.call_args.args[1], "DATABASE_REJECTED")

    def test_connection_error_is_raised_so_offset_is_not_committed(self):
        self.connection.execute.side_effect = psycopg.OperationalError("down")

        with self.assertRaises(psycopg.OperationalError):
            self.process(json.dumps(VALID_STATUS).encode())

        self.quarantine.send.assert_not_called()


class TestQuarantine(unittest.TestCase):
    def setUp(self):
        patcher = patch("serving.latest_state_consumer.Producer")
        self.client = patcher.start().return_value
        self.addCleanup(patcher.stop)
        self.quarantine = Quarantine("gbfs.invalid", "localhost:9092")

    def deliver(self, error):
        def flush(timeout):
            self.client.produce.call_args.kwargs["on_delivery"](error, Mock())
            return 0

        self.client.flush.side_effect = flush

    def test_invalid_message_follows_contract_and_keeps_source_position(self):
        self.deliver(None)

        with self.assertLogs("serving.latest_state_consumer", level="WARNING"):
            self.quarantine.send(FakeMessage(b"not json"), "CONSUMER_VALIDATION_ERROR", ValueError("x"))

        sent = self.client.produce.call_args.kwargs
        payload = json.loads(sent["value"])

        self.assertEqual(sent["topic"], "gbfs.invalid")
        self.assertEqual(sent["key"], b"test-station")
        self.assertEqual(
            set(payload),
            {"source", "error_type", "error_message", "raw_payload", "detected_at"},
        )
        self.assertEqual(payload["source"], "gbfs.station_status")
        self.assertEqual(payload["raw_payload"], "not json")
        self.assertIn(("source_offset", b"41"), sent["headers"])

    def test_delivery_failure_raises_so_offset_is_not_committed(self):
        self.deliver("broker unavailable")

        with self.assertRaises(RuntimeError):
            self.quarantine.send(FakeMessage(b"{}"), "CONSUMER_VALIDATION_ERROR", ValueError("x"))

    def test_undelivered_message_raises(self):
        self.client.flush.return_value = 1

        with self.assertRaises(RuntimeError):
            self.quarantine.send(FakeMessage(b"{}"), "CONSUMER_VALIDATION_ERROR", ValueError("x"))


class TestDecodePayload(unittest.TestCase):
    def test_decode_payload(self):
        self.assertEqual(decode_payload(b'{"a": 1}'), {"a": 1})
        self.assertEqual(decode_payload(b"\xff oops"), "� oops")
        self.assertIsNone(decode_payload(None))


if __name__ == "__main__":
    unittest.main()
