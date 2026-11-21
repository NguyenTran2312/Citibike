import json
import unittest
from unittest.mock import Mock, patch

from ingestion.gbfs.producer import GBFSProducer


class TestGBFSProducer(unittest.TestCase):
    def setUp(self):
        # Khóa các biến cấu hình để test không phụ thuộc máy chạy.
        env_patch = patch.dict("os.environ", {
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "GBFS_STATUS_TOPIC": "gbfs.station_status",
            "GBFS_INFORMATION_TOPIC": "gbfs.station_information",
            "GBFS_INVALID_TOPIC": "gbfs.invalid",
        })
        env_patch.start()
        self.addCleanup(env_patch.stop)

        client_patch = patch(
            "ingestion.gbfs.producer.Producer"
        )
        factory = client_patch.start()
        self.addCleanup(client_patch.stop)

        self.client = factory.return_value
        self.client.flush.return_value = 0
        self.producer = GBFSProducer()

        self.status = {
            "station_id": "test-station",
            "poll_id": "test-poll",
            "schema_version": 1,
            "last_updated": 1794891057,
            "collected_at": "2026-11-17T05:00:00Z",
            "num_bikes_available": 5,
            "num_docks_available": 10,
            "is_installed": 1,
            "is_renting": 1,
            "is_returning": 1,
            "last_reported": 1794891000,
        }

    def test_valid_status_uses_station_id_as_key(self):
        topic = self.producer.publish(
            "station_status", self.status
        )

        self.assertEqual(topic, "gbfs.station_status")

        sent = self.client.produce.call_args.kwargs
        self.assertEqual(sent["topic"], topic)
        self.assertEqual(sent["key"], b"test-station")

        payload = json.loads(sent["value"])
        self.assertEqual(payload["poll_id"], "test-poll")
        self.assertIs(payload["is_renting"], True)

    def test_valid_information_has_correct_topic_and_fields(self):
        row = {
            "station_id": "test-station",
            "name": "Test Station",
            "lat": 40.75,
            "lon": -73.98,
            "last_updated": 1794891057,
            "collected_at": "2026-11-17T05:00:00Z",
        }

        topic = self.producer.publish(
            "station_information", row
        )

        self.assertEqual(topic, "gbfs.station_information")

        sent = self.client.produce.call_args.kwargs
        payload = json.loads(sent["value"])

        self.assertEqual(sent["key"], b"test-station")
        self.assertNotIn("poll_id", payload)
        self.assertNotIn("schema_version", payload)
        self.assertIn("is_virtual_station", payload)
        self.assertIsNone(payload["is_virtual_station"])

    def test_invalid_record_is_routed_to_invalid_topic(self):
        row = dict(self.status)
        del row["station_id"]

        topic = self.producer.publish("station_status", row)

        self.assertEqual(topic, "gbfs.invalid")
        self.client.produce.assert_called_once()

        sent = self.client.produce.call_args.kwargs
        payload = json.loads(sent["value"])

        self.assertEqual(sent["topic"], "gbfs.invalid")
        self.assertIsNone(sent["key"])
        self.assertEqual(
            payload["source"], "gbfs.station_status"
        )
        self.assertEqual(
            payload["error_type"], "VALIDATION_ERROR"
        )
        self.assertEqual(payload["raw_payload"], row)

    def test_unknown_feed_is_rejected_without_sending(self):
        with self.assertRaises(ValueError):
            self.producer.publish("unknown_feed", self.status)

        self.client.produce.assert_not_called()

    def test_full_queue_is_polled_and_retried(self):
        self.client.produce.side_effect = [
            BufferError("Queue full"),
            None,
        ]

        self.producer.publish("station_status", self.status)

        self.assertEqual(self.client.produce.call_count, 2)
        self.client.poll.assert_any_call(0.1)

    def test_full_queue_has_a_timeout(self):
        self.client.produce.side_effect = BufferError("Queue full")

        with patch(
            "ingestion.gbfs.producer.time.monotonic",
            side_effect=[100, 111],
        ):
            with self.assertRaises(TimeoutError):
                self.producer.publish(
                    "station_status", self.status
                )

    def test_delivery_failure_is_reported_by_flush(self):
        self.producer.publish("station_status", self.status)
        callback = self.client.produce.call_args.kwargs[
            "on_delivery"
        ]

        def failed_flush(timeout):
            callback("Simulated broker error", Mock())
            return 0

        self.client.flush.side_effect = failed_flush

        with self.assertRaises(RuntimeError):
            self.producer.flush()

    def test_pending_messages_cannot_report_success(self):
        self.client.flush.return_value = 1

        with self.assertRaises(TimeoutError):
            self.producer.flush()

    def test_successful_delivery_is_counted(self):
        self.producer.publish("station_status", self.status)
        callback = self.client.produce.call_args.kwargs[
            "on_delivery"
        ]

        record = Mock()
        record.topic.return_value = "gbfs.station_status"

        def successful_flush(timeout):
            callback(None, record)
            return 0

        self.client.flush.side_effect = successful_flush

        self.assertEqual(
            self.producer.flush(),
            {"gbfs.station_status": 1},
        )


if __name__ == "__main__":
    unittest.main()
