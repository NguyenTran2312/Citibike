import shutil
import signal
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from ingestion.gbfs import poller


PAYLOAD = {
    "last_updated": 1794891057,
    "ttl": 60,
    "data": {
        "stations": [
            {
                "station_id": "test-station",
                "num_bikes_available": 5,
                "num_docks_available": 10,
                "is_installed": 1,
                "is_renting": 1,
                "is_returning": 1,
                "last_reported": 1794891000,
            }
        ]
    },
}


class TestPollFeedFlow(unittest.TestCase):
    """Thứ tự Kafka -> MinIO, MinIO best-effort và dọn file tạm."""

    def setUp(self):
        self.data_dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.data_dir, ignore_errors=True)

        self.env = {
            "GBFS_DATA_DIR": str(self.data_dir),
            "GBFS_PUBLISH_KAFKA": "true",
            "GBFS_ARCHIVE_RAW": "true",
        }

        # Ghi lại thứ tự gọi giữa Kafka và MinIO.
        self.calls = Mock()
        self.calls.publish.return_value = {"gbfs.station_status": 1}
        self.calls.archive.return_value = "s3://raw/test"

        for target, mock in (
            ("ingestion.gbfs.poller.fetch_json", Mock(return_value=PAYLOAD)),
            ("ingestion.gbfs.producer.publish_poll_output", self.calls.publish),
            ("ingestion.gbfs.raw_storage.upload_raw_json", self.calls.archive),
        ):
            patcher = patch(target, mock)
            patcher.start()
            self.addCleanup(patcher.stop)

    def poll(self, **env):
        with patch.dict("os.environ", {**self.env, **env}):
            return poller.poll_feed("station_status", "https://example.com/s.json")

    def test_kafka_is_published_before_raw_is_archived(self):
        self.poll()

        self.assertEqual(
            [name for name, _, _ in self.calls.mock_calls],
            ["publish", "archive"],
        )

    def test_local_files_are_removed_after_publishing(self):
        output_dir = self.poll()

        self.assertFalse(output_dir.exists())

    def test_keep_local_keeps_files_for_reconciliation(self):
        output_dir = self.poll(GBFS_KEEP_LOCAL="true")

        self.assertTrue((output_dir / "raw.json").exists())
        self.assertTrue((output_dir / "kafka_delivery.json").exists())

    def test_files_are_kept_when_kafka_is_disabled(self):
        output_dir = self.poll(GBFS_PUBLISH_KAFKA="false")

        self.assertTrue((output_dir / "valid.jsonl").exists())
        self.calls.publish.assert_not_called()

    def poll_failing(self, error, **env):
        """Lần polling lỗi: phải báo lỗi và log việc giữ lại thư mục."""
        with self.assertLogs("ingestion.gbfs.poller", level="WARNING") as logs:
            with self.assertRaises(error):
                self.poll(**env)
        self.assertIn("Giữ lại", "\n".join(logs.output))

    def kept_dir(self):
        kept = list(self.data_dir.glob("station_status/*"))
        self.assertEqual(len(kept), 1)
        return kept[0]

    def test_archive_failure_does_not_fail_the_poll_and_keeps_files(self):
        self.calls.archive.side_effect = ConnectionError("MinIO down")

        with self.assertLogs("ingestion.gbfs.poller", level="ERROR") as logs:
            output_dir = self.poll()

        self.calls.publish.assert_called_once()
        self.assertIn("Raw archive thất bại", "\n".join(logs.output))
        # Raw chưa có trên MinIO: bản trên máy là bản đầy đủ duy nhất.
        self.assertTrue((output_dir / "raw.json").exists())

    def test_kafka_failure_archives_raw_and_keeps_files_for_replay(self):
        self.calls.publish.side_effect = RuntimeError("Kafka down")

        self.poll_failing(RuntimeError)

        self.calls.archive.assert_called_once()
        kept = self.kept_dir()
        self.assertTrue((kept / "valid.jsonl").exists())
        self.assertFalse((kept / "kafka_delivery.json").exists())

    def test_kafka_and_minio_failure_does_not_delete_the_only_copy(self):
        self.calls.publish.side_effect = RuntimeError("Kafka down")
        self.calls.archive.side_effect = ConnectionError("MinIO down")

        self.poll_failing(RuntimeError)

        self.assertTrue((self.kept_dir() / "raw.json").exists())

    def test_kafka_failure_without_archive_keeps_files(self):
        self.calls.publish.side_effect = RuntimeError("Kafka down")

        self.poll_failing(RuntimeError, GBFS_ARCHIVE_RAW="false")

        self.calls.archive.assert_not_called()
        self.assertTrue((self.kept_dir() / "raw.json").exists())

    def test_malformed_feed_is_archived_and_kept(self):
        with patch("ingestion.gbfs.poller.fetch_json", return_value={"data": {}}):
            self.poll_failing(KeyError)

        self.calls.publish.assert_not_called()
        self.calls.archive.assert_called_once()
        self.assertTrue((self.kept_dir() / "raw.json").exists())

    def replay(self, output_dir):
        with patch.dict("os.environ", self.env):
            poller.replay_poll(output_dir)

    def test_replay_republishes_with_the_original_poll_id(self):
        self.calls.publish.side_effect = RuntimeError("Kafka down")
        self.poll_failing(RuntimeError)
        kept = self.kept_dir()

        self.calls.publish.side_effect = None
        self.replay(kept)

        self.assertEqual(self.calls.publish.call_count, 2)
        self.assertEqual(
            self.calls.publish.call_args.args, ("station_status", kept)
        )
        archived_poll_id = self.calls.archive.call_args.args[2]
        self.assertEqual(archived_poll_id, kept.name)
        self.assertFalse(kept.exists())

    def test_replay_skips_kafka_when_already_delivered(self):
        self.calls.archive.side_effect = ConnectionError("MinIO down")
        with self.assertLogs("ingestion.gbfs.poller", level="ERROR"):
            self.poll()
        kept = self.kept_dir()

        self.calls.archive.side_effect = None
        self.replay(kept)

        self.calls.publish.assert_called_once()
        self.assertEqual(self.calls.archive.call_count, 2)
        self.assertFalse(kept.exists())

    def test_replay_rejects_a_malformed_feed_directory(self):
        with patch("ingestion.gbfs.poller.fetch_json", return_value={"data": {}}):
            self.poll_failing(KeyError)

        with self.assertRaises(ValueError):
            self.replay(self.kept_dir())


class TestStopHandler(unittest.TestCase):
    def test_sigterm_requests_a_graceful_stop(self):
        previous = signal.getsignal(signal.SIGTERM)
        self.addCleanup(signal.signal, signal.SIGTERM, previous)

        stop_event = threading.Event()
        poller.install_stop_handler(stop_event)

        with self.assertLogs("ingestion.gbfs.poller", level="INFO"):
            signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)

        self.assertTrue(stop_event.is_set())


if __name__ == "__main__":
    unittest.main()
