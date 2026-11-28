import json
import shutil
import signal
import tempfile
import threading
import unittest
from datetime import datetime, timezone
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


TWO_STATIONS = {
    "last_updated": 1794891057,
    "ttl": 60,
    "data": {
        "stations": [
            {**PAYLOAD["data"]["stations"][0], "station_id": "station-a"},
            {**PAYLOAD["data"]["stations"][0], "station_id": "station-b"},
            # Sai schema: phải đi sang gbfs.invalid, không được mất.
            {"station_id": "station-c", "num_bikes_available": None},
        ]
    },
}


class FakeProducer:
    """Thay GBFSProducer: ghi lại những gì publish_poll_output thật gửi đi."""

    sent = []

    def __init__(self):
        self.queued = {}

    def _queue(self, topic, row):
        FakeProducer.sent.append((topic, row))
        self.queued[topic] = self.queued.get(topic, 0) + 1
        return topic

    def publish(self, feed_name, payload):
        return self._queue("gbfs.station_status", payload)

    def send_invalid(self, payload):
        return self._queue("gbfs.invalid", payload)

    def flush(self):
        return dict(self.queued)


class TestInterruptedNormalization(unittest.TestCase):
    """Replay không được gửi một phần valid.jsonl/invalid.jsonl rồi xóa thư mục
    (poller bị kill hoặc lỗi I/O giữa lúc chuẩn hóa, sau khi raw.json đã ghi)."""

    POLL_ID = "station_status-11111111-2222-4333-8444-555555555555"
    COLLECTED_AT = datetime(2026, 11, 28, 2, 0, 0, 123456, tzinfo=timezone.utc)

    def setUp(self):
        self.data_dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.data_dir, ignore_errors=True)
        self.env = {
            "GBFS_DATA_DIR": str(self.data_dir),
            "GBFS_PUBLISH_KAFKA": "true",
            "GBFS_ARCHIVE_RAW": "true",
        }
        FakeProducer.sent = []
        for target, mock in (
            ("ingestion.gbfs.producer.GBFSProducer", FakeProducer),
            ("ingestion.gbfs.raw_storage.upload_raw_json", Mock(return_value="s3://raw/x")),
        ):
            patcher = patch(target, mock)
            patcher.start()
            self.addCleanup(patcher.stop)

        # Một lần polling đầy đủ (như poll_feed ghi), làm chuẩn để cắt bớt.
        self.poll_dir = self.data_dir / "station_status" / self.POLL_ID
        self.poll_dir.mkdir(parents=True)
        (self.poll_dir / "raw.json").write_text(json.dumps({
            "poll_id": self.POLL_ID,
            "source_url": "https://example.com/s.json",
            "collected_at": self.COLLECTED_AT.isoformat(),
            "payload": TWO_STATIONS,
        }), encoding="utf-8")
        with self.assertLogs("ingestion.gbfs.poller", level="INFO"):
            poller.write_messages("station_status", TWO_STATIONS, self.POLL_ID,
                                  self.COLLECTED_AT, self.poll_dir)
        self.full_valid = (self.poll_dir / "valid.jsonl").read_text(encoding="utf-8")
        self.full_invalid = (self.poll_dir / "invalid.jsonl").read_text(encoding="utf-8")

    def interrupt(self, valid, invalid):
        (self.poll_dir / "valid.jsonl").write_text(valid, encoding="utf-8")
        (self.poll_dir / "invalid.jsonl").write_text(invalid, encoding="utf-8")

    def replay(self):
        with patch.dict("os.environ", self.env), \
                self.assertLogs("ingestion.gbfs.poller", level="INFO"):
            poller.replay_poll(self.poll_dir)

    def assert_every_station_sent(self):
        valid = [row for topic, row in FakeProducer.sent if topic == "gbfs.station_status"]
        invalid = [row for topic, row in FakeProducer.sent if topic == "gbfs.invalid"]
        self.assertEqual(sorted(r["station_id"] for r in valid), ["station-a", "station-b"])
        self.assertEqual([r["raw_payload"]["station_id"] for r in invalid], ["station-c"])
        # Poll ID và thời điểm thu thập của lần polling gốc, không sinh mới.
        for row in valid:
            self.assertEqual(row["poll_id"], self.POLL_ID)
            self.assertEqual(
                datetime.fromisoformat(row["collected_at"].replace("Z", "+00:00")),
                self.COLLECTED_AT)
        self.assertFalse(self.poll_dir.exists())

    def test_replay_of_a_partially_written_valid_file_sends_every_station(self):
        self.interrupt(self.full_valid.splitlines(keepends=True)[0], "")
        self.replay()
        self.assert_every_station_sent()

    def test_replay_of_empty_normalized_files_sends_every_station(self):
        self.interrupt("", "")
        self.replay()
        self.assert_every_station_sent()

    def test_replay_of_missing_normalized_files_sends_every_station(self):
        (self.poll_dir / "valid.jsonl").unlink()
        (self.poll_dir / "invalid.jsonl").unlink()
        self.replay()
        self.assert_every_station_sent()

    def test_replay_of_a_line_cut_in_the_middle_sends_every_station(self):
        self.interrupt(self.full_valid[: len(self.full_valid) - 20], self.full_invalid)
        self.replay()
        self.assert_every_station_sent()

    def test_stale_delivery_marker_of_an_incomplete_poll_does_not_skip_kafka(self):
        self.interrupt(self.full_valid.splitlines(keepends=True)[0], "")
        (self.poll_dir / "kafka_delivery.json").write_text("{}", encoding="utf-8")
        self.replay()
        self.assert_every_station_sent()

    def test_complete_poll_is_replayed_as_written(self):
        self.replay()
        self.assert_every_station_sent()

    def test_incomplete_files_are_not_published_and_the_directory_is_kept(self):
        self.interrupt(self.full_valid.splitlines(keepends=True)[0], "")
        with patch.dict("os.environ", self.env), \
                self.assertRaises(poller.IncompletePollError), \
                self.assertLogs("ingestion.gbfs.poller", level="WARNING"):
            poller.finish_poll("station_status", self.POLL_ID, self.COLLECTED_AT,
                               self.poll_dir, publish=True)
        self.assertEqual(FakeProducer.sent, [])
        self.assertTrue((self.poll_dir / "raw.json").exists())


class Interrupted(BaseException):
    """Process chết giữa chừng: không phải Exception, nên không handler nào bắt
    (finish_poll chỉ bắt Exception), giống SIGKILL ở đúng điểm được chọn."""


class FakeKafkaClient:
    """Thay confluent_kafka.Producer bên dưới GBFSProducer thật: giữ message
    cho tới flush() rồi xác nhận, ghi lại (topic, key, value) đã được ack."""

    acked = []

    def __init__(self, config):
        self.pending = []

    def produce(self, topic, key, value, on_delivery):
        self.pending.append((topic, key, value, on_delivery))

    def poll(self, timeout):
        return 0

    def flush(self, timeout):
        for topic, key, value, on_delivery in self.pending:
            FakeKafkaClient.acked.append((topic, key.decode() if key else None,
                                          json.loads(value)))
            on_delivery(None, Mock(topic=Mock(return_value=topic)))
        self.pending = []
        return 0


class TestRepairInterruption(unittest.TestCase):
    """Replay đang sửa một lần polling chưa đủ mà bị ngắt (process chết) ở ranh
    giới giữa bỏ xác nhận Kafka cũ và ghi lại valid.jsonl / invalid.jsonl.

    Trạng thái ban đầu (independent review 84f6cd7): raw.json có A, B (hợp lệ)
    và C (sai schema); valid.jsonl chỉ có A; kafka_delivery.json cũ xác nhận
    đúng một message A. Đi qua replay_poll, finish_poll, publish_poll_output và
    GBFSProducer thật; chỉ client Kafka và upload MinIO là giả.
    """

    POLL_ID = "station_status-99999999-8888-4777-8666-555555555555"
    COLLECTED_AT = datetime(2026, 11, 28, 4, 0, 0, 654321, tzinfo=timezone.utc)

    def setUp(self):
        self.data_dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.data_dir, ignore_errors=True)
        self.env = {
            "GBFS_DATA_DIR": str(self.data_dir),
            "GBFS_PUBLISH_KAFKA": "true",
            "GBFS_ARCHIVE_RAW": "true",
            "GBFS_STATUS_TOPIC": "gbfs.station_status",
            "GBFS_INVALID_TOPIC": "gbfs.invalid",
        }
        FakeKafkaClient.acked = []
        self.archive = Mock(return_value="s3://raw/x")
        for target, mock in (
            ("ingestion.gbfs.producer.Producer", FakeKafkaClient),
            ("ingestion.gbfs.raw_storage.upload_raw_json", self.archive),
        ):
            patcher = patch(target, mock)
            patcher.start()
            self.addCleanup(patcher.stop)

        self.poll_dir = self.data_dir / "station_status" / self.POLL_ID
        self.poll_dir.mkdir(parents=True)
        (self.poll_dir / "raw.json").write_text(json.dumps({
            "poll_id": self.POLL_ID,
            "source_url": "https://example.com/s.json",
            "collected_at": self.COLLECTED_AT.isoformat(),
            "payload": TWO_STATIONS,
        }), encoding="utf-8")
        with self.assertLogs("ingestion.gbfs.poller", level="INFO"):
            poller.write_messages("station_status", TWO_STATIONS, self.POLL_ID,
                                  self.COLLECTED_AT, self.poll_dir)
        only_a = (self.poll_dir / "valid.jsonl").read_text(encoding="utf-8").splitlines(True)[0]
        (self.poll_dir / "valid.jsonl").write_text(only_a, encoding="utf-8")
        (self.poll_dir / "invalid.jsonl").write_text("", encoding="utf-8")
        self.marker = self.poll_dir / "kafka_delivery.json"
        self.marker.write_text(json.dumps({
            "feed_name": "station_status", "poll_id": self.POLL_ID,
            "confirmed": {"gbfs.station_status": 1},
        }), encoding="utf-8")

    def replay(self):
        with patch.dict("os.environ", self.env), \
                self.assertLogs("ingestion.gbfs.poller", level="INFO"):
            poller.replay_poll(self.poll_dir)

    def replay_interrupted(self, **patches):
        """Replay với các hàm của poller bị thay (patches), phải chết với Interrupted."""
        with patch.dict("os.environ", self.env), \
                self.assertLogs("ingestion.gbfs.poller", level="INFO"), \
                patch.multiple(poller, **patches), \
                self.assertRaises(Interrupted):
            poller.replay_poll(self.poll_dir)

    def lines(self, name):
        return (self.poll_dir / name).read_text(encoding="utf-8").splitlines()

    def assert_untouched_spool(self, marker):
        self.assertEqual(FakeKafkaClient.acked, [])
        self.assertTrue(self.poll_dir.exists())
        self.assertEqual(self.marker.exists(), marker)

    def assert_whole_poll_delivered_then_cleaned(self):
        status = [(k, v) for t, k, v in FakeKafkaClient.acked if t == "gbfs.station_status"]
        invalid = [(k, v) for t, k, v in FakeKafkaClient.acked if t == "gbfs.invalid"]
        self.assertEqual({t for t, _, _ in FakeKafkaClient.acked},
                         {"gbfs.station_status", "gbfs.invalid"})
        # B và C (chưa từng tới Kafka) được gửi; A gửi lại, downstream dedup.
        self.assertEqual(sorted(k for k, _ in status), ["station-a", "station-b"])
        self.assertEqual([k for k, _ in invalid], ["station-c"])
        self.assertEqual(invalid[0][1]["raw_payload"]["station_id"], "station-c")
        for key, message in status:
            self.assertEqual(message["station_id"], key)       # Kafka key = station_id
            self.assertEqual(message["poll_id"], self.POLL_ID)
            self.assertEqual(
                datetime.fromisoformat(message["collected_at"].replace("Z", "+00:00")),
                self.COLLECTED_AT)
            self.assertEqual(message["last_updated"], TWO_STATIONS["last_updated"])
        self.archive.assert_called()
        self.assertFalse(self.poll_dir.exists())

    def test_interrupted_right_after_rebuild_then_restarted_delivers_the_whole_poll(self):
        real_write = poller.write_messages

        def write_then_die(*args):
            real_write(*args)
            raise Interrupted("after rebuild")

        self.replay_interrupted(write_messages=write_then_die)
        # Output đã đủ (A, B + C) nhưng chưa gì được gửi, spool còn nguyên.
        self.assertEqual(len(self.lines("valid.jsonl")) + len(self.lines("invalid.jsonl")), 3)
        self.assertTrue(self.poll_dir.exists())
        self.assertEqual(FakeKafkaClient.acked, [])

        self.replay()   # khởi động lại
        self.assert_whole_poll_delivered_then_cleaned()

    def test_obsolete_marker_is_gone_before_normalized_files_become_complete(self):
        real_write = poller.write_messages
        seen = []

        def observe(*args):
            seen.append(self.marker.exists())
            real_write(*args)

        with patch.object(poller, "write_messages", observe):
            self.replay()
        self.assertEqual(seen, [False])
        self.assert_whole_poll_delivered_then_cleaned()

    def test_interrupted_just_before_invalidation(self):
        self.replay_interrupted(invalidate_delivery=Mock(side_effect=Interrupted("before")))
        self.assert_untouched_spool(marker=True)
        self.assertEqual(len(self.lines("valid.jsonl")), 1)   # chưa sửa gì

        self.replay()
        self.assert_whole_poll_delivered_then_cleaned()

    def test_interrupted_just_after_invalidation(self):
        real_invalidate = poller.invalidate_delivery

        def invalidate_then_die(output_dir):
            real_invalidate(output_dir)
            raise Interrupted("after invalidation")

        self.replay_interrupted(invalidate_delivery=invalidate_then_die)
        self.assert_untouched_spool(marker=False)
        self.assertEqual(len(self.lines("valid.jsonl")), 1)   # output chưa sửa

        self.replay()
        self.assert_whole_poll_delivered_then_cleaned()

    def test_failure_to_invalidate_stops_the_repair(self):
        rebuild = Mock()
        with patch.dict("os.environ", self.env), \
                self.assertLogs("ingestion.gbfs.poller", level="INFO"), \
                patch.multiple(poller, write_messages=rebuild,
                               invalidate_delivery=Mock(side_effect=PermissionError("read-only"))), \
                self.assertRaises(PermissionError):
            poller.replay_poll(self.poll_dir)
        rebuild.assert_not_called()
        self.assert_untouched_spool(marker=True)
        self.assertEqual(len(self.lines("valid.jsonl")), 1)

    def test_kafka_failure_after_repair_keeps_the_spool_until_a_later_success(self):
        with patch.dict("os.environ", self.env), \
                self.assertLogs("ingestion.gbfs.poller", level="INFO"), \
                patch("ingestion.gbfs.producer.publish_poll_output",
                      side_effect=RuntimeError("Kafka down")), \
                self.assertRaises(RuntimeError):
            poller.replay_poll(self.poll_dir)
        self.assert_untouched_spool(marker=False)

        self.replay()
        self.assert_whole_poll_delivered_then_cleaned()


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
