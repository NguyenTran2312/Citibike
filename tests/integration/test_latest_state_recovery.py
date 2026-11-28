"""Integration test STR-05/06: Latest State Consumer với Kafka và PostgreSQL thật.

Kiểm chứng trên Docker Compose (không mock):
  1. Message cũ hơn đến sau không ghi đè trạng thái mới hơn (out-of-order).
  2. Gửi lại cùng message không ghi lại dòng (duplicate replay).
  3. PostgreSQL chết: offset không được commit; PostgreSQL sống lại:
     consumer tự phục hồi (restart: unless-stopped) và xử lý message đang chờ.
  4. Process consumer bị kill: Docker khởi động lại, đọc tiếp từ offset đã commit.
  5. Message hỏng đi sang gbfs.invalid; consumer vẫn chạy và offset vẫn tiến.

Chạy trên một Compose project riêng để không đụng stack đang dùng
(từ thư mục gốc repository):

    export COMPOSE_PROJECT_NAME=citibike-itest
    docker compose --profile pipeline build gbfs-poller
    docker compose --profile pipeline up -d --wait latest-state-consumer
    python -m unittest tests.integration.test_latest_state_recovery -v
    docker compose --profile pipeline down -v

Test dừng/khởi động lại service `postgres` của project đó. Dữ liệu test dùng
station_id bắt đầu bằng `itest-` và được xóa khi kết thúc.
"""
import json
import os
import subprocess
import time
import unittest
import uuid
from datetime import datetime, timedelta, timezone

import psycopg
from confluent_kafka import Consumer, Producer, TopicPartition

KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
STATUS_TOPIC = os.getenv("GBFS_STATUS_TOPIC", "gbfs.station_status")
INVALID_TOPIC = os.getenv("GBFS_INVALID_TOPIC", "gbfs.invalid")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "citibike-postgres-latest")
CONSUMER_SERVICE = "latest-state-consumer"

PG = {
    "host": os.getenv("PGHOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": "citibike",
    "user": os.getenv("POSTGRES_USER", "citibike"),
    "password": os.getenv("POSTGRES_PASSWORD", "citibike-local-secret"),
    "connect_timeout": 5,
}

PROCESS_TIMEOUT = 60      # consumer xử lý message ở trạng thái bình thường
RECOVERY_TIMEOUT = 240    # gồm cả độ trễ restart của Docker (tối đa 1 phút)
OUTAGE_SECONDS = 20


def compose(*args):
    return subprocess.run(
        ["docker", "compose", "--profile", "pipeline", *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def restart_count():
    container = compose("ps", "-a", "-q", CONSUMER_SERVICE)
    return int(subprocess.run(
        ["docker", "inspect", "-f", "{{.RestartCount}}", container],
        check=True, capture_output=True, text=True,
    ).stdout.strip())


def kill_consumer_process():
    # Giả lập crash: SIGKILL process Python bên trong container (không phải
    # `docker stop`/`kill`, vì Docker coi đó là dừng thủ công và không restart).
    script = (
        "import os, signal\n"
        "me = os.getpid()\n"
        "for pid in filter(str.isdigit, os.listdir('/proc')):\n"
        "    if int(pid) == me: continue\n"
        "    cmd = open(f'/proc/{pid}/cmdline', 'rb').read()\n"
        "    if b'serving.latest_state_consumer' in cmd:\n"
        "        os.kill(int(pid), signal.SIGKILL)\n"
    )
    compose("exec", "-T", CONSUMER_SERVICE, "python", "-c", script)


class LatestStateRecoveryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.prefix = f"itest-{uuid.uuid4().hex[:8]}-"
        cls.base_time = datetime.now(timezone.utc).replace(microsecond=0)
        cls.producer = Producer({
            "bootstrap.servers": KAFKA,
            "enable.idempotence": True,
            "acks": "all",
        })

    @classmethod
    def tearDownClass(cls):
        compose("start", "postgres")
        deadline = time.monotonic() + 60
        while True:
            try:
                with psycopg.connect(**PG) as connection:
                    connection.execute(
                        "DELETE FROM station_status_latest WHERE station_id LIKE 'itest-%'"
                    )
                return
            except psycopg.OperationalError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(2)

    # ---------------------------------------------------------------- helpers

    def status(self, station, poll, minute):
        """Message station_status hợp lệ; `minute` quyết định collected_at."""
        collected_at = self.base_time + timedelta(minutes=minute)
        return {
            "station_id": self.prefix + station,
            "poll_id": f"{self.prefix}{poll}",
            "schema_version": 1,
            "last_updated": int(collected_at.timestamp()),
            "collected_at": collected_at.isoformat().replace("+00:00", "Z"),
            "num_bikes_available": minute,
            "num_docks_available": 10,
            "is_installed": True,
            "is_renting": True,
            "is_returning": True,
            "last_reported": int(collected_at.timestamp()) - 30,
        }

    def produce(self, key, value):
        """Gửi một message, chờ broker xác nhận, trả về (partition, offset)."""
        delivered = []

        def on_delivery(error, message):
            self.assertIsNone(error)
            delivered.append((message.partition(), message.offset()))

        payload = value if isinstance(value, bytes) else json.dumps(value).encode()
        self.producer.produce(
            STATUS_TOPIC, key=key.encode(), value=payload, on_delivery=on_delivery
        )
        self.assertEqual(self.producer.flush(30), 0)
        return delivered[0]

    def produce_status(self, message):
        return self.produce(message["station_id"], message)

    def committed(self, partition):
        reader = Consumer({
            "bootstrap.servers": KAFKA,
            "group.id": GROUP_ID,
            "enable.auto.commit": False,
        })
        try:
            [position] = reader.committed(
                [TopicPartition(STATUS_TOPIC, partition)], timeout=10
            )
            return position.offset
        finally:
            reader.close()

    def wait_committed(self, partition, offset, timeout=PROCESS_TIMEOUT):
        """Chờ tới khi group đã commit qua `offset` (committed = offset + 1)."""
        deadline = time.monotonic() + timeout
        while self.committed(partition) <= offset:
            if time.monotonic() > deadline:
                self.fail(
                    f"Offset {offset} (partition {partition}) chưa được commit "
                    f"sau {timeout}s; committed={self.committed(partition)}"
                )
            time.sleep(1)

    def row(self, station):
        with psycopg.connect(**PG) as connection:
            return connection.execute(
                "SELECT poll_id, xmin::text FROM station_status_latest "
                "WHERE station_id = %s",
                (self.prefix + station,),
            ).fetchone()

    def assert_consumer_ready(self, timeout=PROCESS_TIMEOUT):
        """Consumer đang chạy và xử lý được message."""
        probe = self.status(f"probe-{uuid.uuid4().hex[:6]}", "probe", 0)
        self.wait_committed(*self.produce_status(probe), timeout=timeout)

    # ------------------------------------------------------------------ tests

    def test_1_older_message_arriving_later_does_not_overwrite_newer_state(self):
        newer = self.status("order", "poll-b", 2)
        older = self.status("order", "poll-a", 1)

        self.produce_status(newer)
        self.wait_committed(*self.produce_status(older))

        poll_id, _ = self.row("order")
        self.assertEqual(poll_id, newer["poll_id"])

    def test_2_duplicate_replay_does_not_rewrite_the_row(self):
        message = self.status("dup", "poll-1", 1)

        self.wait_committed(*self.produce_status(message))
        poll_id, xmin_before = self.row("dup")

        self.wait_committed(*self.produce_status(message))
        poll_id_after, xmin_after = self.row("dup")

        self.assertEqual(poll_id_after, poll_id)
        # xmin không đổi: UPSERT không ghi phiên bản dòng mới.
        self.assertEqual(xmin_after, xmin_before)

    def test_3_postgres_outage_holds_the_offset_then_consumer_recovers(self):
        self.assert_consumer_ready()
        restarts_before = restart_count()

        compose("stop", "postgres")
        try:
            message = self.status("outage", "poll-1", 1)
            partition, offset = self.produce_status(message)

            time.sleep(OUTAGE_SECONDS)

            # Chưa ghi được PostgreSQL thì không được commit offset.
            self.assertLessEqual(self.committed(partition), offset)
            self.assertGreater(restart_count(), restarts_before)
        finally:
            compose("start", "postgres")

        # Không thao tác tay: consumer tự phục hồi và xử lý message đang chờ.
        self.wait_committed(partition, offset, timeout=RECOVERY_TIMEOUT)
        poll_id, _ = self.row("outage")
        self.assertEqual(poll_id, message["poll_id"])

    def test_4_crashed_consumer_resumes_from_the_committed_offset(self):
        self.assert_consumer_ready()
        restarts_before = restart_count()

        kill_consumer_process()
        message = self.status("crash", "poll-1", 1)
        partition, offset = self.produce_status(message)

        self.wait_committed(partition, offset, timeout=RECOVERY_TIMEOUT)
        self.assertGreater(restart_count(), restarts_before)
        poll_id, _ = self.row("crash")
        self.assertEqual(poll_id, message["poll_id"])

    def test_5_bad_message_is_quarantined_and_consumer_keeps_going(self):
        self.assert_consumer_ready()
        reader = Consumer({
            "bootstrap.servers": KAFKA,
            "group.id": f"{self.prefix}invalid-reader",
            "enable.auto.commit": False,
        })
        self.addCleanup(reader.close)
        metadata = reader.list_topics(INVALID_TOPIC, timeout=10)
        start = [
            TopicPartition(
                INVALID_TOPIC, p,
                reader.get_watermark_offsets(TopicPartition(INVALID_TOPIC, p))[1],
            )
            for p in metadata.topics[INVALID_TOPIC].partitions
        ]

        key = self.prefix + "poison"
        bad_partition, bad_offset = self.produce(key, b"not json")
        good = self.status("poison", "poll-1", 1)
        self.wait_committed(*self.produce_status(good))

        # Consumer không bị kẹt ở message hỏng.
        self.assertGreater(self.committed(bad_partition), bad_offset)
        poll_id, _ = self.row("poison")
        self.assertEqual(poll_id, good["poll_id"])

        reader.assign(start)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            message = reader.poll(1.0)
            if message is None or message.error() is not None:
                continue
            headers = dict(message.headers() or [])
            if (
                headers.get("source_partition") == str(bad_partition).encode()
                and headers.get("source_offset") == str(bad_offset).encode()
            ):
                payload = json.loads(message.value())
                self.assertEqual(payload["error_type"], "CONSUMER_VALIDATION_ERROR")
                self.assertEqual(payload["source"], STATUS_TOPIC)
                self.assertEqual(payload["raw_payload"], "not json")
                self.assertEqual(message.key(), key.encode())
                return
        self.fail("Không tìm thấy message hỏng trong gbfs.invalid")


if __name__ == "__main__":
    unittest.main()
