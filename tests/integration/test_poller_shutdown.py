"""Integration test: dừng gbfs-poller giữa lần polling có làm mất dữ liệu không.

Chính sách đã chấp nhận (ingestion/gbfs/README.md):
  * Mỗi lần polling đã lấy được dữ liệu phải ở một trong hai trạng thái:
    - hoàn tất: Kafka có đủ mọi trạm của poll_id, raw có trên MinIO,
      thư mục trên máy đã bị xóa;
    - được giữ lại: thư mục trên máy còn raw.json để replay (--replay).
  * Chỉ khi poller bị buộc dừng (SIGKILL / hết stop_grace_period) mới được
    mất lần polling đang chạy, và chỉ khi raw.json chưa được ghi.

Poller gọi GBFS giả lập (tests/integration/gbfs_mock.py) để điều khiển được độ
trễ và lỗi 503. Chạy trên một Compose project riêng, từ thư mục gốc repository:

    export COMPOSE_PROJECT_NAME=citibike-itest
    export COMPOSE_FILE=docker-compose.yml:tests/integration/poller-shutdown.compose.yml
    export GBFS_MOCK_CONTROL_DIR=$(mktemp -d)
    export MINIO_API_PORT=59000 MINIO_CONSOLE_PORT=59001   # nếu 9000/9001 đã bị chiếm
    docker compose --profile pipeline build gbfs-poller
    docker compose --profile pipeline up -d --wait kafka minio kafka-init minio-init gbfs-mock
    python -m unittest tests.integration.test_poller_shutdown -v
    docker compose --profile pipeline down -v
"""
import io
import json
import os
import subprocess
import tarfile
import time
import unittest
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import boto3
from confluent_kafka import Consumer, KafkaError, TopicPartition

STATIONS = 2519
FEEDS = ("station_status", "station_information")
TOPICS = {
    "station_status": "gbfs.station_status",
    "station_information": "gbfs.station_information",
}
KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONTROL = Path(os.environ["GBFS_MOCK_CONTROL_DIR"])
POLLER = "gbfs-poller"
GRACE_SECONDS = 120   # stop_grace_period của gbfs-poller


def compose(*args, check=True):
    return subprocess.run(
        ["docker", "compose", "--profile", "pipeline", *args],
        check=check, capture_output=True, text=True,
    )


def container_id():
    return compose("ps", "-a", "-q", POLLER).stdout.strip()


def wait_healthy(service, timeout=120):
    """Sau khi unpause, healthcheck cần một lúc mới báo healthy trở lại."""
    container = compose("ps", "-q", service).stdout.strip()
    deadline = time.monotonic() + timeout
    while subprocess.run(
        ["docker", "inspect", "-f", "{{.State.Health.Status}}", container],
        capture_output=True, text=True,
    ).stdout.strip() != "healthy":
        if time.monotonic() > deadline:
            raise TimeoutError(f"{service} chưa healthy sau {timeout}s")
        time.sleep(1)


def exit_code():
    return int(subprocess.run(
        ["docker", "inspect", "-f", "{{.State.ExitCode}}", container_id()],
        check=True, capture_output=True, text=True,
    ).stdout)


def parse_time(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class Polls:
    """Bằng chứng của mọi lần polling có collected_at >= since."""

    def __init__(self, since):
        self.since = since
        # feed -> poll_id -> thông tin
        self.polls = {feed: defaultdict(dict) for feed in FEEDS}
        self.unmapped_info = Counter()
        self._read_raw_sources()
        self._read_kafka()

    def _keep(self, raw):
        return parse_time(raw["collected_at"]) >= self.since

    def _read_raw_sources(self):
        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://localhost:{os.getenv('MINIO_API_PORT', '9000')}",
            aws_access_key_id=os.getenv("MINIO_ROOT_USER", "citibike"),
            aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "citibike-local-secret"),
            region_name="us-east-1",
        )
        pages = s3.get_paginator("list_objects_v2").paginate(Bucket="raw", Prefix="gbfs/")
        for page in pages:
            for item in page.get("Contents", []):
                raw = json.loads(s3.get_object(Bucket="raw", Key=item["Key"])["Body"].read())
                feed = item["Key"].split("/")[1]
                if self._keep(raw):
                    self.polls[feed][raw["poll_id"]].update(
                        archived=True, collected_at=parse_time(raw["collected_at"])
                    )

        # Thư mục trên máy, kể cả khi container đã dừng.
        tar = subprocess.run(
            ["docker", "cp", f"{container_id()}:/app/data/gbfs", "-"],
            capture_output=True,
        )
        if tar.returncode != 0:
            return   # chưa có thư mục nào
        files = defaultdict(set)
        raws = {}
        with tarfile.open(fileobj=io.BytesIO(tar.stdout)) as archive:
            for member in archive.getmembers():
                parts = Path(member.name).parts   # gbfs/<feed>/<poll_id>/<file>
                if member.isfile() and len(parts) == 4:
                    files[(parts[1], parts[2])].add(parts[3])
                    if parts[3] == "raw.json":
                        raws[(parts[1], parts[2])] = json.load(archive.extractfile(member))
        for (feed, poll_id), names in files.items():
            raw = raws.get((feed, poll_id))
            if raw is None or self._keep(raw):
                entry = self.polls[feed][poll_id]
                entry["local"] = sorted(names)
                if raw:
                    entry["collected_at"] = parse_time(raw["collected_at"])

    def _read_kafka(self):
        reader = Consumer({
            "bootstrap.servers": KAFKA,
            "group.id": f"itest-shutdown-{time.time_ns()}",
            "enable.auto.commit": False,
            "enable.partition.eof": True,
        })
        try:
            for feed, topic in TOPICS.items():
                partitions = reader.list_topics(topic, timeout=10).topics[topic].partitions
                reader.assign([TopicPartition(topic, p, 0) for p in partitions])
                remaining = set(partitions)
                stations = defaultdict(set)
                while remaining:
                    message = reader.poll(5.0)
                    if message is None:
                        break
                    if message.error():
                        if message.error().code() == KafkaError._PARTITION_EOF:
                            remaining.discard(message.partition())
                        continue
                    value = json.loads(message.value())
                    if parse_time(value["collected_at"]) < self.since:
                        continue
                    # Message information cũ (trước khi có poll_id) chỉ nối
                    # được qua collected_at.
                    key = value.get("poll_id") or ("collected_at", value["collected_at"])
                    stations[key].add(value["station_id"])
                self._attach(feed, stations)
        finally:
            reader.close()

    def _attach(self, feed, stations):
        by_time = {
            info.get("collected_at"): poll_id
            for poll_id, info in self.polls[feed].items()
        }
        for key, ids in stations.items():
            if isinstance(key, str):
                poll_id = key
            else:   # message không có poll_id: nối qua collected_at
                poll_id = by_time.get(parse_time(key[1]))
                if poll_id is None:
                    self.unmapped_info[key[1]] = len(ids)
                    continue
            self.polls[feed][poll_id]["kafka"] = len(ids)

    def state(self, feed, poll_id):
        info = self.polls[feed][poll_id]
        if "raw.json" in info.get("local", []):
            return "kept"
        if info.get("kafka") == STATIONS and info.get("archived"):
            return "complete"
        return "LOST"

    def summary(self):
        return {
            feed: Counter(self.state(feed, p) for p in self.polls[feed])
            for feed in FEEDS
        }

    def violations(self):
        lost = [
            (feed, poll_id, dict(self.polls[feed][poll_id]))
            for feed in FEEDS for poll_id in self.polls[feed]
            if self.state(feed, poll_id) == "LOST"
        ]
        return lost + [("station_information", "unmapped", dict(self.unmapped_info))] \
            if self.unmapped_info else lost


class PollerShutdownTest(unittest.TestCase):
    generation = 0

    def set_mock(self, delay_seconds=0, fail_first=0):
        # Duy nhất qua các lần chạy: requests.log có thể còn dòng của lần trước.
        PollerShutdownTest.generation = time.time_ns()
        (CONTROL / "behavior.json").write_text(json.dumps({
            "generation": self.generation,
            "delay_seconds": delay_seconds,
            "fail_first": fail_first,
        }))

    def mock_log(self):
        path = CONTROL / "requests.log"
        if not path.exists():
            return []
        return [
            entry for entry in map(json.loads, path.read_text().splitlines())
            if entry.get("generation") == self.generation
        ]

    def wait_for(self, condition, timeout, what):
        deadline = time.monotonic() + timeout
        while not condition():
            if time.monotonic() > deadline:
                self.fail(f"Hết {timeout}s chờ: {what}")
            time.sleep(0.5)

    def start_poller(self, recreate=True):
        self.since = datetime.now(timezone.utc)
        args = ["up", "-d", "--no-deps"] + (["--force-recreate"] if recreate else []) + [POLLER]
        compose(*args)

    def wait_requests(self, count, timeout=60):
        self.wait_for(
            lambda: sum(e["event"] == "received" for e in self.mock_log()) >= count,
            timeout, f"{count} request tới GBFS giả lập",
        )

    def graceful_stop(self):
        started = time.monotonic()
        compose("stop", POLLER)
        return time.monotonic() - started, exit_code()

    def report(self, name, polls, duration=None, code=None):
        print(f"\n[{name}] exit={code} stop={duration and round(duration, 1)}s "
              f"polls={ {f: dict(c) for f, c in polls.summary().items()} }")

    # ------------------------------------------------------------------ tests

    def test_1_stop_while_fetching_finishes_the_poll(self):
        """SIGTERM khi đang chờ GBFS (10 giây): lần polling chạy xong rồi mới dừng."""
        self.set_mock(delay_seconds=10)
        self.start_poller()
        self.wait_requests(2)   # cả hai feed đang chờ

        duration, code = self.graceful_stop()
        polls = Polls(self.since)
        self.report("stop khi đang fetch", polls, duration, code)

        self.assertEqual(code, 0)
        self.assertLess(duration, GRACE_SECONDS)
        self.assertEqual(polls.violations(), [])
        for feed in FEEDS:
            self.assertEqual(polls.summary()[feed], Counter(complete=1))

    def test_2_stop_during_retries_does_not_wait_for_them(self):
        """GBFS treo 45 giây mỗi request (quá timeout 30 giây của poller): đủ 3 lần
        thử mất ~93 giây. Nhận SIGTERM thì poller bỏ các lần retry còn lại và
        dừng sau lần thử đang chạy."""
        self.set_mock(delay_seconds=45, fail_first=4)
        self.start_poller()
        self.wait_requests(2)

        duration, code = self.graceful_stop()
        polls = Polls(self.since)
        self.report("dừng khi đang retry", polls, duration, code)

        # Không có dữ liệu nào được ghi dở: chỉ mất lần polling chưa lấy xong.
        self.assertEqual(polls.violations(), [])
        self.assertEqual(sum(sum(c.values()) for c in polls.summary().values()), 0)
        self.assertEqual(
            code, 0,
            f"Poller không dừng êm: bị Docker kill sau {duration:.0f}s "
            f"(exit {code}) vì vẫn đang retry GBFS",
        )
        # Một lần thử (timeout 30 giây) cộng thời gian dừng, không phải ~93 giây.
        self.assertLess(duration, 45, "Poller vẫn chờ hết các lần retry GBFS")

    def test_3_kafka_down_during_stop_keeps_the_poll_for_replay(self):
        """Kafka treo khi đang gửi: lần polling phải được giữ lại rồi replay được."""
        self.set_mock()
        compose("pause", "kafka")
        try:
            self.start_poller()
            self.wait_requests(2)
            time.sleep(3)   # đã fetch xong, đang chờ Kafka xác nhận
            duration, code = self.graceful_stop()
        finally:
            compose("unpause", "kafka")
            wait_healthy("kafka")

        polls = Polls(self.since)
        self.report("Kafka treo lúc dừng", polls, duration, code)
        self.assertEqual(polls.violations(), [])
        kept = [
            (feed, p) for feed in FEEDS for p in polls.polls[feed]
            if polls.state(feed, p) == "kept"
        ]
        self.assertEqual(len(kept), 2)

        # Khởi động lại đúng container đó (không tạo mới) và replay.
        subprocess.run(["docker", "start", container_id()], check=True, capture_output=True)
        for feed, poll_id in kept:
            compose("exec", "-T", POLLER, "python", "-m", "ingestion.gbfs.poller",
                    "--replay", f"/app/data/gbfs/{feed}/{poll_id}")
        compose("stop", POLLER)

        polls = Polls(self.since)
        self.report("sau replay", polls)
        self.assertEqual(polls.violations(), [])
        for feed, poll_id in kept:
            self.assertEqual(polls.state(feed, poll_id), "complete")

    def test_4_killed_after_raw_is_written_keeps_the_poll(self):
        """SIGKILL khi MinIO treo (đã gửi Kafka, đang upload raw)."""
        self.set_mock()
        compose("pause", "minio")
        try:
            self.start_poller()
            self.wait_requests(2)
            time.sleep(5)   # Kafka xong, đang chờ MinIO
            compose("kill", "-s", "SIGKILL", POLLER)
            code = exit_code()
        finally:
            compose("unpause", "minio")
            wait_healthy("minio")

        polls = Polls(self.since)
        self.report("SIGKILL khi đang upload raw", polls, code=code)
        self.assertEqual(code, 137)
        self.assertEqual(polls.violations(), [])
        for feed in FEEDS:
            self.assertEqual(polls.summary()[feed], Counter(kept=1))

    def test_5_kept_polls_survive_a_recreated_container(self):
        """Thư mục giữ lại để replay nằm trên volume gbfs-spool,
        nên không mất khi container bị tạo lại (up --force-recreate)."""
        self.set_mock()
        before = Polls(datetime(2000, 1, 1, tzinfo=timezone.utc))
        kept_before = sum(c["kept"] for c in before.summary().values())

        compose("up", "-d", "--no-deps", "--force-recreate", POLLER)
        self.wait_requests(2)
        compose("stop", POLLER)

        after = Polls(datetime(2000, 1, 1, tzinfo=timezone.utc))
        self.report("sau khi tạo lại container", after)
        lost = after.violations()
        print(f"  kept trước khi tạo lại: {kept_before}, LOST sau: {len(lost)}")
        self.assertEqual(lost, [], "Thư mục giữ lại để replay bị mất khi tạo lại container")

        # Replay ngay trong container mới: bản giữ lại vẫn gửi bù được.
        kept = [
            (feed, poll_id) for feed in FEEDS for poll_id in after.polls[feed]
            if after.state(feed, poll_id) == "kept"
        ]
        self.assertGreaterEqual(len(kept), kept_before)
        subprocess.run(["docker", "start", container_id()], check=True, capture_output=True)
        for feed, poll_id in kept:
            compose("exec", "-T", POLLER, "python", "-m", "ingestion.gbfs.poller",
                    "--replay", f"/app/data/gbfs/{feed}/{poll_id}")
        compose("stop", POLLER)

        replayed = Polls(datetime(2000, 1, 1, tzinfo=timezone.utc))
        self.report("replay sau khi tạo lại container", replayed)
        self.assertEqual(replayed.violations(), [])
        for feed, poll_id in kept:
            self.assertEqual(replayed.state(feed, poll_id), "complete")


if __name__ == "__main__":
    unittest.main()
