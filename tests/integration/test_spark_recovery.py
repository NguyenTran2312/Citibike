"""STR-12: Spark crash / restart, replay and idempotent Iceberg writes.

Covers the scenarios of docs/contracts.md section 2.4 with real faults on a real
stack (Kafka, MinIO, Iceberg REST, Spark), for Bronze (Kafka -> Iceberg append)
and Silver (Bronze -> Iceberg MERGE):

  1. 100 messages -> 100 observations in Bronze and Silver.
  2. Crash after the Iceberg commit, before the checkpoint commit: the batch's
     commits/<n> file is removed (exactly what such a crash leaves behind; the
     test checks it is gone) and the job restarts; replayed without duplicates.
  3. Crash before / during the Iceberg write: the Bronze driver is SIGKILLed when
     a batch starts its append (first batch, then fourth batch) and the Silver
     driver when a batch starts its MERGE; the checkpoint must show a planned but
     uncommitted batch each time (the kill may land just before or just after the
     Iceberg commit: both must recover without loss or duplicates).
  4. Restart after the checkpoint is complete: neither Bronze nor Silver
     reprocesses a batch or commits a snapshot.
  5. MinIO unavailable in the middle of a run (docker pause after the first
     batch): how Spark fails, and that the next run recovers everything.
  6. Bronze snapshots the Silver checkpoint still needs are expired: the Silver
     query cannot continue; `silver_status.py --rebuild` with a new checkpoint
     restores Silver and keeps streaming.
  7. Crash after the Iceberg commit, and the snapshot of that commit is expired
     before the restart (compaction + expire_snapshots): the replayed batch still
     adds nothing, because Bronze recognises messages by their Kafka position in
     the data, not by snapshot metadata. (The same check covers a checkpoint left
     by the former Iceberg streaming sink; that case is not run here.)

After every scenario: Bronze rows per partition == Kafka end offsets, no
(topic, partition, offset) twice, Silver rows == distinct keys == keys produced.

Safety: it pauses MinIO and kills Spark drivers, so it only runs when
COMPOSE_PROJECT_NAME ends with "-itest"; topic, tables and checkpoints carry a
per-run suffix, and only drivers started with this run's checkpoint are killed.

Run from the repository root (host Python needs confluent-kafka):

    export COMPOSE_PROJECT_NAME=citibike-itest
    export MINIO_API_PORT=59000 MINIO_CONSOLE_PORT=59001 ICEBERG_REST_PORT=58181 \\
           SPARK_UI_PORT=58081 SPARK_WORKER_UI_PORT=58082   # if the defaults are taken
    docker compose --profile pipeline up -d --wait kafka kafka-init minio minio-init \\
        iceberg-rest catalog-init spark-master
    python -m unittest tests.integration.test_spark_recovery -v
    docker compose --profile pipeline down -v

Full Spark output of every step is kept in the directory printed at start.
"""
import json
import os
import queue
import subprocess
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

# 127.0.0.1, not localhost: on some hosts localhost resolves to IPv6 first and the
# broker's advertised EXTERNAL listener is IPv4.
KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
PROJECT = os.getenv("COMPOSE_PROJECT_NAME", "")
RUN = str(int(time.time()))
TOPIC = f"gbfs.station_status.str12test.{RUN}"
BRONZE = f"lakehouse.bronze.gbfs_status_str12test_{RUN}"
SILVER = f"lakehouse.silver.gbfs_status_str12test_{RUN}"
CKPT = f"s3a://checkpoints/str12-{RUN}"
REBUILD_CKPT = f"s3a://checkpoints/str12-{RUN}-rebuild"
BRONZE_CKPT = f"{CKPT}/status"
JOBS = "/opt/project/streaming/jobs"
HELPER = "/opt/project/tests/integration/str12_helper.py"
JOB_ENV = {"GBFS_STATUS_TOPIC": TOPIC, "BRONZE_STATUS_TABLE": BRONZE,
           "SILVER_STATUS_TABLE": SILVER}
LOG_DIR = Path(tempfile.mkdtemp(prefix="str12-"))
_log_count = 0


def log(message):
    print(f"    {message}", flush=True)


def save_log(name, text):
    global _log_count
    _log_count += 1
    path = LOG_DIR / f"{_log_count:02d}-{name}.log"
    path.write_text(text)
    return path


def compose(*args, check=True, timeout=None):
    return subprocess.run(["docker", "compose", "--profile", "pipeline", *args],
                          check=check, capture_output=True, text=True, timeout=timeout)


def spark_cmd(script, *args, env=None):
    env_args = []
    for key, value in {**JOB_ENV, **(env or {})}.items():
        env_args += ["-e", f"{key}={value}"]
    return ["docker", "compose", "exec", "-T", *env_args, "spark-master",
            "/opt/spark/bin/spark-submit", script, *args]


def spark(name, script, *args, env=None, timeout=600):
    run = subprocess.run(spark_cmd(script, *args, env=env), capture_output=True, text=True,
                         timeout=timeout)
    out = run.stdout + run.stderr
    return run.returncode, out, save_log(name, out)


def helper(*args):
    code, out, path = spark(f"helper-{args[0]}", HELPER, *args)
    if code != 0:
        raise RuntimeError(f"helper {args} failed (log {path}):\n{out[-2000:]}")
    for line in out.splitlines():
        if line.startswith("STR12_RESULT "):
            return json.loads(line[len("STR12_RESULT "):])
    return None


def bronze_job(env=None):
    return [f"{JOBS}/gbfs_history.py", "--sink", "bronze", "--feed", "status",
            "--available-now", "--checkpoint-dir", CKPT], env


def silver_job(root=CKPT, rebuild=False):
    return ([f"{JOBS}/silver_status.py", "--available-now", "--checkpoint-dir", root]
            + (["--rebuild"] if rebuild else [])), None


def run_ok(name, job, timeout=600):
    (script, *args), env = job
    code, out, path = spark(name, script, *args, env=env, timeout=timeout)
    if code != 0:
        raise AssertionError(f"{name} failed (exit {code}, log {path}):\n{out[-2000:]}")
    return out


def kill_driver():
    """SIGKILL the Spark driver (JVM and Python) of this run only: its command line
    carries this run's checkpoint path."""
    code = (
        "import os, signal\n"
        "me = os.getpid()\n"
        "for pid in filter(str.isdigit, os.listdir('/proc')):\n"
        "    try:\n"
        "        cmd = open(f'/proc/{pid}/cmdline', 'rb').read()\n"
        "    except OSError:\n"
        "        continue\n"
        f"    if int(pid) != me and b'{CKPT}' in cmd:\n"
        "        os.kill(int(pid), signal.SIGKILL)\n"
        "        print(pid)\n"
    )
    killed = compose("exec", "-T", "spark-master", "python3", "-c", code).stdout.split()
    if not killed:
        raise AssertionError("no driver of this run was running")
    return killed


class Output:
    """Lines of a running process, read by a thread so every wait has a deadline."""

    def __init__(self, process):
        self.process, self.lines, self._queue = process, [], queue.Queue()
        threading.Thread(target=self._pump, daemon=True).start()

    def _pump(self):
        with self.process.stdout:
            for line in self.process.stdout:
                self._queue.put(line)
        self._queue.put(None)

    def until(self, match, timeout):
        """True when a line satisfies `match`, False when the output ends first,
        None when neither happened within `timeout` seconds."""
        deadline = time.monotonic() + timeout
        while True:
            try:
                line = self._queue.get(timeout=max(0.1, deadline - time.monotonic()))
            except queue.Empty:
                if time.monotonic() >= deadline:
                    return None
                continue
            if line is None:
                return False
            self.lines.append(line)
            if match(line):
                return True

    def text(self):
        return "".join(self.lines)


def start_and_kill_on(name, job, trigger, after=1, timeout=600):
    """Start the job, SIGKILL its driver once a line containing `trigger` was
    printed `after` times. Fails if that does not happen within `timeout` s."""
    (script, *args), env = job
    process = subprocess.Popen(spark_cmd(script, *args, env=env), text=True,
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output, seen = Output(process), [0]

    def nth_trigger(line):
        seen[0] += trigger in line
        return seen[0] >= after

    found = output.until(nth_trigger, timeout)
    if found:
        kill_driver()
    elif found is None:
        process.kill()
    output.until(lambda line: False, 120)
    code = process.wait(timeout=120)
    path = save_log(name, output.text())
    if not found:
        raise AssertionError(f"{name}: '{trigger}' x{after} not printed "
                             f"({'timeout' if found is None else f'exit {code}'}, log {path})")
    return code, path


def end_offsets():
    consumer = Consumer({"bootstrap.servers": KAFKA, "group.id": f"str12-{RUN}"})
    try:
        partitions = consumer.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions
        return {str(p): consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)[1]
                for p in partitions}
    finally:
        consumer.close()


def uncommitted(directory):
    batches = helper("batches", directory)
    return sorted(set(batches["offsets"]) - set(batches["commits"]))


class SparkRecoveryTest(unittest.TestCase):
    produced_keys = 0
    base = datetime.now(timezone.utc).replace(microsecond=0)

    @classmethod
    def setUpClass(cls):
        if not PROJECT.endswith("-itest"):
            raise unittest.SkipTest(
                "pauses MinIO and kills Spark drivers: set COMPOSE_PROJECT_NAME=<name>-itest "
                "to run it on a throw-away Compose project")
        print(f"\n    logs: {LOG_DIR}", flush=True)
        admin = AdminClient({"bootstrap.servers": KAFKA})
        admin.create_topics([NewTopic(TOPIC, num_partitions=3, replication_factor=1)])[TOPIC] \
            .result(timeout=30)
        cls.producer = Producer({"bootstrap.servers": KAFKA, "enable.idempotence": True,
                                 "acks": "all"})

    @classmethod
    def tearDownClass(cls):
        compose("unpause", "minio", check=False)
        AdminClient({"bootstrap.servers": KAFKA}).delete_topics([TOPIC])
        try:
            helper("drop", BRONZE, SILVER)
            helper("rm", CKPT)
            helper("rm", REBUILD_CKPT)
        except RuntimeError as error:
            print(f"cleanup failed: {error}")

    # ---------------------------------------------------------------- helpers

    def produce(self, polls, stations, tag):
        """`polls` x `stations` valid, distinct (station_id, poll_id) messages."""
        for p in range(polls):
            collected = self.base + timedelta(minutes=len(tag) + p)
            for s in range(stations):
                station = f"st-{s}"
                message = {
                    "schema_version": 1, "poll_id": f"{tag}-{p}", "station_id": station,
                    "num_bikes_available": s % 10, "num_docks_available": 10,
                    "is_installed": True, "is_renting": True, "is_returning": True,
                    "last_reported": int(collected.timestamp()) - 30,
                    "last_updated": int(collected.timestamp()),
                    "collected_at": collected.isoformat().replace("+00:00", "Z"),
                }
                self.producer.produce(TOPIC, key=station, value=json.dumps(message))
            self.producer.poll(0)
        self.assertEqual(self.producer.flush(60), 0)
        SparkRecoveryTest.produced_keys += polls * stations

    def stats(self):
        return helper("stats", BRONZE, SILVER)

    def assert_integrity(self, step, silver=True):
        """Bronze == Kafka end offsets per partition, no duplicates, Silver == keys."""
        offsets = end_offsets()
        result = self.stats()
        bronze = result["bronze"]
        log(f"[{step}] kafka={offsets} bronze={bronze['per_partition']} "
            f"(rows {bronze['rows']}, dup {bronze['duplicate_offsets']}, "
            f"snapshots {bronze['snapshots']})"
            + (f" silver rows {result['silver']['rows']} / keys {self.produced_keys}"
               if silver else ""))
        self.assertEqual(bronze["duplicate_offsets"], 0)
        self.assertEqual({p: bronze["per_partition"].get(p, 0) for p in offsets}, offsets)
        if silver:
            self.assertEqual(result["silver"]["rows"], self.produced_keys)
            self.assertEqual(result["silver"]["distinct_keys"], self.produced_keys)
        return result

    # ------------------------------------------------------------------ tests

    def test_1_hundred_messages_become_hundred_observations(self):
        self.produce(polls=10, stations=10, tag="normal")
        run_ok("t1-bronze", bronze_job())
        run_ok("t1-silver", silver_job())
        result = self.assert_integrity("100 messages")
        self.assertEqual(result["bronze"]["rows"], 100)
        self.assertEqual(result["silver"]["rows"], 100)

    def test_2_crash_after_iceberg_commit_before_checkpoint_commit(self):
        self.produce(polls=2, stations=50, tag="after-commit")
        run_ok("t2-bronze", bronze_job())
        run_ok("t2-silver", silver_job())
        before = self.assert_integrity("before")

        for name, directory, job in (("bronze", BRONZE_CKPT, bronze_job()),
                                     ("silver", f"{CKPT}/silver_status", silver_job())):
            batches = helper("batches", directory)
            last = batches["commits"][-1]
            self.assertEqual(batches["offsets"][-1], last)
            # The Iceberg snapshot of batch `last` exists, its checkpoint commit does
            # not: the state a crash right after the Iceberg commit leaves behind.
            self.assertTrue(helper("rm", f"{directory}/commits/{last}")["deleted"])
            self.assertEqual(uncommitted(directory), [last])
            log(f"{name}: commits/{last} removed -> replaying batch {last}")
            run_ok(f"t2-{name}-replay", job)
            self.assertEqual(uncommitted(directory), [])

        after = self.assert_integrity("after replay")
        # Bronze: the replayed batch finds its messages already there, appends nothing.
        self.assertEqual(after["bronze"]["snapshots"], before["bronze"]["snapshots"])

    def test_3_drivers_killed_mid_run_then_restarted(self):
        self.produce(polls=20, stations=1000, tag="sigkill")     # 20 000 messages
        small_batches = {"MAX_OFFSETS_PER_TRIGGER": "2000"}       # ~10 micro-batches
        total = sum(end_offsets().values())

        for kill_after in (1, 4):
            before = self.stats()["bronze"]["rows"]
            code, path = start_and_kill_on(f"t3-bronze-kill-{kill_after}",
                                           bronze_job(small_batches),
                                           "appending", kill_after)   # "batch n: appending N messages"
            pending = uncommitted(BRONZE_CKPT)
            bronze = self.stats()["bronze"]
            log(f"bronze killed in batch append #{kill_after}: exit {code}, rows "
                f"{before} -> {bronze['rows']} of {total}, dup {bronze['duplicate_offsets']}, "
                f"planned but not committed {pending} (log {path})")
            self.assertNotEqual(code, 0)
            self.assertTrue(pending, "the Bronze batch was committed before the kill")
            self.assertEqual(bronze["duplicate_offsets"], 0)
            if kill_after > 1:   # the batches before the killed one were committed
                self.assertLess(before, bronze["rows"])
            self.assertLess(bronze["rows"], total)
        run_ok("t3-bronze-finish", bronze_job(small_batches))

        # Silver: kill when a batch announces its MERGE ("[silver] merge ...", printed
        # right before the SQL), then restart.
        code, path = start_and_kill_on("t3-silver-kill", silver_job(), "[silver] merge")
        pending = uncommitted(f"{CKPT}/silver_status")
        log(f"silver killed when its MERGE started: exit {code}, planned but not committed "
            f"{pending} (log {path})")
        self.assertNotEqual(code, 0)
        self.assertTrue(pending, "the Silver batch was committed before the kill")
        run_ok("t3-silver-finish", silver_job())
        self.assert_integrity("after kills and restart")

    def test_4_restart_after_completed_checkpoint_reprocesses_nothing(self):
        before = self.stats()
        bronze_batches = helper("batches", BRONZE_CKPT)
        silver_batches = helper("batches", f"{CKPT}/silver_status")
        run_ok("t4-bronze", bronze_job())
        run_ok("t4-silver", silver_job())
        after = self.assert_integrity("restart, nothing new")
        self.assertEqual(after["bronze"]["snapshots"], before["bronze"]["snapshots"])
        self.assertEqual(after["silver"]["snapshots"], before["silver"]["snapshots"])
        self.assertEqual(helper("batches", BRONZE_CKPT), bronze_batches)
        self.assertEqual(helper("batches", f"{CKPT}/silver_status"), silver_batches)

    def test_5_minio_lost_in_the_middle_of_a_run(self):
        self.produce(polls=10, stations=1000, tag="minio-outage")   # 10 000 messages
        before = self.stats()["bronze"]["rows"]
        total = sum(end_offsets().values())
        (script, *args), env = bronze_job({"MAX_OFFSETS_PER_TRIGGER": "2000"})
        process = subprocess.Popen(spark_cmd(script, *args, env=env), text=True,
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        output, paused_at = Output(process), None
        try:
            started = output.until(lambda l: l.startswith("[progress] gbfs_status_bronze"), 600)
            if started:
                compose("pause", "minio")       # after the first committed batch
                paused_at = time.monotonic()
                ended = output.until(lambda l: False, 300)
                if ended is None:
                    kill_driver()
                    output.until(lambda l: False, 120)
                    outcome = "still retrying 300s after the pause, killed"
                else:
                    outcome = f"ended {time.monotonic() - paused_at:.0f}s after the pause"
            else:
                process.kill()
                output.until(lambda l: False, 120)
                outcome = "no batch committed before the run ended"
            process.wait(timeout=120)
            if started:
                outcome = f"exit {process.returncode}, {outcome}"
        finally:
            compose("unpause", "minio")
        lines = output.lines
        path = save_log("t5-bronze-minio-paused", "".join(lines))
        errors = [l.strip() for l in "".join(lines).splitlines()
                  if ("Exception" in l or "Error" in l) and not l.strip().startswith("at ")][:2]
        log(f"MinIO paused mid-run: {outcome}; {errors} (log {path})")
        self.assertIsNotNone(paused_at, "the run ended before its first batch")
        self.assertNotEqual(process.returncode, 0)

        for _ in range(60):   # MinIO healthy again
            if compose("ps", "--format", "{{.Health}}", "minio").stdout.strip() == "healthy":
                break
            time.sleep(2)
        mid = self.stats()["bronze"]["rows"]
        log(f"bronze rows before the run {before}, when MinIO came back {mid}, Kafka {total}")
        self.assertLess(before, mid)       # batches before the pause were committed
        self.assertLess(mid, total)        # the outage stopped the run part way
        run_ok("t5-bronze-after", bronze_job())
        run_ok("t5-silver-after", silver_job())
        self.assert_integrity("after MinIO is back")

    def test_6_expired_bronze_snapshots_then_silver_rebuild(self):
        self.produce(polls=2, stations=100, tag="expire-a")
        run_ok("t6-bronze-a", bronze_job())
        run_ok("t6-silver-a", silver_job())
        self.produce(polls=2, stations=100, tag="expire-b")
        run_ok("t6-bronze-b", bronze_job())

        # Silver has not read "expire-b" yet; expire every Bronze snapshot but the
        # current one, including those its checkpoint points at.
        expired = helper("expire", BRONZE)
        log(f"expired Bronze snapshots: {expired}")
        self.assertEqual(expired["snapshots"], 1)
        code, _, path = spark("t6-silver-old-checkpoint", *silver_job()[0])
        log(f"Silver with the old checkpoint after expiry: exit {code} (log {path})")
        self.assertNotEqual(code, 0)

        run_ok("t6-silver-rebuild", silver_job(REBUILD_CKPT, rebuild=True))
        self.assert_integrity("after rebuild")

        # The new checkpoint keeps streaming later Bronze commits.
        self.produce(polls=1, stations=100, tag="expire-c")
        run_ok("t6-bronze-c", bronze_job())
        run_ok("t6-silver-c", silver_job(REBUILD_CKPT))
        self.assert_integrity("new checkpoint continues")

    def test_7_replay_after_the_commit_snapshot_was_expired(self):
        self.produce(polls=2, stations=50, tag="expired-replay")
        run_ok("t7-bronze", bronze_job())
        before = self.assert_integrity("before", silver=False)

        batches = helper("batches", BRONZE_CKPT)
        last = batches["commits"][-1]
        # The run appended this scenario's messages in its last batch, so the current
        # snapshot is the one batch `last` committed.
        batch_snapshot = helper("snapshot-ids", BRONZE)["current"]
        self.assertTrue(helper("rm", f"{BRONZE_CKPT}/commits/{last}")["deleted"])
        self.assertEqual(uncommitted(BRONZE_CKPT), [last])
        # Another commit becomes current (compaction: same rows, new files), then
        # every older snapshot is expired, including the one batch `last` made.
        compacted = helper("compact", BRONZE)
        expired = helper("expire", BRONZE)
        snapshot_ids = helper("snapshot-ids", BRONZE)
        log(f"batch {last} (snapshot {batch_snapshot}) uncommitted in the checkpoint; "
            f"compacted {compacted}, expired {expired}, left {snapshot_ids}")
        self.assertGreater(compacted["rewritten"], 0)
        self.assertEqual(expired["snapshots"], 1)
        self.assertNotEqual(snapshot_ids["current"], batch_snapshot)
        self.assertNotIn(batch_snapshot, snapshot_ids["ids"])

        out = run_ok("t7-bronze-replay", bronze_job())
        self.assertEqual(uncommitted(BRONZE_CKPT), [])
        self.assertIn("already in Bronze, skipped", out)
        after = self.assert_integrity("replay after expiry", silver=False)
        self.assertEqual(after["bronze"]["rows"], before["bronze"]["rows"])
        self.assertEqual(after["bronze"]["snapshots"], 1)   # nothing appended


if __name__ == "__main__":
    unittest.main()
