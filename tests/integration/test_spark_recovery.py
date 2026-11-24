"""STR-12: Spark crash / restart, replay and idempotent Iceberg writes.

Covers the scenarios of docs/contracts.md section 2.4 with real faults on a real
stack (Kafka, MinIO, Iceberg REST, Spark), for Bronze (Kafka -> Iceberg append)
and Silver (Bronze -> Iceberg MERGE):

  1. 100 messages -> 100 observations in Bronze and Silver.
  2. Crash after the Iceberg commit, before the checkpoint commit: the batch's
     commits/<n> file is removed (exactly what such a crash leaves behind) and the
     job restarts; the batch is replayed without duplicate rows.
  3. Crash before / during the Iceberg write: the Spark driver is SIGKILLed in the
     middle of a multi-batch run (twice), then restarted; nothing lost or doubled.
  4. Restart after the checkpoint is complete: no batch is reprocessed.
  5. MinIO unavailable (docker pause) while the job runs: how Spark fails, and
     that the next run recovers everything.

After every scenario: Bronze rows per partition == Kafka end offsets, no
(topic, partition, offset) twice, Silver rows == distinct keys == keys produced.

Run from the repository root on a separate Compose project (host Python needs
confluent-kafka):

    export COMPOSE_PROJECT_NAME=citibike-itest
    export MINIO_API_PORT=59000 MINIO_CONSOLE_PORT=59001 ICEBERG_REST_PORT=58181 \\
           SPARK_UI_PORT=58081 SPARK_WORKER_UI_PORT=58082   # if the defaults are taken
    docker compose --profile pipeline up -d --wait kafka kafka-init minio minio-init \\
        iceberg-rest catalog-init spark-master
    python -m unittest tests.integration.test_spark_recovery -v
    docker compose --profile pipeline down -v
"""
import json
import os
import subprocess
import time
import unittest
from datetime import datetime, timedelta, timezone

from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
RUN = str(int(time.time()))
TOPIC = "gbfs.station_status.str12test"
BRONZE = "lakehouse.bronze.gbfs_status_str12test"
SILVER = "lakehouse.silver.gbfs_status_str12test"
CKPT = f"s3a://checkpoints/str12-{RUN}"
BRONZE_CKPT = f"{CKPT}/status"
SILVER_CKPT = f"{CKPT}/silver_status"
JOBS = "/opt/project/streaming/jobs"
HELPER = "/opt/project/tests/integration/str12_helper.py"
JOB_ENV = {"GBFS_STATUS_TOPIC": TOPIC, "BRONZE_STATUS_TABLE": BRONZE,
           "SILVER_STATUS_TABLE": SILVER}


def log(message):
    print(f"    {message}", flush=True)


def compose(*args, check=True, timeout=None):
    return subprocess.run(["docker", "compose", "--profile", "pipeline", *args],
                          check=check, capture_output=True, text=True, timeout=timeout)


def spark_cmd(script, *args, env=None):
    env_args = []
    for key, value in {**JOB_ENV, **(env or {})}.items():
        env_args += ["-e", f"{key}={value}"]
    return ["docker", "compose", "exec", "-T", *env_args, "spark-master",
            "/opt/spark/bin/spark-submit", script, *args]


def spark(script, *args, env=None, timeout=600):
    run = subprocess.run(spark_cmd(script, *args, env=env), capture_output=True, text=True,
                         timeout=timeout)
    return run.returncode, run.stdout + run.stderr


def helper(*args):
    code, out = spark(HELPER, *args)
    if code != 0:
        raise RuntimeError(f"helper {args} failed:\n{out[-3000:]}")
    for line in out.splitlines():
        if line.startswith("STR12_RESULT "):
            return json.loads(line[len("STR12_RESULT "):])
    return None


def bronze_job(env=None):
    return [f"{JOBS}/gbfs_history.py", "--sink", "bronze", "--feed", "status",
            "--available-now", "--checkpoint-dir", CKPT], env


def silver_job():
    return [f"{JOBS}/silver_status.py", "--available-now", "--checkpoint-dir", CKPT], None


def run_ok(job, timeout=600):
    (script, *args), env = job
    code, out = spark(script, *args, env=env, timeout=timeout)
    if code != 0:
        raise AssertionError(f"{script} failed (exit {code}):\n{out[-3000:]}")
    return out


def kill_driver(script_name):
    """SIGKILL the Spark driver (JVM and Python) running `script_name`."""
    code = (
        "import os, signal\n"
        "me = os.getpid()\n"
        "for pid in filter(str.isdigit, os.listdir('/proc')):\n"
        "    try:\n"
        "        cmd = open(f'/proc/{pid}/cmdline', 'rb').read()\n"
        "    except OSError:\n"
        "        continue\n"
        f"    if int(pid) != me and b'{script_name}' in cmd and b'python3 -c' not in cmd:\n"
        "        os.kill(int(pid), signal.SIGKILL)\n"
    )
    compose("exec", "-T", "spark-master", "python3", "-c", code)


def end_offsets():
    consumer = Consumer({"bootstrap.servers": KAFKA, "group.id": f"str12-{RUN}"})
    try:
        partitions = consumer.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions
        return {str(p): consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)[1]
                for p in partitions}
    finally:
        consumer.close()


class SparkRecoveryTest(unittest.TestCase):
    produced_keys = 0
    base = datetime.now(timezone.utc).replace(microsecond=0)

    @classmethod
    def setUpClass(cls):
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

    def run_until_progress_then_kill(self, job, batches_done, script_name):
        """Start the job, SIGKILL its driver after it finished `batches_done` batches,
        i.e. while the next batch is being planned, read or written."""
        (script, *args), env = job
        process = subprocess.Popen(spark_cmd(script, *args, env=env), text=True,
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        done = 0
        for line in process.stdout:
            if line.startswith("[progress] gbfs_status_bronze batch="):
                done += 1
                if done >= batches_done:
                    kill_driver(script_name)
                    break
        process.stdout.read()
        process.stdout.close()
        return process.wait(timeout=120)

    # ------------------------------------------------------------------ tests

    def test_1_hundred_messages_become_hundred_observations(self):
        self.produce(polls=10, stations=10, tag="normal")
        run_ok(bronze_job())
        run_ok(silver_job())
        result = self.assert_integrity("100 messages")
        self.assertEqual(result["bronze"]["rows"], 100)
        self.assertEqual(result["silver"]["rows"], 100)

    def test_2_crash_after_iceberg_commit_before_checkpoint_commit(self):
        self.produce(polls=2, stations=50, tag="after-commit")
        run_ok(bronze_job())
        run_ok(silver_job())
        before = self.assert_integrity("before")

        for name, directory, job in (("bronze", BRONZE_CKPT, bronze_job()),
                                     ("silver", SILVER_CKPT, silver_job())):
            batches = helper("batches", directory)
            last = batches["commits"][-1]
            self.assertEqual(batches["offsets"][-1], last)
            # The Iceberg snapshot of batch `last` exists, its checkpoint commit does
            # not: the state a crash right after the Iceberg commit leaves behind.
            helper("rm", f"{directory}/commits/{last}")
            log(f"{name}: removed commits/{last} -> replaying batch {last}")
            run_ok(job)
            self.assertIn(last, helper("batches", directory)["commits"])

        after = self.assert_integrity("after replay")
        # Bronze: Iceberg recognises the replayed epoch and does not commit again.
        self.assertEqual(after["bronze"]["snapshots"], before["bronze"]["snapshots"])

    def test_3_driver_killed_mid_run_then_restarted(self):
        self.produce(polls=20, stations=1000, tag="sigkill")     # 20 000 messages
        small_batches = {"MAX_OFFSETS_PER_TRIGGER": "2000"}       # ~10 micro-batches
        total = sum(end_offsets().values())

        for kill_after in (1, 4):
            before = self.stats()["bronze"]["rows"]
            job = bronze_job(small_batches)
            code = self.run_until_progress_then_kill(job, kill_after, "gbfs_history.py")
            batches = helper("batches", BRONZE_CKPT)
            bronze = self.stats()["bronze"]
            planned_not_committed = sorted(set(batches["offsets"]) - set(batches["commits"]))
            log(f"killed after {kill_after} batch(es): exit {code}, bronze rows "
                f"{before} -> {bronze['rows']} of {total}, dup {bronze['duplicate_offsets']}, "
                f"planned but not committed: {planned_not_committed}")
            self.assertNotEqual(code, 0)
            self.assertEqual(bronze["duplicate_offsets"], 0)
            self.assertLess(bronze["rows"], total)

        run_ok(bronze_job(small_batches))

        # Silver: kill while its MERGE batch runs, then restart.
        process = subprocess.Popen(spark_cmd(*silver_job()[0]), text=True,
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in process.stdout:
            if line.startswith("started silver"):
                time.sleep(8)
                kill_driver("silver_status.py")
                break
        process.stdout.read()
        process.stdout.close()
        log(f"silver killed during MERGE: exit {process.wait(timeout=120)}")
        run_ok(silver_job())
        self.assert_integrity("after kills and restart")

    def test_4_restart_after_completed_checkpoint_reprocesses_nothing(self):
        before = self.stats()
        bronze_batches = helper("batches", BRONZE_CKPT)
        run_ok(bronze_job())
        run_ok(silver_job())
        after = self.assert_integrity("restart, nothing new")
        self.assertEqual(after["bronze"]["snapshots"], before["bronze"]["snapshots"])
        self.assertEqual(helper("batches", BRONZE_CKPT), bronze_batches)

    def test_5_minio_unavailable_then_back(self):
        self.produce(polls=2, stations=500, tag="minio-outage")
        compose("pause", "minio")
        started = time.monotonic()
        try:
            (script, *args), env = bronze_job()
            try:
                code, out = spark(script, *args, env=env, timeout=240)
                outcome = f"exit {code} after {time.monotonic() - started:.0f}s"
                errors = [l.strip() for l in out.splitlines()
                          if "Exception" in l and not l.strip().startswith("at ")][:2]
            except subprocess.TimeoutExpired:
                kill_driver("gbfs_history.py")
                code, errors = None, []
                outcome = "still waiting/retrying after 240s, killed"
            log(f"MinIO paused: {outcome}; {errors}")
        finally:
            compose("unpause", "minio")
        self.assertNotEqual(code, 0)

        for _ in range(60):   # MinIO healthy again
            if compose("ps", "--format", "{{.Health}}", "minio").stdout.strip() == "healthy":
                break
            time.sleep(2)
        run_ok(bronze_job())
        run_ok(silver_job())
        self.assert_integrity("after MinIO is back")


if __name__ == "__main__":
    unittest.main()
