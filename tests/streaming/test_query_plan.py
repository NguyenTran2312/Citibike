"""STR-11: the streaming application starts one query per feed and layer, each with
its own checkpoint, and refuses to start when a checkpoint is missing or shared."""
import unittest

try:
    import pyspark  # noqa: F401
except ImportError:  # pragma: no cover - pyspark is only in the Spark image
    pyspark = None

if pyspark is not None:
    from streaming.jobs.gbfs_history import (MAX_OFFSETS_PER_TRIGGER, TRIGGER_INTERVAL,
                                             check_plan, plan_queries)

BRONZE = {"status": "s3a://checkpoints/status-history/",
          "information": "s3a://checkpoints/information-history/"}
SILVER = {"status": "s3a://checkpoints/silver-status/",
          "information": "s3a://checkpoints/silver-information/"}
FEEDS = ["status", "information"]


@unittest.skipIf(pyspark is None, "pyspark not installed")
class QueryPlanTest(unittest.TestCase):
    def plan(self, sink="bronze", feeds=FEEDS, checkpoint_dir=None, available_now=False,
             with_silver=True, bronze=None, silver=None):
        return plan_queries(sink, feeds, checkpoint_dir, available_now, with_silver,
                            bronze or BRONZE, silver or SILVER)

    def test_service_runs_four_queries_with_their_own_checkpoints(self):
        plan = self.plan()
        self.assertEqual([(q.name, q.checkpoint) for q in plan], [
            ("gbfs_status_bronze", BRONZE["status"]),
            ("gbfs_information_bronze", BRONZE["information"]),
            ("gbfs_status_silver", SILVER["status"]),
            ("gbfs_information_silver", SILVER["information"]),
        ])
        check_plan(plan)  # no error

    def test_checkpoint_dir_gives_each_query_a_subdirectory(self):
        plan = self.plan(checkpoint_dir="s3a://checkpoints/test/")
        self.assertEqual([q.checkpoint for q in plan], [
            "s3a://checkpoints/test/status", "s3a://checkpoints/test/information",
            "s3a://checkpoints/test/silver_status", "s3a://checkpoints/test/silver_information",
        ])
        check_plan(plan)

    def test_available_now_and_no_silver_start_bronze_only(self):
        for plan in (self.plan(available_now=True), self.plan(with_silver=False)):
            self.assertEqual([q.kind for q in plan], ["bronze", "bronze"])

    def test_console_may_run_without_a_checkpoint(self):
        plan = self.plan(sink="console")
        self.assertEqual([(q.kind, q.checkpoint) for q in plan],
                         [("console", None), ("console", None)])
        check_plan(plan)

    def test_missing_checkpoint_is_refused(self):
        plan = self.plan(silver={"status": None, "information": SILVER["information"]})
        with self.assertRaisesRegex(SystemExit, "no checkpoint for gbfs_status_silver"):
            check_plan(plan)

    def test_shared_checkpoint_is_refused(self):
        plan = self.plan(bronze={"status": BRONZE["status"],
                                 "information": BRONZE["status"].rstrip("/")})
        with self.assertRaisesRegex(SystemExit, "share a checkpoint"):
            check_plan(plan)

    def test_nested_checkpoint_is_refused(self):
        plan = self.plan(silver={"status": BRONZE["status"] + "silver",
                                 "information": SILVER["information"]})
        with self.assertRaisesRegex(SystemExit, "gbfs_status_bronze and gbfs_status_silver"):
            check_plan(plan)

    def test_defaults_match_the_contract(self):
        # contract 2.3.3: Spark micro-batch trigger 5 minutes
        self.assertEqual(TRIGGER_INTERVAL, "5 minutes")
        # must exceed ~12600 status messages per 5-minute trigger
        self.assertGreater(MAX_OFFSETS_PER_TRIGGER, 2519 * 5)


if __name__ == "__main__":
    unittest.main()
