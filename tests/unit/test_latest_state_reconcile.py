"""Gate 7 must fail unless PostgreSQL equals the latest Silver observation.

Runs tests/integration/latest_state_reconcile.sh (and through it
latest_state_compare.py) with the two database exports replaced by shell
commands (PG_EXPORT_CMD, SILVER_EXPORT_CMD), so no Docker stack is needed.
Each side is written in the format its real export produces: PostgreSQL
COPY CSV (t/f, to_char ...US"Z"), Trino CSV_HEADER (quoted, true/false,
to_iso8601).
"""
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "tests" / "integration" / "latest_state_reconcile.sh"
HEADER = ("station_id,poll_id,num_bikes_available,num_docks_available,is_installed,"
          "is_renting,is_returning,last_updated,last_reported,collected_at")

STATIONS = {
    "station-a": {"poll_id": "station_status-aaaa", "bikes": 5, "docks": 12,
                  "flags": (True, True, True), "updated": "2026-11-28T02:00:00.000000Z",
                  "reported": "2026-11-28T01:59:30.000000Z",
                  "collected": "2026-11-28T02:00:01.123456Z"},
    "station-b": {"poll_id": "station_status-aaaa", "bikes": 0, "docks": None,
                  "flags": (True, False, True), "updated": "2026-11-28T02:00:00.000000Z",
                  "reported": "2026-11-28T01:58:00.000000Z",
                  "collected": "2026-11-28T02:00:01.123456Z"},
}


def pg_csv(stations):
    lines = [HEADER]
    for sid, s in sorted(stations.items()):
        flags = ",".join("t" if f else "f" for f in s["flags"])
        docks = "" if s["docks"] is None else str(s["docks"])
        lines.append(f"{sid},{s['poll_id']},{s['bikes']},{docks},{flags},"
                     f"{s['updated']},{s['reported']},{s['collected']}")
    return "\n".join(lines) + "\n"


def trino_csv(stations):
    def q(value):
        return f'"{value}"'
    lines = [",".join(q(c) for c in HEADER.split(","))]
    for sid, s in sorted(stations.items()):
        flags = [q("true" if f else "false") for f in s["flags"]]
        docks = "" if s["docks"] is None else q(s["docks"])
        lines.append(",".join([q(sid), q(s["poll_id"]), q(s["bikes"]), docks, *flags,
                               q(s["updated"]), q(s["reported"]), q(s["collected"])]))
    return "\n".join(lines) + "\n"


def changed(station, **fields):
    stations = {k: dict(v) for k, v in STATIONS.items()}
    stations[station].update(fields)
    return stations


class LatestStateReconcileTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

    def run_gate(self, pg=None, silver=None, pg_cmd=None, silver_cmd=None):
        """pg / silver: file contents to export; *_cmd: a shell command instead."""
        env = dict(os.environ)
        for side, content, cmd in (("PG", pg, pg_cmd), ("SILVER", silver, silver_cmd)):
            if cmd is None:
                path = self.tmp / f"{side}.csv"
                path.write_text(content, encoding="utf-8")
                cmd = f"cat '{path}'"
            env[f"{side}_EXPORT_CMD"] = cmd
        result = subprocess.run(["bash", str(SCRIPT), str(self.tmp / "out")], env=env,
                                capture_output=True, text=True, timeout=60)
        return result.returncode, result.stdout + result.stderr

    def assert_fails(self, text, **kwargs):
        code, output = self.run_gate(**kwargs)
        self.assertNotEqual(code, 0, output)
        self.assertIn(text, output)
        self.assertNotIn("[PASS]", output)

    # --- matching data passes -------------------------------------------------
    def test_matching_latest_state_passes(self):
        code, output = self.run_gate(pg=pg_csv(STATIONS), silver=trino_csv(STATIONS))
        self.assertEqual(code, 0, output)
        self.assertIn("[PASS] 2 stations, every field equal", output)

    # --- a failed or empty query never passes ---------------------------------
    def test_postgres_query_failure_fails(self):
        self.assert_fails("PostgreSQL export failed with exit 17",
                          pg_cmd="echo 'connection refused' >&2; exit 17",
                          silver=trino_csv(STATIONS))

    def test_silver_query_failure_fails(self):
        self.assert_fails("Silver export failed with exit 19",
                          pg=pg_csv(STATIONS), silver_cmd="echo 'Query failed' >&2; exit 19")

    def test_both_query_failures_fail(self):
        code, output = self.run_gate(pg_cmd="exit 17", silver_cmd="exit 19")
        self.assertNotEqual(code, 0, output)
        self.assertIn("PostgreSQL export failed with exit 17", output)
        self.assertIn("Silver export failed with exit 19", output)

    def test_empty_query_output_fails(self):
        self.assert_fails("PostgreSQL export returned no output",
                          pg_cmd="true", silver=trino_csv(STATIONS))

    def test_header_without_rows_fails(self):
        self.assert_fails("Silver: no data rows",
                          pg=pg_csv(STATIONS), silver=trino_csv({}))

    # --- wrong state values fail ----------------------------------------------
    def test_changed_bike_count_with_the_same_timestamp_fails(self):
        self.assert_fails("station station-a: num_bikes_available: postgres=4 silver=5",
                          pg=pg_csv(changed("station-a", bikes=4)), silver=trino_csv(STATIONS))

    def test_changed_flag_fails(self):
        self.assert_fails("is_renting",
                          pg=pg_csv(changed("station-b", flags=(True, True, True))),
                          silver=trino_csv(STATIONS))

    def test_changed_poll_id_fails(self):
        self.assert_fails("poll_id",
                          pg=pg_csv(changed("station-a", poll_id="station_status-older")),
                          silver=trino_csv(STATIONS))

    def test_null_versus_value_fails(self):
        self.assert_fails("num_docks_available",
                          pg=pg_csv(changed("station-b", docks=0)), silver=trino_csv(STATIONS))

    def test_one_microsecond_difference_in_collected_at_fails(self):
        self.assert_fails("collected_at",
                          pg=pg_csv(changed("station-a", collected="2026-11-28T02:00:01.123455Z")),
                          silver=trino_csv(STATIONS))

    def test_changed_last_reported_fails(self):
        self.assert_fails("last_reported",
                          pg=pg_csv(changed("station-a", reported="2026-11-28T01:59:31.000000Z")),
                          silver=trino_csv(STATIONS))

    # --- missing or additional stations fail ----------------------------------
    def test_station_missing_in_postgres_fails(self):
        self.assert_fails("1 station(s) in Silver but missing in PostgreSQL: ['station-b']",
                          pg=pg_csv({"station-a": STATIONS["station-a"]}),
                          silver=trino_csv(STATIONS))

    def test_additional_station_in_postgres_fails(self):
        extra = {**STATIONS, "station-z": STATIONS["station-a"]}
        self.assert_fails("1 station(s) in PostgreSQL but not in Silver: ['station-z']",
                          pg=pg_csv(extra), silver=trino_csv(STATIONS))

    def test_duplicate_station_fails(self):
        doubled = trino_csv(STATIONS) + trino_csv(STATIONS).splitlines()[1] + "\n"
        self.assert_fails("appears twice", pg=pg_csv(STATIONS), silver=doubled)

    def test_missing_column_fails(self):
        no_poll = "\n".join(",".join(c for i, c in enumerate(line.split(",")) if i != 1)
                            for line in pg_csv(STATIONS).splitlines()) + "\n"
        self.assert_fails("missing column(s) ['poll_id']", pg=no_poll, silver=trino_csv(STATIONS))


if __name__ == "__main__":
    unittest.main()
