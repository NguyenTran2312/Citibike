"""Gate 7 comparator: PostgreSQL station_status_latest == latest Silver observation.

    python3 tests/integration/latest_state_compare.py <postgres.csv> <silver.csv>

Both files are CSV with a header and the columns in FIELDS (any order), as
written by tests/integration/latest_state_reconcile.sh:
  * postgres.csv: every row of public.station_status_latest;
  * silver.csv:   for every station, its latest row of silver.gbfs_status
                  (max collected_at, ties broken by max poll_id).

Fails (exit 1) when a file is missing, unreadable, has no data rows, lacks a
column, has a station twice, when a station is only on one side, or when any
field differs. Timestamps are compared as UTC instants in microseconds,
booleans and NULLs are normalised (PostgreSQL t/f, Trino true/false, empty =
NULL). Standard library only. Exit 0 only when every station matches.
"""
import csv
import sys
from datetime import datetime, timezone

KEY = "station_id"
FIELDS = [KEY, "poll_id", "num_bikes_available", "num_docks_available",
          "is_installed", "is_renting", "is_returning",
          "last_updated", "last_reported", "collected_at"]
INTEGERS = {"num_bikes_available", "num_docks_available"}
BOOLEANS = {"is_installed", "is_renting", "is_returning"}
TIMESTAMPS = {"last_updated", "last_reported", "collected_at"}
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
MAX_REPORTED = 20


class Unreadable(Exception):
    pass


def _timestamp_micros(text):
    """ISO-8601 (with Z/offset, or naive = UTC) -> integer microseconds since epoch."""
    value = text.strip().replace(" UTC", "").replace("Z", "+00:00").replace(" ", "T", 1)
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    delta = parsed - EPOCH
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def normalise(field, text):
    if text is None or text.strip() == "" or text.strip().upper() == "NULL":
        return None
    text = text.strip()
    if field in INTEGERS:
        return int(text)
    if field in BOOLEANS:
        lowered = text.lower()
        if lowered in ("t", "true", "1"):
            return True
        if lowered in ("f", "false", "0"):
            return False
        raise ValueError(f"not a boolean: {text!r}")
    if field in TIMESTAMPS:
        return _timestamp_micros(text)
    return text


def load(path, label):
    """{station_id: {field: normalised value}}; raises Unreadable on any problem."""
    try:
        with open(path, newline="", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            missing = [f for f in FIELDS if f not in (reader.fieldnames or [])]
            if missing:
                raise Unreadable(f"{label}: missing column(s) {missing} "
                                 f"(header: {reader.fieldnames})")
            rows = {}
            for number, row in enumerate(reader, start=2):
                try:
                    values = {f: normalise(f, row[f]) for f in FIELDS}
                except (TypeError, ValueError) as error:
                    raise Unreadable(f"{label} line {number}: {error}") from error
                station = values[KEY]
                if station is None:
                    raise Unreadable(f"{label} line {number}: empty {KEY}")
                if station in rows:
                    raise Unreadable(f"{label}: {KEY} {station} appears twice")
                rows[station] = values
    except OSError as error:
        raise Unreadable(f"{label}: {error}") from error
    if not rows:
        raise Unreadable(f"{label}: no data rows")
    return rows


def compare(postgres, silver):
    """List of human-readable problems; empty when both sides match."""
    problems = []
    only_pg = sorted(set(postgres) - set(silver))
    only_silver = sorted(set(silver) - set(postgres))
    if only_silver:
        problems.append(f"{len(only_silver)} station(s) in Silver but missing in PostgreSQL: "
                        f"{only_silver[:MAX_REPORTED]}")
    if only_pg:
        problems.append(f"{len(only_pg)} station(s) in PostgreSQL but not in Silver: "
                        f"{only_pg[:MAX_REPORTED]}")
    mismatched = 0
    for station in sorted(set(postgres) & set(silver)):
        diffs = [f"{f}: postgres={postgres[station][f]!r} silver={silver[station][f]!r}"
                 for f in FIELDS if postgres[station][f] != silver[station][f]]
        if diffs:
            mismatched += 1
            if mismatched <= MAX_REPORTED:
                problems.append(f"station {station}: " + "; ".join(diffs))
    if mismatched > MAX_REPORTED:
        problems.append(f"... {mismatched - MAX_REPORTED} more mismatched station(s)")
    return problems


def main(argv):
    if len(argv) != 3:
        print(__doc__.strip().splitlines()[2].strip())
        return 2
    try:
        postgres = load(argv[1], "PostgreSQL")
        silver = load(argv[2], "Silver")
    except Unreadable as error:
        print(f"[FAIL] {error}")
        return 1
    problems = compare(postgres, silver)
    if problems:
        for problem in problems:
            print(f"[FAIL] {problem}")
        print(f"[FAIL] latest state differs: {len(postgres)} PostgreSQL / {len(silver)} Silver stations")
        return 1
    print(f"[PASS] {len(postgres)} stations, every field equal "
          f"({', '.join(FIELDS[1:])})")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
