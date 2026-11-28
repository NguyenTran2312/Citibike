from __future__ import annotations

import os
import shutil
import tempfile
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from ingestion.trip_history.manifest import (
    TripManifest,
    compute_sha256,
    inspect_and_extract_zip,
)

import string
import tempfile
from pathlib import Path
import shutil


def get_candidate_temp_roots() -> list[Path]:
    candidates: list[Path] = []

    # Explicit override always wins if it exists.
    custom_tmp = os.getenv("TMP_DIR")
    if custom_tmp:
        candidates.append(Path(custom_tmp))

    # Windows drives: C:\, D:\, E:\, ...
    if os.name == "nt":
        for letter in string.ascii_uppercase:
            drive = Path(f"{letter}:/")
            if drive.exists():
                candidates.append(drive)

    # Linux/macOS common locations.
    for path in (Path("/tmp"), Path("/var/tmp"), Path.home()):
        candidates.append(path)

    # Remove duplicates while preserving order.
    return list(dict.fromkeys(candidates))


def choose_temp_root(min_free_bytes: int = 10 * 1024**3) -> Path:
    """Choose the writable candidate directory with the most free space."""
    candidates = get_candidate_temp_roots()
    usable: list[tuple[int, Path]] = []

    for candidate in candidates:
        try:
            candidate.mkdir(parents=True, exist_ok=True)

            # Verify the process can actually create files there.
            probe = candidate / ".citibike_downloader_write_test"
            probe.touch()
            probe.unlink()

            free_bytes = shutil.disk_usage(candidate).free
            usable.append((free_bytes, candidate))
        except (OSError, PermissionError):
            continue

    if not usable:
        raise RuntimeError("No writable temporary directory found.")

    # Prefer TMP_DIR if it is usable and has enough space.
    custom_tmp = os.getenv("TMP_DIR")
    if custom_tmp:
        custom_path = Path(custom_tmp)
        for free_bytes, candidate in usable:
            if candidate == custom_path and free_bytes >= min_free_bytes:
                return candidate

    free_bytes, best = max(usable, key=lambda item: item[0])

    if free_bytes < min_free_bytes:
        raise RuntimeError(
            f"Insufficient temporary disk space: best candidate {best} has "
            f"{free_bytes / 1024**3:.2f} GiB free; need at least "
            f"{min_free_bytes / 1024**3:.2f} GiB."
        )

    return best


BUCKET_LIST_URL = "https://s3.amazonaws.com/tripdata/"
DEFAULT_BASE_URL = "https://s3.amazonaws.com/tripdata"


def get_s3_client():
    endpoint = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "citibike")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "citibike-local-secret")
    region = os.getenv("AWS_REGION", "us-east-1")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def iter_months(start_ym: str, end_ym: str) -> list[str]:
    start_dt = datetime.strptime(start_ym, "%Y-%m")
    end_dt = datetime.strptime(end_ym, "%Y-%m")
    if start_dt > end_dt:
        raise ValueError(f"TRIP_START_MONTH ({start_ym}) > TRIP_END_MONTH ({end_ym})")

    months: list[str] = []
    year, month = start_dt.year, start_dt.month
    while (year, month) <= (end_dt.year, end_dt.month):
        months.append(f"{year:04d}-{month:02d}")
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    return months


def list_remote_tripdata_keys(list_url: str = BUCKET_LIST_URL) -> list[str]:
    req = urllib.request.Request(list_url, headers={"User-Agent": "citibike-downloader/1.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        xml_bytes = resp.read()

    root = ET.fromstring(xml_bytes)
    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    keys = [elem.text.strip() for elem in root.findall(".//s3:Contents/s3:Key", ns) if elem.text]
    if not keys:
        keys = [elem.text.strip() for elem in root.findall(".//Contents/Key") if elem.text]
    return keys


def resolve_archive_url_for_month(ym: str, available_keys: list[str]) -> tuple[str, str]:
    compact = ym.replace("-", "")
    candidates = [
        f"{compact}-citibike-tripdata.zip",
        f"JC-{compact}-citibike-tripdata.zip",
    ]
    for cand in candidates:
        if cand in available_keys:
            encoded = urllib.parse.quote(cand, safe="")
            return f"{DEFAULT_BASE_URL}/{encoded}", cand

    matches = [
        k for k in available_keys
        if k.startswith(compact) and "citibike-tripdata" in k and k.endswith(".zip")
    ]
    if matches:
        chosen = sorted(matches)[0]
        encoded = urllib.parse.quote(chosen, safe="")
        return f"{DEFAULT_BASE_URL}/{encoded}", chosen

    raise FileNotFoundError(f"No matching ZIP archive found in tripdata bucket for month {ym}")


def download_to_file(url: str, dest_path: Path) -> None:
    part_path = dest_path.with_suffix(dest_path.suffix + ".part")
    req = urllib.request.Request(url, headers={"User-Agent": "citibike-downloader/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=300) as resp, part_path.open("wb") as out:
            while True:
                chunk = resp.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
        part_path.replace(dest_path)
    except Exception:
        if part_path.exists():
            part_path.unlink()
        raise


def s3_object_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def load_existing_manifest(s3, bucket: str, key: str) -> TripManifest | None:
    if not s3_object_exists(s3, bucket, key):
        return None
    obj = s3.get_object(Bucket=bucket, Key=key)
    raw_text = obj["Body"].read().decode("utf-8")
    return TripManifest.from_json(raw_text)


def staged_csvs_exist(s3, bucket: str, staging_prefix: str, csv_names: list[str]) -> bool:
    if not csv_names:
        return False
    return all(
        s3_object_exists(s3, bucket, f"{staging_prefix}/{name}")
        for name in csv_names
    )


def process_month(s3, bucket: str, ym: str, available_keys: list[str], work_root: Path) -> None:
    year, month = ym.split("-")
    raw_prefix = f"trip_history/nyc/year={year}/month={month}"
    manifest_key = f"{raw_prefix}/manifest.json"

    source_url, archive_name = resolve_archive_url_for_month(ym, available_keys)
    zip_key = f"{raw_prefix}/{archive_name}"
    staging_prefix = f"trip_history/staging/{ym}"

    month_tmp = work_root / ym
    if month_tmp.exists():
        shutil.rmtree(month_tmp)
    month_tmp.mkdir(parents=True, exist_ok=True)
    local_zip = month_tmp / archive_name

    try:
        existing_manifest = load_existing_manifest(s3, bucket, manifest_key)
        zip_in_s3 = s3_object_exists(s3, bucket, zip_key)
        expected_csvs = (
            [m.member_name for m in existing_manifest.csv_members]
            if existing_manifest
            else []
        )

        if (
            existing_manifest
            and existing_manifest.status == "staged"
            and zip_in_s3
            and staged_csvs_exist(s3, bucket, staging_prefix, expected_csvs)
        ):
            print(f"[{ym}] Already staged (sha256={existing_manifest.sha256[:12]}...), skipping download.")
            return

        print(f"[{ym}] Downloading {source_url} ...")
        download_to_file(source_url, local_zip)
        local_sha = compute_sha256(local_zip)

        if existing_manifest and existing_manifest.sha256 == local_sha and zip_in_s3:
            print(f"[{ym}] SHA-256 matches existing manifest; ensuring staging CSVs are present.")
        else:
            s3.upload_file(str(local_zip), bucket, zip_key)
            print(f"[{ym}] Uploaded raw archive -> s3://{bucket}/{zip_key}")

        manifest, extracted_csvs = inspect_and_extract_zip(
            zip_path=local_zip,
            extract_dir=month_tmp / "extracted",
            source_url=source_url,
            month=ym,
        )

        for csv_path in extracted_csvs:
            staging_key = f"{staging_prefix}/{csv_path.name}"
            s3.upload_file(str(csv_path), bucket, staging_key)
            print(f"[{ym}] Uploaded staging CSV -> s3://{bucket}/{staging_key}")

        s3.put_object(
            Bucket=bucket,
            Key=manifest_key,
            Body=manifest.to_json().encode("utf-8"),
            ContentType="application/json",
        )
        print(f"[{ym}] Wrote manifest -> s3://{bucket}/{manifest_key}")
    finally:
        shutil.rmtree(month_tmp, ignore_errors=True)


def main() -> None:
    start_month = os.getenv("TRIP_START_MONTH", "2026-01")
    end_month = os.getenv("TRIP_END_MONTH", "2026-10")
    raw_bucket = os.getenv("RAW_BUCKET", "raw")

    months = iter_months(start_month, end_month)
    s3 = get_s3_client()
    available_keys = list_remote_tripdata_keys()

    base_dir = choose_temp_root(min_free_bytes=10 * 1024**3)

    print(f"Using temporary directory: {base_dir}")
    tmp_base = Path(tempfile.mkdtemp(prefix="citibike_trips_", dir=base_dir))

    try:
        for ym in months:
            try:
                process_month(s3, raw_bucket, ym, available_keys, tmp_base)
            except FileNotFoundError as exc:
                print(f"[{ym}] WARNING: {exc}")
    finally:
        shutil.rmtree(tmp_base, ignore_errors=True)


if __name__ == "__main__":
    main()