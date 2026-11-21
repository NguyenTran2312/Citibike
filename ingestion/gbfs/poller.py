import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen
from uuid import uuid4
import argparse
import threading
import time
import random

from urllib.error import HTTPError, URLError
from concurrent.futures import ThreadPoolExecutor
from pydantic import ValidationError

from ingestion.gbfs.schemas import (
    FeedMetadata,
    InvalidMessage,
    ObservationMetadata,
    StationInformationMessage,
    StationStatusMessage,
)


logger = logging.getLogger(__name__)

DISCOVERY_URL = os.getenv(
    "GBFS_DISCOVERY_URL",
    "https://gbfs.lyft.com/gbfs/2.3/bkn/gbfs.json",
)

MODELS = {
    "station_status": StationStatusMessage,
    "station_information": StationInformationMessage,
}


def utc_now():
    return datetime.now(timezone.utc)


def fetch_json(url):
    request = Request(
        url,
        headers={"User-Agent": "citibike-student-project/0.1"},
    )
    max_attempts = 3

    for attempt in range(1, max_attempts + 1):
        try:
            with urlopen(request, timeout=30) as response:
                return json.load(response)

        except HTTPError as error:
            error.close()
            if error.code not in {429, 500, 502, 503, 504}:
                raise
            if attempt == max_attempts:
                raise
            reason = f"HTTP {error.code}"

        except (URLError, TimeoutError, ConnectionError) as error:
            if attempt == max_attempts:
                raise
            reason = str(error)

        max_delay = 2 ** (attempt - 1)
        delay = random.uniform(0, max_delay)
        logger.warning(
            "GBFS request failed attempt=%d/%d url=%s reason=%s "
            "retry_in=%.2fs",
            attempt,
            max_attempts,
            url,
            reason,
            delay,
        )
        time.sleep(delay)


def discover_feeds():
    discovery = fetch_json(DISCOVERY_URL)
    return {
        feed["name"]: feed["url"]
        for feed in discovery["data"]["en"]["feeds"]
    }


def poll_feed(feed_name, source_url):
    # Mỗi lần gọi cho một feed có mã riêng.
    poll_id = f"{feed_name}-{uuid4()}"
    payload = fetch_json(source_url)
    collected_at = utc_now()

    output_dir = Path("data/gbfs") / feed_name / poll_id
    output_dir.mkdir(parents=True, exist_ok=True)

    # Lưu nguồn trước khi kiểm tra dữ liệu.
    raw = {
        "poll_id": poll_id,
        "source_url": source_url,
        "collected_at": collected_at.isoformat(),
        "payload": payload,
    }
    raw_path = output_dir / "raw.json"
    raw_path.write_text(
        json.dumps(raw, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    if os.getenv("GBFS_ARCHIVE_RAW", "false").lower() == "true":
        from ingestion.gbfs.raw_storage import upload_raw_json

        archive_uri = upload_raw_json(
            raw_path, feed_name, poll_id, collected_at
        )
        logger.info("Raw archived feed=%s uri=%s", feed_name, archive_uri)

    feed_metadata = {
        "last_updated": payload["last_updated"],
        "collected_at": collected_at,
    }

    if feed_name == "station_status":
        metadata = ObservationMetadata(
            **feed_metadata,
            poll_id=poll_id,
            schema_version=1,
        )
    else:
        metadata = FeedMetadata(**feed_metadata)

    rows = payload["data"]["stations"]
    if not isinstance(rows, list):
        raise ValueError("data.stations phải là danh sách")

    model = MODELS[feed_name]
    valid_count = 0
    invalid_count = 0

    with (
        (output_dir / "valid.jsonl").open(
            "w", encoding="utf-8"
        ) as valid_file,
        (output_dir / "invalid.jsonl").open(
            "w", encoding="utf-8"
        ) as invalid_file,
    ):
        for row in rows:
            try:
                if not isinstance(row, dict):
                    raise ValueError("Bản ghi trạm phải là object")

                # Metadata do hệ thống tạo có quyền ưu tiên.
                message = model.model_validate({
                    **row,
                    **metadata.model_dump(),
                })

            except (ValidationError, ValueError) as error:
                invalid = InvalidMessage(
                    source=f"gbfs.{feed_name}",
                    error_type="VALIDATION_ERROR",
                    error_message=str(error),
                    raw_payload=row,
                    detected_at=utc_now(),
                )
                invalid_file.write(invalid.model_dump_json() + "\n")
                invalid_count += 1
                continue

            valid_file.write(
                message.model_dump_json() + "\n"
            )
            valid_count += 1

    logger.info(
        "feed=%s poll_id=%s total=%d valid=%d invalid=%d",
        feed_name,
        poll_id,
        len(rows),
        valid_count,
        invalid_count,
    )
    logger.info("output=%s", output_dir)

    publish_enabled = os.getenv(
        "GBFS_PUBLISH_KAFKA",
        "false",
    ).lower() == "true"

    if publish_enabled:
        from ingestion.gbfs.producer import publish_poll_output

        confirmed = publish_poll_output(feed_name, output_dir)

        # Chỉ ghi xác nhận sau khi Kafka đã xác nhận toàn bộ.
        (output_dir / "kafka_delivery.json").write_text(
            json.dumps(
                {
                    "feed_name": feed_name,
                    "poll_id": poll_id,
                    "confirmed": confirmed,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        logger.info(
            "Kafka confirmed feed=%s poll_id=%s counts=%s",
            feed_name,
            poll_id,
            confirmed,
        )

    return output_dir


def run_feed_loop(feed_name, source_url, interval, stop_event):
    logger.info(
        "Bắt đầu lịch feed=%s interval=%ss",
        feed_name,
        interval,
    )

    while not stop_event.is_set():
        started_at = time.monotonic()

        try:
            poll_feed(feed_name, source_url)
        except Exception:
            # Lỗi được log; không tự biến lỗi mạng thành invalid.
            logger.exception("Polling thất bại feed=%s", feed_name)

        elapsed = time.monotonic() - started_at

        # Tính cả thời gian gọi API vào chu kỳ.
        # Nếu xử lý quá lâu, không chạy dồn các lần đã lỡ.
        wait_seconds = max(1.0, interval - elapsed)

        if stop_event.wait(wait_seconds):
            break

    logger.info("Đã dừng lịch feed=%s", feed_name)


def main():
    parser = argparse.ArgumentParser(description="Citi Bike GBFS Poller")

    parser.add_argument(
        "--once",
        action="store_true",
        help="Lấy mỗi feed một lần rồi thoát",
    )

    parser.add_argument(
        "--duration",
        type=float,
        default=None,
        help="Thời gian chạy thử, tính bằng giây",
    )

    args = parser.parse_args()

    if args.duration is not None and args.duration <= 0:
        parser.error("--duration phải lớn hơn 0")

    feeds = discover_feeds()

    if args.once:
        for feed_name in MODELS:
            poll_feed(feed_name, feeds[feed_name])
        return

    intervals = {
        "station_status": int(
            os.getenv("POLL_INTERVAL_SECONDS", "60")
        ),
        "station_information": int(
            os.getenv(
                "INFORMATION_POLL_INTERVAL_SECONDS",
                os.getenv("GBFS_INFORMATION_INTERVAL_SECONDS", "86400"),
            )
        ),
    }

    if any(interval <= 0 for interval in intervals.values()):
        raise ValueError("Chu kỳ polling phải lớn hơn 0")

    stop_event = threading.Event()

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(
                run_feed_loop,
                feed_name,
                feeds[feed_name],
                interval,
                stop_event,
            )
            for feed_name, interval in intervals.items()
        ]

        try:
            stop_event.wait(args.duration)
        except KeyboardInterrupt:
            logger.info("Nhận Ctrl+C, đang dừng poller...")
        finally:
            stop_event.set()

        # Đưa lỗi ngoài vòng xử lý của worker về luồng chính.
        for future in futures:
            future.result()

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    try:
        main()
    except Exception:
        logger.exception("Poller thất bại")
        raise
