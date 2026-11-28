import os
import time
from collections import Counter
from datetime import datetime, timezone

from confluent_kafka import Producer
from pydantic import ValidationError

from ingestion.gbfs.schemas import (
    InvalidMessage,
    StationInformationMessage,
    StationStatusMessage,
)


class GBFSProducer:
    """Dùng một instance trong một luồng xử lý."""

    def __init__(self):
        self.routes = {
            "station_status": (
                os.getenv("GBFS_STATUS_TOPIC", "gbfs.station_status"),
                StationStatusMessage,
            ),
            "station_information": (
                os.getenv(
                    "GBFS_INFORMATION_TOPIC",
                    "gbfs.station_information",
                ),
                StationInformationMessage,
            ),
        }

        self.invalid_topic = os.getenv(
            "GBFS_INVALID_TOPIC",
            "gbfs.invalid",
        )

        self.delivered = Counter()
        self.errors = []

        self.client = Producer({
            "bootstrap.servers": os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS",
                "localhost:9092",
            ),
            "client.id": "citibike-gbfs-producer",
            "enable.idempotence": True,
            "acks": "all",
            "message.timeout.ms": 30000,
        })

    def on_delivery(self, error, message):
        if error is not None:
            self.errors.append(str(error))
        else:
            self.delivered[message.topic()] += 1

    def check_errors(self):
        if self.errors:
            raise RuntimeError(
                "Kafka delivery thất bại: " + "; ".join(self.errors)
            )

    def enqueue(self, topic, key, value):
        deadline = time.monotonic() + 10

        while True:
            self.check_errors()

            try:
                self.client.produce(
                    topic=topic,
                    key=key.encode("utf-8") if key else None,
                    value=value.encode("utf-8"),
                    on_delivery=self.on_delivery,
                )
                break
            except BufferError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("Hàng đợi producer bị đầy")

                # Xử lý callback và đợi hàng đợi có chỗ trống.
                self.client.poll(0.1)

        self.client.poll(0)
        self.check_errors()

    def send_invalid(self, payload):
        invalid = InvalidMessage.model_validate(payload)

        raw = invalid.raw_payload
        key = raw.get("station_id") if isinstance(raw, dict) else None

        if not isinstance(key, str) or not key:
            key = None

        self.enqueue(
            self.invalid_topic,
            key,
            invalid.model_dump_json(),
        )

        return self.invalid_topic

    def publish(self, feed_name, payload):
        if feed_name not in self.routes:
            raise ValueError(f"Feed không được hỗ trợ: {feed_name}")

        topic, model = self.routes[feed_name]

        try:
            message = model.model_validate(payload)
        except ValidationError as error:
            return self.send_invalid({
                "source": topic,
                "error_type": "VALIDATION_ERROR",
                "error_message": str(error),
                "raw_payload": payload,
                "detected_at": datetime.now(timezone.utc),
            })

        self.enqueue(
            topic,
            message.station_id,
            message.model_dump_json(),
        )

        return topic

    def flush(self):
        remaining = self.client.flush(35)
        self.check_errors()

        if remaining:
            raise TimeoutError(
                f"Còn {remaining} message chưa được xác nhận"
            )

        return dict(self.delivered)

def publish_poll_output(feed_name, output_dir):
    import json
    from pathlib import Path

    output_dir = Path(output_dir)

    def read_jsonl(filename):
        with (output_dir / filename).open(encoding="utf-8") as file:
            return [
                json.loads(line)
                for line in file
                if line.strip()
            ]

    # Đọc xong các file trước khi bắt đầu gửi.
    valid_rows = read_jsonl("valid.jsonl")
    invalid_rows = read_jsonl("invalid.jsonl")

    producer = GBFSProducer()
    queued = Counter()

    for row in valid_rows:
        topic = producer.publish(feed_name, row)
        queued[topic] += 1

    for row in invalid_rows:
        topic = producer.send_invalid(row)
        queued[topic] += 1

    confirmed = producer.flush()

    if dict(queued) != confirmed:
        raise RuntimeError(
            f"Đối soát Kafka thất bại: "
            f"queued={dict(queued)}, confirmed={confirmed}"
        )

    return confirmed
