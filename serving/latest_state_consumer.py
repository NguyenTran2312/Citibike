import json
import logging
import os
import signal
from datetime import datetime, timezone

import psycopg
from confluent_kafka import Consumer, Producer
from pydantic import ValidationError

from ingestion.gbfs.schemas import InvalidMessage, StationStatusMessage

logger = logging.getLogger(__name__)

UPSERT_STATION = """
INSERT INTO public.station_status_latest AS current_status (
    station_id, poll_id, num_bikes_available, num_docks_available,
    is_installed, is_renting, is_returning,
    last_updated, last_reported, collected_at
)
VALUES (
    %s, %s, %s, %s, %s, %s, %s,
    to_timestamp(%s), to_timestamp(%s), %s
)
ON CONFLICT (station_id) DO UPDATE SET
    poll_id = EXCLUDED.poll_id,
    num_bikes_available = EXCLUDED.num_bikes_available,
    num_docks_available = EXCLUDED.num_docks_available,
    is_installed = EXCLUDED.is_installed,
    is_renting = EXCLUDED.is_renting,
    is_returning = EXCLUDED.is_returning,
    last_updated = EXCLUDED.last_updated,
    last_reported = EXCLUDED.last_reported,
    collected_at = EXCLUDED.collected_at
WHERE EXCLUDED.collected_at > current_status.collected_at
"""

# Lỗi do chính dữ liệu: ghi lại bao nhiêu lần cũng vẫn lỗi.
# Lỗi kết nối (OperationalError, ...) không nằm ở đây: để consumer dừng,
# Docker khởi động lại và offset chưa commit sẽ được đọc lại.
REJECTED_BY_DATABASE = (psycopg.DataError, psycopg.IntegrityError)


class Quarantine:
    """Gửi message hỏng sang gbfs.invalid và chờ broker xác nhận."""

    def __init__(self, topic, bootstrap_servers):
        self.topic = topic
        self.producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "client.id": "citibike-latest-state-quarantine",
            "enable.idempotence": True,
            "acks": "all",
            "message.timeout.ms": 30000,
        })

    def send(self, message, error_type, error):
        invalid = InvalidMessage(
            source=message.topic(),
            error_type=error_type,
            error_message=str(error) or type(error).__name__,
            raw_payload=decode_payload(message.value()),
            detected_at=datetime.now(timezone.utc),
        )
        errors = []

        def on_delivery(delivery_error, _):
            if delivery_error is not None:
                errors.append(str(delivery_error))

        self.producer.produce(
            topic=self.topic,
            key=message.key(),
            value=invalid.model_dump_json().encode("utf-8"),
            # Vị trí gốc để tra lại; schema invalid không cho thêm trường.
            headers=[
                ("source_topic", message.topic().encode("utf-8")),
                ("source_partition", str(message.partition()).encode()),
                ("source_offset", str(message.offset()).encode()),
            ],
            on_delivery=on_delivery,
        )
        remaining = self.producer.flush(35)

        if errors or remaining:
            # Chưa cách ly được thì không được commit offset.
            raise RuntimeError(
                f"Không gửi được sang {self.topic}: "
                f"errors={errors}, remaining={remaining}"
            )

        logger.warning(
            "Đã chuyển message sang %s: error_type=%s partition=%s offset=%s",
            self.topic, error_type, message.partition(), message.offset(),
        )


def decode_payload(value):
    if value is None:
        return None
    try:
        return json.loads(value)
    except ValueError:
        return value.decode("utf-8", errors="replace")


def process_message(message, connection, quarantine):
    """Trả về "upserted" hoặc "quarantined"; lỗi kết nối được ném ra ngoài."""
    try:
        status = StationStatusMessage.model_validate_json(message.value() or b"")
    except ValidationError as error:
        quarantine.send(message, "CONSUMER_VALIDATION_ERROR", error)
        return "quarantined"

    try:
        with connection.transaction():
            connection.execute(UPSERT_STATION, (
                status.station_id,
                status.poll_id,
                status.num_bikes_available,
                status.num_docks_available,
                status.is_installed,
                status.is_renting,
                status.is_returning,
                status.last_updated,
                status.last_reported,
                status.collected_at,
            ))
    except REJECTED_BY_DATABASE as error:
        quarantine.send(message, "DATABASE_REJECTED", error)
        return "quarantined"

    return "upserted"


def main() -> None:
    running = True

    def stop(signum, frame):
        nonlocal running
        logger.info("Nhận %s, đang dừng consumer", signal.Signals(signum).name)
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    topic = os.environ["GBFS_STATUS_TOPIC"]
    bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": os.environ["KAFKA_GROUP_ID"],
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    })
    quarantine = Quarantine(
        os.getenv("GBFS_INVALID_TOPIC", "gbfs.invalid"),
        bootstrap_servers,
    )

    try:
        # psycopg lấy cấu hình kết nối từ các biến PG* trong Compose.
        with psycopg.connect(connect_timeout=10) as connection:
            consumer.subscribe([topic])
            logger.info("Đang đọc topic %s", topic)

            while running:
                message = consumer.poll(1.0)
                if message is None:
                    continue
                if message.error() is not None:
                    raise RuntimeError(f"Lỗi đọc Kafka: {message.error()}")

                process_message(message, connection, quarantine)

                # Chỉ commit sau khi PostgreSQL đã commit,
                # hoặc message hỏng đã được broker xác nhận ở gbfs.invalid.
                offsets = consumer.commit(message=message, asynchronous=False)
                for offset in offsets:
                    if offset.error is not None:
                        raise RuntimeError(f"Lỗi commit Kafka offset: {offset.error}")
    finally:
        consumer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
