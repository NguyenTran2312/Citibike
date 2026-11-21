import logging
import os

import psycopg
from confluent_kafka import Consumer
from pydantic import ValidationError

from ingestion.gbfs.schemas import StationStatusMessage

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


def main() -> None:
    topic = os.environ["GBFS_STATUS_TOPIC"]
    consumer = Consumer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "group.id": os.environ["KAFKA_GROUP_ID"],
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    })

    try:
        # psycopg lấy cấu hình kết nối từ các biến PG* trong Compose.
        with psycopg.connect(connect_timeout=10) as connection:
            consumer.subscribe([topic])
            logger.info("Đang đọc topic %s", topic)

            while True:
                message = consumer.poll(1.0)
                if message is None:
                    continue
                if message.error() is not None:
                    raise RuntimeError(f"Lỗi đọc Kafka: {message.error()}")

                try:
                    status = StationStatusMessage.model_validate_json(
                        message.value()
                    )
                except ValidationError:
                    logger.error(
                        "Message sai schema: topic=%s partition=%s offset=%s",
                        message.topic(), message.partition(), message.offset(),
                    )
                    raise

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

                # Thoát khối transaction thành công nghĩa là PostgreSQL đã commit.
                offsets = consumer.commit(message=message, asynchronous=False)
                for offset in offsets:
                    if offset.error is not None:
                        raise RuntimeError(f"Lỗi commit Kafka offset: {offset.error}")

    except KeyboardInterrupt:
        logger.info("Đang dừng consumer")
    finally:
        consumer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()

