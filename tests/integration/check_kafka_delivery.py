import argparse
import json
import os
import time
from collections import Counter
from pathlib import Path
from uuid import uuid4

from confluent_kafka import Consumer, KafkaError, TopicPartition


def signature(key, payload):
    return (
        key,
        json.dumps(payload, sort_keys=True, ensure_ascii=False),
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("poll_directory")
    args = parser.parse_args()

    folder = Path(args.poll_directory)
    feed_name = folder.parent.name

    topics = {
        "station_status": os.getenv(
            "GBFS_STATUS_TOPIC", "gbfs.station_status"
        ),
        "station_information": os.getenv(
            "GBFS_INFORMATION_TOPIC", "gbfs.station_information"
        ),
    }

    if feed_name not in topics:
        raise ValueError("Thư mục không thuộc feed được hỗ trợ")

    topic = topics[feed_name]

    raw = json.loads(
        (folder / "raw.json").read_text(encoding="utf-8")
    )

    with (folder / "valid.jsonl").open(encoding="utf-8") as file:
        rows = [json.loads(line) for line in file if line.strip()]

    if not rows:
        raise RuntimeError("Không có message hợp lệ để đối soát")

    expected = Counter(
        signature(row["station_id"], row)
        for row in rows
    )
    received = Counter()

    def belongs_to_this_poll(payload):
        # Cả status và information đều mang poll_id của lần polling.
        return payload.get("poll_id") == raw["poll_id"]

    consumer = Consumer({
        "bootstrap.servers": os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        ),
        "group.id": f"gbfs-delivery-check-{uuid4()}",
        "enable.auto.commit": False,
        "enable.partition.eof": True,
    })

    try:
        topic_info = consumer.list_topics(
            topic=topic, timeout=10
        ).topics[topic]

        if topic_info.error is not None:
            raise RuntimeError(str(topic_info.error))

        # Chốt phạm vi offset trước khi đọc.
        bounds = {}

        for partition in sorted(topic_info.partitions):
            bounds[partition] = consumer.get_watermark_offsets(
                TopicPartition(topic, partition),
                timeout=10,
                cached=False,
            )

        for partition, (low, high) in bounds.items():
            if low == high:
                continue

            consumer.assign([
                TopicPartition(topic, partition, low)
            ])
            deadline = time.monotonic() + 30

            while True:
                if time.monotonic() > deadline:
                    raise TimeoutError(
                        f"Đọc partition {partition} quá thời gian"
                    )

                message = consumer.poll(1)

                if message is None:
                    continue

                if message.error():
                    if (
                        message.error().code()
                        == KafkaError._PARTITION_EOF
                    ):
                        if message.offset() >= high:
                            break
                        continue

                    raise RuntimeError(str(message.error()))

                if message.offset() >= high:
                    break

                if message.value() is not None:
                    payload = json.loads(message.value())

                    if belongs_to_this_poll(payload):
                        key = (
                            message.key().decode("utf-8")
                            if message.key() is not None
                            else None
                        )
                        received[signature(key, payload)] += 1

                if message.offset() + 1 >= high:
                    break

    finally:
        consumer.close()

    missing = expected - received
    unexpected = received - expected

    print("Feed:", feed_name)
    print("Poll ID trong raw:", raw["poll_id"])
    print("Message trong file:", sum(expected.values()))
    print("Message Kafka thuộc lần polling:", sum(received.values()))
    print("Thiếu hoặc sai key/nội dung:", sum(missing.values()))
    print("Thừa, trùng hoặc sai key/nội dung:", sum(unexpected.values()))

    if missing or unexpected:
        raise AssertionError("Đối soát Kafka KHÔNG đạt")

    print("PASS: key, nội dung và số lượng khớp hoàn toàn.")


if __name__ == "__main__":
    main()
