#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"

# Cấu hình thử nghiệm theo đề xuất trong contract.
# Chưa phải kết quả benchmark hoặc quyết định cuối cùng của nhóm.
PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-3}"

TOPICS=(
  "gbfs.station_status"
  "gbfs.station_information"
  "gbfs.invalid"
)

for topic in "${TOPICS[@]}"; do
  echo "Khởi tạo topic: ${topic}"

  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${PARTITIONS}" \
    --replication-factor 1 \
    --config cleanup.policy=delete
done

echo "Danh sách topic:"

/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --list
