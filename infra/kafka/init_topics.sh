#!/bin/bash
set -euo pipefail
for topic in gbfs.station_status gbfs.station_information gbfs.invalid; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 \
    --create --if-not-exists --topic "$topic" --partitions 3 \
    --replication-factor 1 --config cleanup.policy=delete \
    --config retention.ms=604800000
done
