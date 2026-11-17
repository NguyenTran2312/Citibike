#!/bin/bash
# Creates the GBFS topics and (re)applies their configs, so topics created by an
# older version of this script converge to the same settings. Safe to re-run.
set -euo pipefail
BS=kafka:29092
CONFIGS="cleanup.policy=delete,retention.ms=604800000"
for topic in gbfs.station_status gbfs.station_information gbfs.invalid; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BS" \
    --create --if-not-exists --topic "$topic" --partitions 3 \
    --replication-factor 1 --config cleanup.policy=delete \
    --config retention.ms=604800000
  /opt/kafka/bin/kafka-configs.sh --bootstrap-server "$BS" --alter \
    --entity-type topics --entity-name "$topic" --add-config "$CONFIGS"
done
