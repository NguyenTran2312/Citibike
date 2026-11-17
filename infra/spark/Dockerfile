FROM apache/spark:3.5.6
USER root
ENV PYTHONPATH=/opt/project PYTHONUNBUFFERED=1 TZ=UTC
WORKDIR /opt/project
# Drivers and workers use the same Python runtime and project mount paths.
# Iceberg/Kafka/S3A connectors are versioned in spark-defaults.conf.
