#!/bin/sh
set -eu
mc alias set local http://minio:9000 "$AWS_ACCESS_KEY_ID" "$AWS_SECRET_ACCESS_KEY"
for bucket in "$RAW_BUCKET" "$WAREHOUSE_BUCKET" "$CHECKPOINT_BUCKET" "$QUARANTINE_BUCKET"; do
  mc mb --ignore-existing "local/$bucket"
done
