#!/bin/sh
# Creates the lakehouse namespaces in the Iceberg REST Catalog. Safe to re-run:
# HTTP 409 means the namespace already exists.
set -eu
for ns in $ICEBERG_NAMESPACES; do
  code=$(curl -sS -o /tmp/resp -w '%{http_code}' -X POST \
    -H 'Content-Type: application/json' \
    -d "{\"namespace\": [\"$ns\"]}" \
    "$ICEBERG_REST_URI/v1/namespaces")
  case "$code" in
    200) echo "created namespace $ns" ;;
    409) echo "namespace $ns already exists" ;;
    *) echo "failed to create namespace $ns: HTTP $code"; cat /tmp/resp; exit 1 ;;
  esac
done
