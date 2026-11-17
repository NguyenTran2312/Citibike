# Streaming — GBFS Kafka → Spark

| File | Vai trò |
|---|---|
| `utils/schemas.py` | `StructType` của 3 topic `gbfs.station_status`, `gbfs.station_information`, `gbfs.invalid` (contract: `docs/contracts.md` 1.4, 1.5, 2.3.2) |
| `jobs/gbfs_history.py` | Entry point của service `spark-streaming`: đọc Kafka → `from_json` → phân loại hợp lệ / sai schema → sink |
| `fixtures/gbfs/*.jsonl` | Message mẫu (mỗi dòng 1 message). `station_status_bad.jsonl`: 10 message sai, mỗi dòng một kiểu lỗi |

Hiện tại (STR-07) job chỉ có sink `console`. Sink Iceberg (dedup `(station_id, poll_id)`, dùng `STATUS_/INFORMATION_CHECKPOINT_LOCATION`) làm ở issue sau.

## Phân loại message

`validate()` thêm 3 cột `dq_is_valid`, `dq_error_type`, `dq_error_message`:

| `dq_error_type` | Khi nào |
|---|---|
| `PARSE_ERROR` | Message rỗng hoặc không phải JSON |
| `SCHEMA_ERROR` | JSON hợp lệ nhưng không phải object, hoặc field sai kiểu (`"five"` cho số, `"true"` cho boolean) |
| `VALIDATION_ERROR` | Parse được nhưng vi phạm contract: thiếu field bắt buộc, chuỗi rỗng, số âm, `schema_version != 1`, datetime thiếu timezone, `lat/lon` ngoài phạm vi… (liệt kê tất cả lỗi, nối bằng `; `) |

Message sai được đưa về đúng dạng `gbfs.invalid` (`source, error_type, error_message, raw_payload, detected_at`) bằng `invalid_rows()`.

## Chạy

Cần các service `kafka`, `spark-master`, `iceberg-rest` đang chạy. `spark-defaults.conf` đặt `lakehouse` làm catalog mặc định, nên mọi query Spark đều cần kết nối được `iceberg-rest`.

```bash
# Kiểm tra parse + validate với fixture (không cần Kafka)
docker compose exec spark-master /opt/spark/bin/spark-submit \
  /opt/project/streaming/jobs/gbfs_history.py --fixture

# Đọc Kafka, in message hợp lệ và sai schema của từng micro-batch
docker compose exec -e STREAM_TRIGGER_INTERVAL="30 seconds" spark-master \
  /opt/spark/bin/spark-submit /opt/project/streaming/jobs/gbfs_history.py \
  --feed status --checkpoint-dir /tmp/str07-checkpoints
```

| Tham số | Ý nghĩa |
|---|---|
| `--feed status\|information\|all` | Feed cần đọc (mặc định `all`) |
| `--checkpoint-dir DIR` | Checkpoint thử nghiệm, mỗi feed một thư mục `DIR/<feed>`. Không truyền thì Spark dùng thư mục tạm và luôn đọc lại từ `STARTING_OFFSETS` |
| `--available-now` | Xử lý hết dữ liệu đang có trong Kafka rồi dừng (dùng cho test restart) |

Biến môi trường dùng đúng tên trong `docker-compose.yml`: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_GROUP_ID`, `GBFS_STATUS_TOPIC`, `GBFS_INFORMATION_TOPIC`, `STREAM_TRIGGER_INTERVAL` (mặc định 5 phút), cùng `STARTING_OFFSETS`, `MAX_OFFSETS_PER_TRIGGER`.

## Test

```bash
# Unit + contract tests (chạy trong image Spark, không cần stack)
docker run --rm -v "$PWD":/opt/project:ro -w /opt/project --entrypoint bash citibike/spark:3.5.6-local -c \
  'export PYTHONPATH=/opt/project:/opt/spark/python:$(ls /opt/spark/python/lib/py4j-*.zip); python3 -W ignore::ResourceWarning -m unittest discover -s tests/streaming -v'

# Integration: đọc Kafka + restart từ checkpoint (dùng topic test riêng, tự xoá sau khi chạy)
bash tests/integration/str07_checkpoint_restart.sh
```

`test_matches_producer_json_schemas` so tên field với `ingestion/gbfs/json_schemas/*.schema.json` của producer; test này tự skip khi các file đó chưa có trên nhánh.
