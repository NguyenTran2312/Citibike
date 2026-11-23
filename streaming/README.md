# Streaming — GBFS Kafka → Spark

| File | Vai trò |
|---|---|
| `utils/schemas.py` | `StructType` của 3 topic `gbfs.station_status`, `gbfs.station_information`, `gbfs.invalid` (contract: `docs/contracts.md` 1.4, 1.5, 2.3.2) |
| `jobs/gbfs_history.py` | Entry point của service `spark-streaming`: đọc Kafka → `from_json` → phân loại hợp lệ / sai schema → sink; khởi động thêm query Silver status |
| `jobs/silver_status.py` | Bronze → `silver.gbfs_status` (STR-09): parse + validate lại `raw_value`, dedup `(station_id, poll_id)`, MERGE chỉ insert |
| `jobs/dq_checks.py` | Data-quality checks dạng SQL "đếm dòng xấu"; check `error` thất bại thì exit 1 |
| `fixtures/gbfs/*.jsonl` | Message mẫu (mỗi dòng 1 message). `station_status_bad.jsonl`: 10 message sai, mỗi dòng một kiểu lỗi |

Sink mặc định là **Bronze** (STR-08). Sink `console` dùng để debug. Dedup `(station_id, poll_id)` và lọc message sai làm ở Silver (STR-09/10). Với `--sink bronze` ở chế độ liên tục, job khởi động thêm query Silver status (tắt bằng `--no-silver`).

Schema Spark khớp tên field với JSON Schema của producer (`ingestion/gbfs/json_schemas`). Cả hai feed đều bắt buộc có `poll_id` không rỗng; `poll_id` của information là mã lần polling information (`station_information-<uuid4>`), khác `poll_id` của status.

## Phân loại message

`validate()` thêm 3 cột `dq_is_valid`, `dq_error_type`, `dq_error_message`:

| `dq_error_type` | Khi nào |
|---|---|
| `PARSE_ERROR` | Message rỗng hoặc không phải JSON |
| `SCHEMA_ERROR` | JSON hợp lệ nhưng không phải object, hoặc field sai kiểu (`"five"` cho số, `"true"` cho boolean) |
| `VALIDATION_ERROR` | Parse được nhưng vi phạm contract: thiếu field bắt buộc, chuỗi rỗng, số âm, `schema_version != 1`, datetime thiếu timezone, `lat/lon` ngoài phạm vi… (liệt kê tất cả lỗi, nối bằng `; `) |

Message sai được đưa về đúng dạng `gbfs.invalid` (`source, error_type, error_message, raw_payload, detected_at`) bằng `invalid_rows()`.

## Bronze (STR-08)

| Bảng (catalog `lakehouse`, Trino: `iceberg.bronze.*`) | Topic | Checkpoint (service `spark-streaming`) |
|---|---|---|
| `bronze.gbfs_status` | `gbfs.station_status` | `STATUS_CHECKPOINT_LOCATION` = `s3a://checkpoints/status-history/` |
| `bronze.gbfs_station_information` | `gbfs.station_information` | `INFORMATION_CHECKPOINT_LOCATION` = `s3a://checkpoints/information-history/` |

- **Grain:** một dòng = một message Kafka, khóa `(kafka_topic, kafka_partition, kafka_offset)`.
- **Append-only, không lọc, không dedup:** message sai vẫn được ghi, kèm `dq_is_valid`, `dq_error_type`, `dq_error_message`. Silver đọc từ Bronze và lọc theo `dq_is_valid`.
- `raw_value` là value Kafka nguyên văn, để dựng lại Silver từ Bronze khi đổi logic parse.
- `poll_id`, `station_id`, `collected_at` (timestamp) được tách ra từ message cho việc lọc, đọc incremental và dedup ở Silver; null khi message không parse được.
- `kafka_key` là key producer gửi (`station_id`); `ingested_at` là thời điểm micro-batch được xử lý.
- Partition theo `days(ingested_at)`. Iceberg format v2.
- Exactly-once: mỗi micro-batch là một commit Iceberg, offset Kafka nằm trong checkpoint của query. Restart không đọc lại batch đã commit.
- Mỗi feed một query, một checkpoint riêng. **Không** dùng chung checkpoint giữa Bronze và console.

## Silver status (STR-09)

`silver.gbfs_status` (Trino: `iceberg.silver.gbfs_status`), partition `days(collected_at)`.

- **Grain:** một dòng = một trạm tại một lần polling, khóa `(station_id, poll_id)`. Lần polling mà trạm không đổi trạng thái vẫn là một dòng mới.
- **Nguồn:** đọc `bronze.gbfs_status` incremental (Iceberg streaming source), checkpoint `SILVER_STATUS_CHECKPOINT_LOCATION` = `s3a://checkpoints/silver-status/`. Checkpoint này **phải nằm trên S3/MinIO**: Iceberg source ghi offset bằng S3FileIO của bảng.
- **Validation:** parse + validate lại `raw_value` bằng cùng schema và luật của STR-07 (không tin cờ `dq_is_valid` đã lưu ở Bronze), nên đổi luật thì dựng lại Silver từ Bronze được. Message sai chỉ nằm ở Bronze.
- **Dedup:** message phát lại cùng `(station_id, poll_id)` (producer retry, `--replay`) chỉ còn một dòng; giữ Kafka offset nhỏ nhất.
- **Idempotent:** `MERGE ... WHEN NOT MATCHED THEN INSERT *` theo `(station_id, poll_id, collected_at)`. Micro-batch chạy lại, hoặc mất checkpoint Silver rồi đọc lại toàn bộ Bronze, không thêm dòng trùng. Khoảng `collected_at` của batch được đưa vào điều kiện MERGE để Iceberg chỉ quét các partition liên quan.
- `last_updated`, `last_reported` (epoch giây) và `collected_at` được đổi sang `TIMESTAMP` UTC, giữ riêng từng cột. Cột lineage: `kafka_partition`, `kafka_offset`, `bronze_ingested_at`, `silver_processed_at`.

```bash
# Chạy bù Silver từ Bronze rồi dừng (thủ công / Airflow / test)
docker compose exec spark-master /opt/spark/bin/spark-submit \
  /opt/project/streaming/jobs/silver_status.py --available-now

# Data-quality gate (exit 1 khi có check error thất bại)
docker compose exec spark-master /opt/spark/bin/spark-submit \
  /opt/project/streaming/jobs/dq_checks.py --layer silver_status
```

| Check `silver_status` | Mức | Ý nghĩa |
|---|---|---|
| `silver_status_grain_unique` | error | Không có hai dòng cùng `(station_id, poll_id)` |
| `silver_status_required_not_null` | error | Các cột bắt buộc không null |
| `silver_status_counts_non_negative` | error | Số xe, số dock không âm |
| `silver_status_has_every_valid_bronze_key` | error | Mọi khóa hợp lệ ở Bronze đều có ở Silver (không mất) |
| `silver_status_only_valid_bronze_keys` | error | Silver không có khóa mà Bronze không coi là hợp lệ |
| `silver_status_poll_completeness` | warn | Lần polling có ít hơn 90% số trạm của lần lớn nhất |
| `silver_status_last_reported_not_after_collected` | warn | `last_reported` sau `collected_at` |

Hai check đối soát với Bronze chỉ có nghĩa khi Silver đã bắt kịp Bronze (ví dụ ngay sau `silver_status.py --available-now`); khi query đang chạy, Silver trễ Bronze tối đa một trigger. Layer `bronze_status` có thêm check không trùng offset.

Đối soát Bronze với Kafka (số dòng theo partition, offset trùng, số trạm mỗi `poll_id`, số snapshot):

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  /opt/project/tests/integration/str08_bronze_check.py lakehouse.bronze.gbfs_status
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:29092 --topic gbfs.station_status --time -1
```

## Chạy

Cần các service `kafka`, `spark-master`, `iceberg-rest` đang chạy. `spark-defaults.conf` đặt `lakehouse` làm catalog mặc định, nên mọi query Spark đều cần kết nối được `iceberg-rest`.

```bash
# Kiểm tra parse + validate với fixture (không cần Kafka)
docker compose exec spark-master /opt/spark/bin/spark-submit \
  /opt/project/streaming/jobs/gbfs_history.py --fixture

# Đọc Kafka, in message hợp lệ và sai schema của từng micro-batch
docker compose exec -e STREAM_TRIGGER_INTERVAL="30 seconds" spark-master \
  /opt/spark/bin/spark-submit /opt/project/streaming/jobs/gbfs_history.py \
  --sink console --feed status --checkpoint-dir /tmp/str07-checkpoints

# Ghi Bronze liên tục (service; trigger STREAM_TRIGGER_INTERVAL, mặc định 5 phút)
docker compose --profile pipeline up -d spark-streaming
```

| Tham số | Ý nghĩa |
|---|---|
| `--sink bronze\|console` | `bronze` (mặc định): ghi bảng Bronze; `console`: in ra màn hình |
| `--feed status\|information\|all` | Feed cần đọc (mặc định `all`) |
| `--checkpoint-dir DIR` | Mỗi feed một thư mục `DIR/<feed>`. Không truyền thì Bronze dùng `STATUS_/INFORMATION_CHECKPOINT_LOCATION`, còn console dùng thư mục tạm và luôn đọc lại từ `STARTING_OFFSETS` |
| `--available-now` | Xử lý hết dữ liệu đang có trong Kafka rồi dừng (dùng cho test restart) |

Biến môi trường dùng đúng tên trong `docker-compose.yml`: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_GROUP_ID`, `GBFS_STATUS_TOPIC`, `GBFS_INFORMATION_TOPIC`, `STREAM_TRIGGER_INTERVAL` (mặc định 5 phút), `STATUS_CHECKPOINT_LOCATION`, `INFORMATION_CHECKPOINT_LOCATION`, cùng `STARTING_OFFSETS`, `MAX_OFFSETS_PER_TRIGGER`, `BRONZE_STATUS_TABLE`, `BRONZE_INFORMATION_TABLE`.

## Test

```bash
# Unit + contract tests (chạy trong image Spark, không cần stack)
docker run --rm -v "$PWD":/opt/project:ro -w /opt/project --entrypoint bash citibike/spark:3.5.6-local -c \
  'export PYTHONPATH=/opt/project:/opt/spark/python:$(ls /opt/spark/python/lib/py4j-*.zip); python3 -W ignore::ResourceWarning -m unittest discover -s tests/streaming -v'

# Integration: đọc Kafka + restart từ checkpoint (dùng topic test riêng, tự xoá sau khi chạy)
bash tests/integration/str07_checkpoint_restart.sh

# Integration STR-08: Bronze khớp end offset Kafka, restart không trùng/không sót
# (topic, bảng, checkpoint riêng, tự xoá sau khi chạy)
bash tests/integration/str08_bronze.sh

# Integration STR-09: Silver dedup, idempotent khi chạy lại / mất checkpoint, DQ gate
bash tests/integration/str09_silver.sh
```

`test_matches_producer_json_schemas` so tên field với `ingestion/gbfs/json_schemas/*.schema.json` của producer; test này tự skip khi các file đó chưa có trên nhánh.

### Kết quả đã ghi nhận (23/11/2026)

| Kiểm tra | Kết quả |
|---|---|
| Unit + contract tests (`tests/streaming`) | 14/14 pass, gồm so field với JSON Schema của producer |
| `--fixture` (qua `spark-defaults.conf` thật) | PASSED |
| `tests/integration/str07_checkpoint_restart.sh` | 3/3: lần đầu đọc đủ 2 valid + 10 invalid; restart chỉ đọc 2 message mới; restart không có message mới thì không đọc lại gì |
| Dữ liệu thật: `gbfs-poller` (GBFS API) → Kafka → job `--available-now` | `status` 2519 valid / 0 invalid; `information` 2519 valid / 0 invalid |
| Service `spark-streaming` (client mode lên `spark-master` + `spark-worker`) | Batch 0 của mỗi feed: 2519 valid / 0 invalid |

Các lần chạy dùng một Compose project riêng (`COMPOSE_PROJECT_NAME=citibike-itest`, đổi cổng MinIO/Iceberg REST/Spark UI) để không đụng stack đang dùng.

### Kết quả STR-08 (23/11/2026)

| Kiểm tra | Kết quả |
|---|---|
| Unit tests (`tests/streaming`) | 19/19 pass (thêm 5 test Bronze: giữ mọi message kể cả sai, `raw_value` nguyên văn, Kafka key, kiểu cột khớp bảng) |
| `tests/integration/str08_bronze.sh` | 3/3: 12 message (2 valid + 10 invalid) → 12 dòng, khớp end offset từng partition; restart với 3 message mới → 15 dòng; restart không có gì mới → 15 dòng, không thêm snapshot; 0 offset trùng |
| `str07_checkpoint_restart.sh` (hồi quy, `--sink console`) | 3/3 |
| Dữ liệu thật: `gbfs-poller` → Kafka → service `spark-streaming` (checkpoint `s3a://checkpoints/...`), có restart service giữa chừng | `bronze.gbfs_status`: 4 lần polling, 10076 dòng = tổng end offset Kafka (3368 + 3428 + 3280), 0 offset trùng, mỗi `poll_id` đủ 2519 trạm. `bronze.gbfs_station_information`: 2519 dòng = end offset (842 + 857 + 820) |

Chưa kiểm tra: đọc Bronze bằng Trino; crash giữa lúc commit Iceberg (STR-12).

### Kết quả STR-09 (23/11/2026)

| Kiểm tra | Kết quả |
|---|---|
| Unit tests (`tests/streaming`) | 31/31 pass (thêm 12 test Silver + DQ: chỉ message hợp lệ, không tin cờ Bronze, dedup, poll không đổi vẫn giữ, kiểu cột; DQ pass khi sạch, fail khi trùng / thiếu / thừa khóa) |
| `tests/integration/str09_silver.sh` | 7/7: replay dedup + poll không đổi giữ + invalid loại → 3 dòng; chạy lại không đổi; mất checkpoint Silver, đọc lại toàn bộ Bronze → vẫn 3 dòng; thêm poll mới + replay → 4 dòng; DQ pass, và exit 1 khi chèn dòng trùng |
| Dữ liệu thật: `gbfs-poller` → Kafka → service `spark-streaming` (Bronze + Silver), restart giữa chừng | 3 lần polling → `silver.gbfs_status` 7557 dòng = 3 × 2519, 7557 khóa khác nhau; `dq_checks --layer silver_status` 7/7 PASS |

Ghi chú: khi mất checkpoint Silver và đọc lại Bronze, MERGE không thêm dòng nhưng Iceberg vẫn ghi một snapshot rỗng.
Chưa kiểm tra: đọc Silver bằng Trino; xóa snapshot cũ của Bronze (maintenance) khi query Silver đang chạy.
