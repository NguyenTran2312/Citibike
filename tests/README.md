# Tests

| Thư mục | Loại | Cần Docker stack? |
|---|---|---|
| `smoke/` | Smoke test hạ tầng (INF-07) | Có |
| `streaming/` | Unit test cho Spark streaming job | Không (PySpark local) |
| `integration/` | Kiểm thử tích hợp theo issue (vd. STR-07 checkpoint restart) | Có |

## Smoke test hạ tầng

Chạy sau mỗi lần `docker compose up -d --build`, sau khi đổi `docker-compose.yml`/`infra/`, hoặc khi nghi hạ tầng có vấn đề:

```bash
bash tests/smoke/infra_smoke.sh
```

Script không cần cài gì trên host ngoài Docker; mọi lệnh chạy bằng `docker compose exec`. Các bước kiểm tra:

| Bước | Kiểm tra | Issue |
|---|---|---|
| 1 | Service healthy, job init `Exited (0)`, Spark master thấy worker | INF-01 |
| 2 | 4 bucket MinIO: raw, warehouse, checkpoints, quarantine | INF-02 |
| 3 | 3 topic Kafka, 3 partition, retention 7 ngày | INF-03 |
| 4 | Namespace Iceberg bronze/silver/gold; bảng serving PostgreSQL | INF-05 |
| 5 | Spark ghi/đọc `s3a://checkpoints` và append vào `lakehouse.smoke.e2e` | INF-04 |
| 6 | Trino đọc đúng số dòng Spark vừa ghi (**Spark → Iceberg → Trino**) | milestone |

Bảng `iceberg.smoke.e2e` bị xóa sau khi chạy; đặt `KEEP_SMOKE_TABLE=1` để giữ lại xem. Exit code khác 0 nếu có bước `[FAIL]`; log Spark khi lỗi được giữ lại ở đường dẫn in ra màn hình.

## Unit test streaming

Dùng `unittest` (image Spark không có pytest); xem thêm `streaming/README.md`:

```bash
docker compose exec -T -w /opt/project spark-master bash -c \
  'export PYTHONPATH=/opt/project:/opt/spark/python:$(ls /opt/spark/python/lib/py4j-*.zip); python3 -W ignore::ResourceWarning -m unittest discover -s tests/streaming -v'
```

## Integration

```bash
bash tests/integration/str07_checkpoint_restart.sh
```

### Latest State Consumer với Kafka và PostgreSQL thật (STR-05/06)

`test_latest_state_recovery.py` chạy trên một Compose project riêng nên không
đụng stack đang dùng. Cần Python trên host có `confluent-kafka`, `psycopg[binary]`
(xem `ingestion/gbfs/requirements.txt`, `infra/python/requirements.txt`).

```bash
export COMPOSE_PROJECT_NAME=citibike-itest
# export POSTGRES_PORT=55432   # nếu cổng 5432 trên host đã bị chiếm
docker compose --profile pipeline build gbfs-poller
docker compose --profile pipeline up -d --wait latest-state-consumer
python -m unittest tests.integration.test_latest_state_recovery -v
docker compose --profile pipeline down -v
```

| Test | Chứng minh |
|---|---|
| `test_1` | Message cũ hơn đến sau không ghi đè trạng thái mới hơn (out-of-order) |
| `test_2` | Gửi lại cùng message không ghi lại dòng (`xmin` không đổi) |
| `test_3` | PostgreSQL dừng 20 giây: offset không được commit, consumer bị Docker restart; PostgreSQL chạy lại thì consumer tự xử lý message đang chờ |
| `test_4` | Process consumer bị SIGKILL: Docker restart, đọc tiếp từ offset đã commit, không mất message |
| `test_5` | Message không phải JSON đi sang `gbfs.invalid` (kèm header offset gốc), consumer không bị kẹt |

Thời gian chạy khoảng 75 giây. Đã kiểm tra test bắt được lỗi bằng cách cố tình
sửa sai consumer: commit offset trước khi ghi (test_3 fail), bỏ điều kiện
`collected_at` trong UPSERT (test_1, test_2 fail), crash khi gặp message hỏng
(test_5 fail).
