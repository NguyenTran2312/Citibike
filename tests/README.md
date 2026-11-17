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
