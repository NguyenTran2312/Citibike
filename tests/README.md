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
bash tests/integration/str08_bronze.sh   # Bronze: rows == Kafka end offset mỗi partition, restart không trùng
bash tests/integration/str09_silver.sh   # Silver: dedup, chạy lại / mất checkpoint không trùng, DQ gate bắt lỗi
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

### Dừng gbfs-poller giữa lần polling

`test_poller_shutdown.py` cho poller gọi GBFS giả lập (`gbfs_mock.py`, điều khiển
độ trễ và lỗi 503) rồi dừng nó ở các thời điểm khác nhau, sau đó đối chiếu Kafka,
MinIO, thư mục trên volume `gbfs-spool` và log của GBFS giả lập.

```bash
export COMPOSE_PROJECT_NAME=citibike-itest
export COMPOSE_FILE=docker-compose.yml:tests/integration/poller-shutdown.compose.yml
export GBFS_MOCK_CONTROL_DIR=$(mktemp -d)
# export MINIO_API_PORT=59000 MINIO_CONSOLE_PORT=59001   # nếu 9000/9001 đã bị chiếm
docker compose --profile pipeline build gbfs-poller
docker compose --profile pipeline up -d --wait kafka minio kafka-init minio-init gbfs-mock
python -m unittest tests.integration.test_poller_shutdown -v
docker compose --profile pipeline down -v
```

Mỗi lần polling phải **hoàn tất** (Kafka đủ mọi trạm, raw trên MinIO, thư mục đã
xóa) hoặc **được giữ lại** (thư mục còn `raw.json` để `--replay`); không có trạng
thái nào khác.

| Test | Kịch bản | Kết quả đo được |
|---|---|---|
| `test_1` | SIGTERM khi GBFS đang trả lời chậm (10 s) | Dừng sau ~12 s, exit 0, cả hai feed hoàn tất |
| `test_2` | SIGTERM khi GBFS treo quá timeout (cần ~93 s nếu retry đủ) | Dừng sau ~30 s, exit 0, không có dữ liệu ghi dở |
| `test_3` | SIGTERM khi Kafka treo | Dừng sau ~29 s, exit 0, hai lần polling được giữ lại; `--replay` đưa về hoàn tất |
| `test_4` | SIGKILL khi MinIO treo (đã gửi Kafka) | Exit 137, hai lần polling được giữ lại |
| `test_5` | Tạo lại container (`up --force-recreate`) | Thư mục giữ lại vẫn còn (volume `gbfs-spool`), và `--replay` trong container mới đưa chúng về hoàn tất. Bỏ volume thì test này fail |

Thời gian chạy khoảng 2,5 phút. Trước khi sửa, `test_2` (dừng sau ~92 s) và `test_5`
(mất thư mục giữ lại) đều fail.
