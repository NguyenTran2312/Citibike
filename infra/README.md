# Hạ tầng Citi Bike (local Docker Compose)

Stack phát triển trên một máy: Kafka KRaft 3.9.1 (1 broker), Spark 3.5.6 + Iceberg 1.9.2, Iceberg REST Catalog, MinIO, PostgreSQL 16.6, Trino 484 và Grafana 12.3. Airflow 2.10.5 nằm ở profile riêng `airflow` (INF-09, bật khi cần).

Mọi lệnh dưới đây chạy ở thư mục gốc repo, trong shell bash (WSL/Linux/macOS). Trên PowerShell, các lệnh `docker compose` giữ nguyên, chỉ thay `cp` bằng `Copy-Item`.

## 1. Khởi động lần đầu (cả nhóm)

Yêu cầu: Docker Engine/Docker Desktop với Compose v2, cấp khoảng 8 GB RAM cho stack mặc định (12–16 GB nếu chạy thêm Airflow và các job).

```bash
cp .env.example .env              # sửa mật khẩu trong .env; .env đã nằm trong .gitignore
docker compose config --quiet     # kiểm tra cú pháp
docker compose up -d --build      # lần đầu cần Internet để pull image
docker compose ps -a              # các service phải ở trạng thái healthy / Exited (0)
bash tests/smoke/infra_smoke.sh   # smoke test toàn bộ hạ tầng + Spark -> Iceberg -> Trino
```

Kết quả `docker compose ps -a` mong đợi:

| Service | Trạng thái đúng |
|---|---|
| minio, iceberg-rest, kafka, postgres, spark-master, trino, grafana | `Up (healthy)` |
| spark-worker | `Up` |
| catalog-volume-init, minio-init, kafka-init, catalog-init | `Exited (0)`: job khởi tạo, chạy một lần, chạy lại an toàn |

Smoke test in `[PASS]/[FAIL]` cho từng hạng mục và kết thúc bằng `Smoke test: N passed, 0 failed`. Chi tiết xem [tests/README.md](../tests/README.md).

## 2. Thành phần

| Thành phần | Vai trò | Từ máy host | Trong mạng `citibike` |
|---|---|---|---|
| MinIO | Bucket `raw`, `warehouse`, `checkpoints`, `quarantine` | API http://localhost:9000, console http://localhost:9001 | `http://minio:9000` |
| Iceberg REST | Catalog dùng chung cho Spark và Trino; namespace `bronze`, `silver`, `gold` | http://localhost:8181/v1/config | `http://iceberg-rest:8181` |
| Kafka | Topic `gbfs.station_status`, `gbfs.station_information`, `gbfs.invalid` (3 partition, RF 1, giữ 7 ngày) | `localhost:9092` | `kafka:29092` |
| Spark master / worker | Cluster standalone, worker 2 core / 4 GB | UI http://localhost:8081, http://localhost:8082 | `spark://spark-master:7077` |
| PostgreSQL | DB `citibike`: `station_status_latest`, `active_alerts` | `localhost:5432` | `postgres:5432` |
| Trino | SQL trên Iceberg, catalog `iceberg` | http://localhost:8080 | `http://trino:8080` |
| Grafana | Datasource PostgreSQL + Trino provision sẵn | http://localhost:3000 | `http://grafana:3000` |
| Airflow (profile `airflow`) | Scheduler, webserver, metadata DB riêng | http://localhost:8083 | - |

Tài khoản lấy từ `.env`. Port host chỉ bind vào `127.0.0.1`.

Quy ước bảng: Spark dùng catalog `lakehouse` (catalog mặc định) nên viết `lakehouse.bronze.<table>` hoặc `bronze.<table>`; Trino đọc cùng bảng qua `iceberg.bronze.<table>`. Dữ liệu bảng Iceberg dùng `s3://warehouse/` (S3FileIO); checkpoint Structured Streaming dùng `s3a://checkpoints/...` (Hadoop S3A).

### Các job khởi tạo (chạy lại an toàn)

| Job | Script | Việc làm |
|---|---|---|
| `minio-init` | [minio/init_buckets.sh](minio/init_buckets.sh) | `mc mb --ignore-existing` cho 4 bucket |
| `kafka-init` | [kafka/init_topics.sh](kafka/init_topics.sh) | Tạo 3 topic và áp lại config retention/cleanup |
| `catalog-init` | [iceberg/init_namespaces.sh](iceberg/init_namespaces.sh) | Tạo namespace `bronze`, `silver`, `gold` qua REST API (409 = đã có) |
| `catalog-volume-init` | (inline) | Đặt quyền volume SQLite của catalog |
| PostgreSQL init | [postgres/init.sql](postgres/init.sql) | Tạo bảng serving, **chỉ chạy khi volume `postgres-data` còn mới** |

## 3. Lệnh cho từng thành viên

Các entry point dưới đây đã khai báo trong Compose (env, volume, dependency). Chúng chỉ chạy được khi module ứng dụng tương ứng đã được viết; nếu chưa có file, container sẽ báo lỗi thiếu module/file.

**Khang (Batch)**: cần `ingestion/trip_history/downloader.py`, `batch/jobs/trips_bronze.py`, `batch/jobs/trips_silver.py`:

```bash
docker compose --profile tools run --rm trip-downloader
docker compose --profile tools run --rm spark-batch
docker compose --profile tools run --rm spark-batch-silver
```

**Quan (Streaming ingestion)**: cần `ingestion/gbfs/poller.py`, `serving/latest_state_consumer.py`:

```bash
docker compose --profile pipeline up -d --build gbfs-poller latest-state-consumer
```

**Nguyen (Spark Streaming)**: `streaming/jobs/gbfs_history.py`:

```bash
docker compose --profile pipeline up -d spark-streaming
```

**Kien (dbt)**: cần `models/dbt/dbt_project.yml` với `profile: citibike`:

```bash
docker compose --profile tools run --rm dbt debug --profiles-dir /app/profiles
docker compose --profile tools run --rm dbt build --profiles-dir /app/profiles
```

Lần đầu chạy, image Python (`citibike/python:local`) được build từ [python/requirements.txt](python/requirements.txt); Spark tải jar Iceberg/Kafka/S3A từ Maven ở lần `spark-submit` đầu và cache vào volume `spark-ivy`.

Truy vấn nhanh để kiểm tra kết quả:

```bash
docker compose exec trino trino --execute "SHOW TABLES FROM iceberg.silver"
docker compose exec postgres psql -U citibike -d citibike -c "SELECT count(*) FROM station_status_latest"
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic gbfs.station_status --from-beginning --max-messages 5
```

(Thay `citibike` bằng `POSTGRES_USER` nếu đã đổi trong `.env`.)

## 4. Biến môi trường

Danh sách đầy đủ và giải thích nằm trong [.env.example](../.env.example). Mọi biến đều có giá trị mặc định cho local trong `docker-compose.yml`, nên stack vẫn chạy khi chưa có `.env`; tuy vậy nên tạo `.env` để đặt mật khẩu riêng. Không commit `.env`.

Lưu ý: user/password PostgreSQL và Airflow DB chỉ được áp dụng khi volume được tạo lần đầu. Đổi mật khẩu sau đó cần đổi trong DB, hoặc xóa volume nếu chấp nhận mất dữ liệu.

## 5. Xử lý sự cố

| Triệu chứng | Nguyên nhân và cách xử lý |
|---|---|
| `Bind for 127.0.0.1:5432 failed: port is already allocated` hoặc postgres không lên | Port đang bị chiếm (thường là PostgreSQL cài sẵn trong WSL: `sudo service postgresql stop`, hoặc container lab cũ: `docker stop <tên>`). Giữ port mặc định, dừng tiến trình đang chiếm port thay vì đổi port. Kiểm tra: `ss -ltnp \| grep 5432` |
| Port 9092/8080/… bị chiếm | Như trên. `docker ps` để tìm container cũ (vd. lab Kafka) rồi `docker stop` |
| `pull access denied for minio/minio` | MinIO đã ngừng phát hành image Docker. Compose dùng `bitnamilegacy/minio:2025.4.22` (cùng commit với RELEASE.2025-04-22). Không đổi lại `minio/minio` |
| `catalog-init` hoặc `trino` không lên | Xem `docker compose logs iceberg-rest catalog-init`; iceberg-rest cần MinIO đã healthy và bucket đã tạo |
| Spark job treo ở `Initial job has not accepted any resources` | Worker hết core (mỗi app dùng tối đa 1 core, worker có 2). Dừng bớt job hoặc tăng `SPARK_WORKER_CORES` |
| `error mounting ... to rootfs at "/init_topics.sh"` (Docker Desktop + WSL) | Container tạo từ bản Compose cũ (mount từng file) giữ mount tới file trước khi git ghi lại. Bản hiện tại chỉ mount thư mục nên không còn lỗi này; với container cũ, tạo lại: `docker compose up -d --force-recreate`. Khi thêm mount mới, luôn mount thư mục, không mount từng file |
| `No such image: citibike/spark:3.5.6-local` khi chạy job profile `tools`/`pipeline` | Chưa build image Spark: chạy `docker compose up -d --build` (stack mặc định) trước |
| Grafana không cài được `trino-datasource` | Plugin tải từ Internet lúc khởi động; kiểm tra mạng rồi `docker compose restart grafana` |

Xem log: `docker compose logs --tail 100 <service>`.

## 6. Dừng và dọn dẹp

```bash
docker compose stop                 # dừng, giữ dữ liệu
docker compose down                 # xóa container, giữ named volumes
docker compose down -v              # XÓA toàn bộ dữ liệu (MinIO, Kafka, catalog, Postgres...)
```

## 7. Ghi chú thiết kế

- **Iceberg REST Catalog** dùng image fixture chính thức (`apache/iceberg-rest-fixture`, khóa theo digest) với JdbcCatalog trên SQLite trong volume `catalog-data`. Phù hợp cho một instance phát triển; triển khai nhiều replica cần catalog backend khác (xem mục HA trong `docs/contracts.md`).
- **MinIO** dùng bản build Bitnami legacy vì image `minio/minio` không còn tải được; phiên bản không đổi so với cấu hình ban đầu.
- **Spark**: driver và executor dùng chung image [spark/Dockerfile](spark/Dockerfile), cùng `PYTHONPATH=/opt/project` và các mount `batch/`, `streaming/`, `tests/`. `spark.cores.max=1` để batch và streaming chia sẻ worker 2 core.
- **Build image**: mỗi image chỉ do một service build (`spark-master` cho `citibike/spark`, `gbfs-poller`/các tool một lần cho `citibike/python`), các service khác dùng lại với `pull_policy: never`. Nếu nhiều service cùng build một tag, Compose gắn nhãn tên service vào image nên các bản build ghi đè nhau và mỗi lần `up -d --build` sẽ tạo lại container (làm đứt job streaming). Vì vậy các service profile `tools`/`pipeline` dùng image Spark cần stack mặc định đã được `up` ít nhất một lần.
- **Airflow** (INF-09) chưa có DAG; bật bằng `docker compose --profile airflow up -d`. DAG đặt trong `infra/airflow/dags/`.
- **Grafana/dbt** (INF-06): datasource và profile đã có; dashboard JSON đặt trong `infra/grafana/dashboards/`.

## Tài liệu tham chiếu

- [Apache Iceberg Spark quickstart](https://iceberg.apache.org/spark-quickstart/)
- [Iceberg REST fixture](https://github.com/apache/iceberg/blob/main/docker/iceberg-rest-fixture/Dockerfile)
- [Apache Kafka 3.9 quickstart](https://kafka.apache.org/39/getting-started/quickstart/)
- [Trino S3 file system](https://trino.io/docs/current/object-storage/file-system-s3.html)
- [Airflow 2.10.5 Docker Compose](https://airflow.apache.org/docs/apache-airflow/2.10.5/howto/docker-compose/index.html)
- [Grafana Trino datasource](https://github.com/trinodb/grafana-trino)
