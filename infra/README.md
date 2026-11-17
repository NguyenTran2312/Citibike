# Hạ tầng Citi Bike theo scope.md

Compose dành cho môi trường phát triển trên một máy. Cấu hình sử dụng Kafka KRaft một broker, Spark 3.5.6 với Iceberg 1.9.2, Trino 484, Airflow 2.10.5 LocalExecutor và PostgreSQL 16.6. Đây là các lựa chọn triển khai của bản Compose.

## Khởi động bằng PowerShell

Mở Docker Desktop và bật Linux containers. Nên cấp khoảng 12–16 GB RAM cho Docker Desktop khi chạy toàn bộ hạ tầng. Phần CPU/RAM cần điều chỉnh theo kích thước job thực tế.

```powershell
Copy-Item .env.example .env
docker compose config --quiet
docker compose up -d
docker compose ps -a
```

Chỉnh các tài khoản trong `.env` trước khi khởi động. Cổng trên máy được bind vào `127.0.0.1`. Mật khẩu mặc định phục vụ phát triển cục bộ. Với mật khẩu Airflow metadata chứa ký tự đặc biệt trong URL, cần URL-encode giá trị trong chuỗi kết nối tương ứng.

## Thành phần và dữ liệu lưu bền

| Thành phần | Vai trò | URL trên máy |
|---|---|---|
| MinIO | Các bucket raw, warehouse, checkpoints và quarantine | API http://localhost:9000, console http://localhost:9001 |
| Iceberg REST | Danh mục dùng chung cho Spark và Trino | http://localhost:8181/v1/config |
| Kafka | Topic station_status, station_information và invalid | localhost:9092 |
| Spark master | Nhận job từ spark-submit | UI http://localhost:8081 |
| Spark worker | Chạy task, mặc định 2 core / 4 GB | UI http://localhost:8082 |
| Trino | SQL trên Iceberg, catalog iceberg | http://localhost:8080 |
| PostgreSQL | CSDL citibike phục vụ latest state và active alerts | localhost:5432 |
| Grafana | Datasource PostgreSQL và Trino được provision sẵn | http://localhost:3000 |
| Airflow | Scheduler, webserver và metadata DB riêng | http://localhost:8083 |

Các tài khoản lấy từ `.env.example` hoặc `.env`. Kafka dùng `kafka:29092` khi truy cập giữa các container. Spark dùng `spark://spark-master:7077`. Các endpoint nội bộ giữ nguyên khi thay cổng host.

MinIO, Kafka, PostgreSQL, Grafana, Airflow metadata/logs và REST Catalog có named volume. REST fixture dùng JdbcCatalog với SQLite lưu trong volume `catalog-data`. Phù hợp cho một instance phát triển, cần thay catalog backend khi triển khai nhiều replica. `catalog-volume-init` chỉ đặt quyền thư mục. `minio-init` và `kafka-init` tạo bucket/topic theo cách có thể chạy lại.

Image REST fixture chính thức hiện dùng tag `latest`, theo quickstart Apache Iceberg. Sau khi thử nghiệm, đặt `ICEBERG_REST_IMAGE=apache/iceberg-rest-fixture@sha256:...` để cố định bản image đã kiểm chứng. Grafana tải plugin `trino-datasource` ở lần khởi động, cần truy cập Internet. Hai thành phần này chưa khóa hoàn toàn phiên bản qua digest.

## Hợp đồng cho mã ứng dụng

Scope có hai nhánh consumer độc lập. Profile `pipeline` khai báo các container ứng dụng dưới đây. Các entry point này cần được nhóm triển khai trước khi bật profile.

| Service | Entry point cần có | Trách nhiệm |
|---|---|---|
| gbfs-poller | ingestion/gbfs/poller.py | Discovery GBFS 2.3, lấy hai feed theo TTL, lưu raw JSON, Pydantic validation và Kafka producer |
| latest-state-consumer | serving/latest_state_consumer.py | UPSERT observation mới hơn vào PostgreSQL, cập nhật station_status_latest và active_alerts theo infra/postgres/init.sql |
| spark-streaming | streaming/jobs/gbfs_history.py | Lưu Bronze/Silver Iceberg, dedup observation, checkpoint và replay |
| spark-batch | batch/jobs/trips_bronze.py và batch/jobs/trips_silver.py | Đọc manifest/raw trên MinIO, làm sạch và khử trùng ride_id, ghi Silver |

infra/postgres/init.sql khởi tạo hai bảng serving khi volume PostgreSQL còn mới. Compose chưa triển khai logic Python, dbt models, DAG hoặc các biểu đồ dashboard. Mỗi service ứng dụng được cấu hình sẵn biến môi trường, volume và dependency. Chạy profile khi entry point chưa tồn tại sẽ báo lỗi module/file bị thiếu.

```powershell
# Sau khi có mã cho cả ba ứng dụng liên tục
docker compose --profile pipeline up -d --build gbfs-poller latest-state-consumer spark-streaming

# Job batch một lần, sau khi triển khai batch/jobs/trips_bronze.py và batch/jobs/trips_silver.py
docker compose --profile tools run --rm spark-batch
docker compose --profile tools run --rm spark-batch-silver

# Đặt project dbt trong models/dbt/, có profile: citibike trong dbt_project.yml
docker compose --profile tools run --rm dbt debug
docker compose --profile tools run --rm dbt build
```

Image Python cài requirements tại build. Spark tải Iceberg, Kafka và Hadoop S3A jars từ Maven ở lần spark-submit đầu, cache ở `spark-ivy`. Job và executor sử dụng cùng image Spark. Prefix bảng Spark là `lakehouse.bronze`, `lakehouse.silver`, `lakehouse.gold`. Trino truy cập các namespace tương ứng bằng `iceberg.bronze`, `iceberg.silver`, `iceberg.gold`.

Airflow có thư mục DAG tại `infra/airflow/dags/`. Nhóm cần bổ sung DAG và provider/runtime phù hợp với operator lựa chọn để điều phối downloader, Spark, dbt và maintenance. LocalExecutor hiện cung cấp nền tảng scheduler/webserver và metadata DB. Các ứng dụng chạy liên tục là service riêng trong Compose.

## Quy tắc dữ liệu khi triển khai job

- Đọc CSV timestamps bằng schema có chủ đích, giữ giờ raw và local. Session Spark dùng UTC, còn múi giờ nguồn Trip History áp dụng America/New_York và xử lý AMBIGUOUS/NONEXISTENT.
- Mã station_id là chuỗi. Bridge nối Trip station_id với GBFS station_information.short_name, sau đó dùng station_id GBFS cho các feed.
- Consumer PostgreSQL và Spark có nhóm đọc Kafka độc lập. Spark checkpoint là nguồn quản lý offset cho job lịch sử. Job phải cấu hình khóa dedup và tính idempotent khi replay.
- `CHECKPOINT_LOCATION` sử dụng `s3a://` qua Hadoop S3A. Dữ liệu bảng Iceberg sử dụng `s3://` qua S3FileIO.
- Chu kỳ lấy GBFS mục tiêu 60 giây. Streaming trigger mặc định 5 phút, điều chỉnh bằng STREAM_TRIGGER_INTERVAL. Các job cần đọc các biến này.
- LOW_BIKES mặc định 2 xe, STALE mặc định đề xuất 180 giây. Tính STALE bằng thời gian hiện tại và last_reported, bao gồm lúc poller dừng. Consumer/SQL/Grafana cần triển khai phép tính và cơ chế cập nhật cảnh báo tương ứng.
- Khôi phục PostgreSQL từ Iceberg, compaction và đối soát được triển khai bằng job/DAG của nhóm. Dữ liệu PostgreSQL cập nhật trực tiếp từ Kafka consumer như scope.

## Kiểm tra và dừng

```powershell
docker compose --profile pipeline --profile tools config --quiet
docker compose logs --tail 100 iceberg-rest kafka trino airflow-scheduler
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --list
docker compose exec trino trino --execute "SHOW SCHEMAS FROM iceberg"
docker compose exec postgres psql -U citibike -d citibike -c "SELECT now();"
docker compose stop
```

Thay `citibike` bằng POSTGRES_USER khi đã đổi tài khoản. `stop` giữ các named volume để chạy tiếp. Bản Compose đã được kiểm tra bằng `docker compose config` cho cả profile `pipeline` và `tools`. Docker Engine tại máy tạo file chưa chạy nên chưa kiểm thử pull/build, healthcheck hoặc các kết nối giữa container.

## Tài liệu cấu hình đã đối chiếu

- [Apache Iceberg Spark quickstart](https://iceberg.apache.org/spark-quickstart/)
- [REST fixture Dockerfile](https://github.com/apache/iceberg/blob/main/docker/iceberg-rest-fixture/Dockerfile)
- [Apache Kafka 3.9 quickstart](https://kafka.apache.org/39/getting-started/quickstart/)
- [Trino S3 filesystem](https://trino.io/docs/current/object-storage/file-system-s3.html)
- [Airflow 2.10.5 Docker](https://airflow.apache.org/docs/apache-airflow/2.10.5/howto/docker-compose/index.html)
- [Grafana Trino datasource](https://github.com/trinodb/grafana-trino)

## Các đường dẫn theo cây thư mục dự án

- Poller chạy bằng `python -m ingestion.gbfs.poller` và sử dụng producer/schemas trong cùng package.
- Latest consumer chạy bằng `python -m serving.latest_state_consumer`.
- `spark-streaming` gọi `streaming/jobs/gbfs_history.py`. Entry point này cần khởi chạy các truy vấn Bronze/Silver và station information trong cùng driver. Mỗi truy vấn phải có checkpoint riêng dưới CHECKPOINT_LOCATION và giữ tiến trình bằng awaitAnyTermination.
- Các job Spark có PYTHONPATH=/opt/project, driver và worker dùng cùng Dockerfile và cùng đường dẫn mount.
- Job `spark-batch` ghi Bronze, `spark-batch-silver` ghi Silver. Chạy Bronze thành công trước Silver.
- `trip-downloader` chạy `ingestion/trip_history/downloader.py` với phạm vi tháng 1–10/2026.
- `latest-state-reconciliation` chạy `serving/reconcile_latest_state.py`, truy vấn Iceberg qua Trino và ghi PostgreSQL.
- dbt dùng project models/dbt/ và profile infra/dbt/profiles.yml. Nhóm cần tạo dbt_project.yml với profile citibike.
- Grafana tự đọc dashboard JSON tại infra/grafana/dashboards/ khi các file dashboard được bổ sung.
- init SQL chỉ chạy trên volume PostgreSQL mới. Với volume đã có dữ liệu, áp dụng migration phù hợp thay vì xóa volume.

Các lệnh chạy công cụ sau khi nhóm bổ sung mã ứng dụng:

```powershell
docker compose --profile tools run --rm trip-downloader
docker compose --profile tools run --rm latest-state-reconciliation
```

Mặc định một ứng dụng Spark chiếm tối đa một core, worker có hai core để batch và streaming có thể chia sẻ cluster. Điều chỉnh tài nguyên sau khi đo workload. Airflow hiện dùng image chính thức và LocalExecutor. DAG gọi Spark/dbt/container cần được triển khai với operator, provider và runtime tương ứng trước khi có thể chạy tác vụ. Các thư mục mã ứng dụng hiện là scaffold, chưa có logic pipeline.
