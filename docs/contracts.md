# Citi Bike Big Data — Data & Infrastructure Contracts

> **Mục đích:** Tài liệu thống nhất cách trao đổi dữ liệu và vận hành các module Batch, Streaming, Storage và Analytics trước khi tích hợp.
>
> **Phạm vi:** Môi trường phát triển local bằng Docker Compose; các mục tiêu High Availability (HA) được ghi riêng, **không đồng nghĩa cấu hình local hiện tại đã đạt HA**.
>
> **Trạng thái tài liệu:** Bản sắp xếp lại từ ghi chú họp nhóm. Các mục chưa được xác minh hoặc chưa quyết định được đánh dấu **Đề xuất** / **Pending**; không tự động coi là đã chốt.

## Mục lục

1. [Data Contracts](#1-data-contracts)
2. [Infrastructure Contracts](#2-infrastructure-contracts)
3. [CI, kiểm thử và tiêu chí tích hợp](#3-ci-kiểm-thử-và-tiêu-chí-tích-hợp)
4. [Phân công và phụ thuộc giữa các thành viên](#4-phân-công-và-phụ-thuộc-giữa-các-thành-viên)
5. [Danh sách quyết định còn mở](#5-danh-sách-quyết-định-còn-mở)

---

## 1. Data Contracts

### 1.1. Trip History — Schema nguồn (13 trường)

**Phối hợp:** Khang (Batch) và Kien (Data Modeling / Data Quality).

| # | Trường nguồn | Kiểu dữ liệu tại đầu vào | Nullable |
|---:|---|---|---|
| 1 | Ride ID | `string` | Chưa quy định trong ghi chú |
| 2 | Rideable type | `string` | Chưa quy định |
| 3 | Started at | `string` | Chưa quy định |
| 4 | Ended at | `string` | Chưa quy định |
| 5 | Start station name | `string` | Có |
| 6 | Start station ID | `string` | Có |
| 7 | End station name | `string` | Có |
| 8 | End station ID | `string` | Có |
| 9 | Start latitude | `string` | Có |
| 10 | Start longitude | `string` | Có |
| 11 | End latitude | `string` | Có |
| 12 | End Longitude | `string` | Có |
| 13 | Member or casual ride | `string` | Có |

> Các kiểu dữ liệu trên phản ánh **schema đầu vào** trong ghi chú. Quy tắc chuyển kiểu ở Silver (ví dụ tọa độ) chưa được chốt tại đây.

#### Quy tắc dedup và xử lý xung đột `ride_id`

| Tình huống | Xử lý |
|---|---|
| Cùng `ride_id`, nội dung bản ghi giống nhau | Dedup thông thường. |
| Cùng `ride_id`, nội dung khác nhau | Lưu bản ghi gây xung đột vào **file/dataset riêng để audit**, không âm thầm gộp. |
| `start_station_id` hoặc `end_station_id` là `NULL` | **Giữ nguyên `NULL`**, không tự điền và không xóa hàng chỉ vì thiếu station ID. |

**Schema audit cho `ride_id` conflict:**

| Trường | Ý nghĩa |
|---|---|
| `ride_id` | Ride ID gây xung đột |
| `record_hash` | Dấu vân tay của nội dung bản ghi |
| `source_month` | Tháng dữ liệu nguồn |
| `source_file` | File nguồn chứa bản ghi |
| `row_number` | Vị trí dòng trong file nguồn |
| `detected_at` | Thời điểm phát hiện conflict |
| `conflict_fields` | Các trường khác biệt giữa các bản ghi cùng Ride ID |
| `resolution_status` | Trạng thái xử lý conflict |
| `resolution_reason` | Lý do hoặc kết quả xử lý |

**Pending:** Vị trí lưu audit file, định dạng file, và chính sách đưa các `ride_id` có conflict vào/ra khỏi Silver chưa được ghi rõ trong nguồn.

### 1.2. Timezone và Daylight Saving Time (DST)

**Timezone chuẩn:** `America/New_York`.

| Khoảng thời gian địa phương (năm 2026) | Quy tắc |
|---|---|
| Trước **08/03/2026 02:00** | UTC−5 |
| **08/03/2026 02:00–02:59** | Giờ địa phương không tồn tại do chuyển sang DST; cần đánh dấu **INVALID / NONEXISTENT** khi gặp. |
| Từ **08/03/2026 03:00** đến hết tháng 10 | UTC−4 |
| **01/11/2026 01:00–01:59** | Giờ địa phương lặp lại; có thể tương ứng UTC−4 hoặc UTC−5. Đánh dấu **AMBIGUOUS**, không tự chọn offset. |

**Ghi nhận từ dữ liệu đã kiểm tra:** Có **36 giá trị `ended_at`** trong khoảng `01:00–01:59` ngày 01/11/2026 được ghi nhận là ambiguous. Đây là số lượng quan sát trong ghi chú, không phải quy tắc chỉ áp dụng cho `ended_at`: cần kiểm tra cả `started_at` nếu nằm trong khoảng tương tự.

#### Chính sách theo tầng dữ liệu

| Tầng | Quy tắc |
|---|---|
| **Bronze** | Giữ `started_at` và `ended_at` ở dạng `string` để bảo toàn dữ liệu gốc. |
| **Silver** | Parse sang local timestamp; tạo các cột phục vụ phân tích như `start_date`, `start_hour`; đánh dấu ambiguous hoặc invalid. |
| **UTC** | Chỉ tạo UTC timestamp khi xác định được thời điểm chính xác; không suy đoán offset. |
| **Gold** | Tính số chuyến theo giờ địa phương New York; phải quy định cách xử lý thiếu timestamp/ambiguous. |

**Đề xuất cần chốt:** Nếu `ended_at < started_at`, đánh dấu `INVALID` hoặc `AMBIGUOUS` theo policy sau khi đã xét vấn đề DST, thay vì xóa ngay.

### 1.3. Iceberg Silver — `silver.trips`

**Grain:** `1 dòng = 1 ride_id hợp lệ`.

| Cột | Mục đích |
|---|---|
| `ride_id` | Định danh duy nhất một chuyến xe |
| `started_at` | Thời gian bắt đầu |
| `ended_at` | Thời gian kết thúc |
| `start_station_id` | Trạm xuất phát |
| `end_station_id` | Trạm kết thúc |
| `rideable_type` | Loại xe |
| `source_month` | Tháng dữ liệu nguồn |

**Yêu cầu thực thi:** Spark Batch phải có logic dedup và kiểm tra Data Quality; việc ghi grain trong tài liệu **không tự bảo đảm** bảng thực tế không có duplicate.

### 1.4. GBFS — `station_status`

**Phối hợp:** Quan (Streaming) và Kien (Schema / Data Quality).

**Schema hiện thực:** Pydantic `ingestion/gbfs/schemas.py` (`StationStatusMessage`), xuất ra `ingestion/gbfs/json_schemas/station_status.schema.json` bằng `python -m ingestion.gbfs.export_schemas`. Spark đọc cùng message theo `streaming/utils/schemas.py` (`STATION_STATUS_SCHEMA`) và luật trong `streaming/jobs/gbfs_history.py` (`status_rules`). Test `tests/streaming/test_gbfs_history.py` (`test_matches_producer_json_schemas`, `test_types_match_producer_json_schemas`, `test_required_fields_are_enforced_by_spark_rules`) giữ hai phía khớp nhau về tên, kiểu và trường bắt buộc. Khi schema đổi, sửa mục này trong cùng PR.

**Message trên topic `gbfs.station_status`** (1 message = 1 trạm trong 1 lần polling):

```json
{
  "last_updated": 1794796800,
  "collected_at": "2026-11-16T02:40:00.123456Z",
  "poll_id": "station_status-3f2b8c1e-5d7a-4e0b-9c61-2a4f0e8d9b17",
  "schema_version": 1,
  "station_id": "abc",
  "num_bikes_available": 5,
  "num_docks_available": 12,
  "is_installed": true,
  "is_renting": true,
  "is_returning": true,
  "last_reported": 1794796740,
  "vehicle_types_available": [{"vehicle_type_id": "1", "count": 5}]
}
```

| Field | Kiểu JSON | Bắt buộc | Quy tắc | Ngữ nghĩa |
|---|---|---|---|---|
| `poll_id` | string | Có | Không rỗng; poller sinh `station_status-<uuid4>` | Một giá trị cho mọi trạm của một lần polling; giữ nguyên khi replay |
| `last_updated` | integer | Có | Epoch giây, `0 ≤ x ≤ 4102444800` | Thời điểm feed được sinh (metadata cấp feed) |
| `collected_at` | string | Có | RFC 3339, có timezone (UTC, `Z`) | Thời điểm poller nhận response; một giá trị cho cả lần polling; giữ nguyên khi replay |
| `schema_version` | integer | Không (producer luôn ghi) | `= 1` | Phiên bản message |
| `station_id` | string | Có | Không rỗng | Khóa Kafka |
| `num_bikes_available` | integer | Có | `≥ 0`; `NULL` bị đưa vào `gbfs.invalid` | — |
| `num_docks_available` | integer | Có | `≥ 0` | — |
| `is_installed`, `is_renting`, `is_returning` | boolean | Có | Nguồn gửi 0/1 hoặc boolean, message luôn là boolean | — |
| `last_reported` | integer | Có | Epoch giây, `0 ≤ x ≤ 4102444800` | Lần trạm báo cáo gần nhất; dùng cho STALE |
| `vehicle_types_available` | array hoặc `null` | Không | Mỗi phần tử: `vehicle_type_id` chuỗi không rỗng, `count ≥ 0` | — |

**Iceberg tables:** `lakehouse.bronze.gbfs_status`, `lakehouse.silver.gbfs_status` (Trino: `iceberg.bronze.gbfs_status`, `iceberg.silver.gbfs_status`); chi tiết ở mục 1.8.

**Grain / khóa logic:** `(station_id, poll_id)`.

**Quy tắc:**

- Lưu observation cho từng lần polling khác nhau, **kể cả khi trạng thái không đổi**.
- Nếu retry phát lại **cùng `(station_id, poll_id)`**, dedup để chỉ còn một observation logic.
- Message chỉ chứa các field ở bảng trên. Các field nguồn khác (ví dụ `num_bikes_disabled`, `num_docks_disabled`, `num_ebikes_available`) chỉ có trong raw JSON (mục 2.3.5). **Pending (mục 5):** Kien + Quan xác nhận tập field này; thêm field sau là thay đổi additive, nhưng dữ liệu trước đó chỉ backfill được từ raw JSON.
- `num_bikes_available = NULL`: hiện thực đang **reject** (producer và consumer gửi sang `gbfs.invalid`; Spark đánh dấu `VALIDATION_ERROR`). **Pending:** chờ Kien xác nhận (mục 5).
- Spark jobs xử lý dedup và có Data Quality tests kiểm chứng (`streaming/jobs/dq_checks.py`).

### 1.5. GBFS — `station_information`

**Mục tiêu:** Lưu dữ liệu thông tin trạm để liên kết với status, hiển thị dashboard và hỗ trợ mapping.

**Schema hiện thực:** `StationInformationMessage` trong `ingestion/gbfs/schemas.py`, xuất ra `ingestion/gbfs/json_schemas/station_information.schema.json`; phía Spark là `STATION_INFORMATION_SCHEMA` và `information_rules`.

| Field | Kiểu JSON | Bắt buộc | Quy tắc / mục đích |
|---|---|---|---|
| `poll_id` | string | Có | Không rỗng; poller sinh `station_information-<uuid4>`, **không dùng chung** với `station_status` |
| `last_updated` | integer | Có | Epoch giây, `0 ≤ x ≤ 4102444800` |
| `collected_at` | string | Có | RFC 3339, có timezone; một giá trị cho cả lần polling |
| `station_id` | string | Có | Không rỗng; join với station status; khóa Kafka |
| `name` | string | Có | Không rỗng; tên trạm trên dashboard |
| `lat`, `lon` | number | Có | `[-90, 90]`, `[-180, 180]`; không NaN/Infinity |
| `short_name` | string hoặc `null` | Không | Mapping với Trip History station ID |
| `capacity` | integer hoặc `null` | Không | `≥ 0` |
| `is_virtual_station` | boolean hoặc `null` | Không | Phân biệt trạm ảo/vật lý (nguồn hiện không gửi) |
| `region_id` | string hoặc `null` | Không | Field nguồn |
| `rental_uris` | object hoặc `null` | Không | `{android, ios, web}`, mỗi giá trị string hoặc `null` |
| `is_charging` | boolean hoặc `null` | Không | Field nguồn |

Message `station_information` không có `schema_version`.

**Polling:** Mỗi **24 giờ**, và một lần mỗi khi poller khởi động.

**Iceberg tables:** `lakehouse.bronze.gbfs_station_information`, `lakehouse.silver.gbfs_station_information` (mục 1.8).

**Grain (hiện thực):** `(station_id, poll_id)`; mỗi lần polling là một snapshot đầy đủ danh sách trạm, giữ nguyên như quan sát được. **Pending (mục 5):** Kien + Quan xác nhận grain/snapshot policy và có cần refresh giữa hai lần poll 24 giờ không.

### 1.6. Gold metrics và station mapping

- **Gold:** Chốt **grain của từng metric** trước khi viết SQL; công thức và số liệu phải đối chiếu với **fixture mẫu**.
- **Station mapping:** Kien mở issue nghiên cứu riêng; chưa tự coi các quy tắc mapping là đã hoàn tất.

### 1.7. PostgreSQL serving — `station_status_latest` và `active_alerts`

**Schema:** `infra/postgres/init.sql` (chạy khi volume `postgres-data` được tạo lần đầu). **Ghi:** `serving/latest_state_consumer.py` (service `latest-state-consumer`, consumer group `citibike-postgres-latest`), đọc `gbfs.station_status`. Không dùng CDC.

#### `station_status_latest` (đã hiện thực)

**Grain:** 1 dòng = trạng thái mới nhất của 1 `station_id` (khóa chính).

| Cột | Kiểu | Nguồn |
|---|---|---|
| `station_id` | `TEXT` PK | message |
| `poll_id` | `TEXT NOT NULL` | message |
| `num_bikes_available` | `INTEGER NOT NULL`, ≥ 0 | message |
| `num_docks_available` | `INTEGER`, ≥ 0 | message |
| `is_installed`, `is_renting`, `is_returning` | `BOOLEAN NOT NULL` | message |
| `last_updated`, `last_reported` | `TIMESTAMPTZ` | epoch giây trong message, đổi bằng `to_timestamp` |
| `collected_at` | `TIMESTAMPTZ NOT NULL` | message, giữ nguyên độ chính xác microsecond |

**Quy tắc:**

- UPSERT chỉ ghi đè khi `collected_at` của message **mới hơn** dòng hiện có (`WHERE EXCLUDED.collected_at > current.collected_at`); message cũ hơn hoặc replay cùng `collected_at` không đổi dòng.
- Thứ tự commit: transaction PostgreSQL commit trước, rồi mới commit Kafka offset của các message trong transaction đó (mục 2.3.4).
- Message sai schema (`CONSUMER_VALIDATION_ERROR`) hoặc bị PostgreSQL từ chối (`DATABASE_REJECTED`) được gửi sang `gbfs.invalid` kèm header `source_topic`, `source_partition`, `source_offset`; offset chỉ được commit sau khi broker xác nhận message invalid.
- Mất kết nối PostgreSQL hoặc Kafka: consumer dừng với lỗi, Docker restart (`restart: unless-stopped`), offset chưa commit được đọc lại.

#### `active_alerts` (chưa hiện thực — STR-05b, issue #39)

Bảng đã có trong `init.sql` nhưng **chưa có code ghi**. Contract đề xuất cho STR-05b:

| Mục | Đề xuất | Trạng thái |
|---|---|---|
| Grain | 1 dòng = 1 cảnh báo đang mở của 1 trạm, khóa `(station_id, alert_type)`; trạm không có dòng = `NORMAL` | Theo #39 |
| `LOW_BIKES` | `0 < num_bikes_available <= LOW_BIKES_THRESHOLD` (mặc định 2) | Theo scope |
| `EMPTY` | `num_bikes_available = 0` và `is_renting = true` | Theo scope |
| `UNAVAILABLE` | Trạm không hoạt động hoặc không cho thuê/trả theo cờ `is_*` | **Pending** (Quan + Kien) |
| `STALE` | `now() - last_reported > STALE_AFTER_SECONDS` (mặc định 180), job định kỳ chạy cả khi không có message mới | Công thức theo scope; cơ chế chạy **Pending** |
| `opened_at` / `updated_at` | Theo `collected_at` hay `now()` | **Pending** (#39) |
| Transaction | Cảnh báo theo message mở/đóng trong cùng transaction với UPSERT, chỉ khi UPSERT thực sự cập nhật dòng; replay không đổi `opened_at` | Theo #39 |

`LOW_BIKES_THRESHOLD` và `STALE_AFTER_SECONDS` đã được truyền cho `latest-state-consumer` trong `docker-compose.yml` nhưng code hiện tại chưa dùng.

### 1.8. Iceberg streaming tables (GBFS)

Catalog Spark `lakehouse` và catalog Trino `iceberg` trỏ cùng Iceberg REST Catalog. Mọi timestamp là UTC. Các cột bắt buộc được bảo đảm bằng validation trước khi ghi; bảng không khai báo `NOT NULL`.

| Bảng | Grain / khóa | Ghi bởi | Partition | Ghi chú |
|---|---|---|---|---|
| `bronze.gbfs_status`, `bronze.gbfs_station_information` | 1 dòng = 1 Kafka message (kể cả message sai); khóa `(kafka_topic, kafka_partition, kafka_offset, kafka_timestamp)` | Query `gbfs_status_bronze`, `gbfs_information_bronze` (`streaming/jobs/gbfs_history.py`, `BRONZE_DDL`) | `days(ingested_at)` | Append-only. Cột: Kafka metadata, `raw_value` nguyên văn, `poll_id`, `station_id`, `collected_at`, `dq_is_valid`, `dq_error_type`, `dq_error_message`, `ingested_at`. Batch chạy lại chỉ append message chưa có |
| `silver.gbfs_status` | `(station_id, poll_id)` | Query `gbfs_status_silver` (`streaming/jobs/silver_status.py`) | `days(collected_at)` | Mọi field của mục 1.4 (epoch → `TIMESTAMP`) + lineage `kafka_partition`, `kafka_offset`, `bronze_ingested_at`, `silver_processed_at` |
| `silver.gbfs_station_information` | `(station_id, poll_id)` | Query `gbfs_information_silver` (`streaming/jobs/silver_information.py`) | `months(collected_at)` | Mọi field của mục 1.5 + lineage như trên |

- Silver parse và validate lại `raw_value` của Bronze (không tin cờ `dq_is_valid`), giữ một dòng mỗi khóa (Kafka position thấp nhất), ghi bằng `MERGE … WHEN NOT MATCHED THEN INSERT`: batch chạy lại, mất checkpoint Silver hoặc `--rebuild` không tạo dòng trùng.
- Message mà Spark coi là sai (`PARSE_ERROR`, `SCHEMA_ERROR`, `VALIDATION_ERROR`) **không** được gửi sang `gbfs.invalid`; chúng nằm trong Bronze với `dq_is_valid = false`.
- Một writer cho mỗi bảng. Không expire snapshot Bronze mà checkpoint Silver chưa đọc; không `DELETE` trên Bronze khi Silver đang stream (xem `streaming/README.md`, "Giả định phạm vi").

---

## 2. Infrastructure Contracts

### 2.1. Môi trường local và định hướng High Availability

| Thành phần | Local trong ghi chú ban đầu | Định hướng HA |
|---|---|---|
| **Kafka** | 1 broker | Nhiều broker, replication |
| **Spark** | 1 master, 1 worker | Nhiều worker, phục hồi driver/jobs và HA control plane |
| **MinIO** | 1 instance | Distributed object storage hoặc managed S3 |
| **PostgreSQL** | 1 instance cho mỗi database | Replication, failover, backups |
| **Iceberg REST** | 1 fixture + SQLite | Production REST Catalog + HA database |
| **Airflow** | 1 scheduler | HA schedulers, workers và metadata database |

**Phạm vi:** Bảng này ghi lại trạng thái local tại thời điểm lập contract và **mục tiêu nâng cấp**. Không khẳng định Compose hiện tại đã thực hiện mọi hạng mục HA. Khi chạy tất cả container trên một máy, vẫn còn single point of failure ở host.

### 2.2. Nguyen ↔ Khang — Batch Runtime & Storage

#### 2.2.1. MinIO raw storage

**Quy ước đường dẫn ZIP và manifest:**

```text
s3://raw/trip_history/nyc/year=2026/month=01/
├── 202601-citibike-tripdata.zip
└── manifest.json
```

**CSV sau giải nén — đường dẫn đề xuất:**

```text
s3a://raw/trip_history/staging/2026-01/*.csv
```

**Luồng xử lý ZIP:**

```text
Nguồn Trip History
    → Python Downloader
    → MinIO (raw ZIP + manifest)
    → tải ZIP về /tmp trong container
    → Python giải nén / xử lý CSV
    → upload CSV staging lên MinIO
    → Spark đọc CSV
```

**Quy tắc:** Không đưa file ZIP trực tiếp vào Spark CSV reader.

#### 2.2.2. `manifest.json` — thông tin bắt buộc

| Trường thông tin | Mô tả |
|---|---|
| URL nguồn | Đường dẫn tải archive |
| Tên archive | Tên file ZIP |
| Kích thước | Size của file |
| SHA-256 | Checksum của nội dung |
| Danh sách member | Các file trong ZIP |
| Header từng CSV | Danh sách cột của từng CSV |
| Row count | Số dòng khi đã kiểm kê |
| Trạng thái xử lý | Download / verify / extraction / processing status theo quy ước sẽ chốt |

**Yêu cầu vận hành:**

- Xác nhận download hoàn tất trước khi coi archive là hợp lệ; tránh dùng file tải dở.
- Tự tính checksum để phát hiện archive thay đổi giữa các lần tải và kiểm tra tính nhất quán của object.
- Chạy lại theo tháng phải **idempotent**, tránh tạo dữ liệu trùng khi retry.

#### 2.2.3. Batch job execution và smoke test

**Thứ tự pipeline:**

```text
Downloader → MinIO (S3) → Spark Bronze → Spark Silver → Iceberg → Trino
```

**Thử nghiệm trước với một tháng sample** và kiểm tra:

| Kết nối | Câu hỏi nghiệm thu |
|---|---|
| Spark ↔ MinIO | Spark container có kết nối và đọc dữ liệu trong bucket được không? |
| Spark ↔ Iceberg | Spark có tạo bảng và ghi dữ liệu qua Iceberg REST Catalog thành công không? |
| Trino ↔ Iceberg | Trino có đọc được đúng bảng Spark vừa ghi không? |

**Pending:** Airflow hiện chưa có job-launch mechanism hoàn chỉnh. **Đề xuất** dùng Docker CLI để khởi chạy container/job; cần review cách cấp quyền và bảo mật trước khi triển khai.

### 2.3. Nguyen ↔ Quan — Kafka Runtime & Ingestion

#### 2.3.1. Kafka local

- **Phiên bản:** Kafka `3.9.1`.
- **Số broker local trong ghi chú:** `1`.
- **Replication factor local:** `1`.
- **Trade-off:** Tiết kiệm tài nguyên nhưng khả năng chịu lỗi kém hơn HA cluster.
- **Partitions:** Đã hiện thực **3 partitions/topic** (`infra/kafka/init_topics.sh`); con số cuối cùng chờ benchmark (mục 5).
- **Retention:** 7 ngày (`retention.ms=604800000`), `cleanup.policy=delete`; auto-create topic tắt.
- **Message key:** `station_id` cho `gbfs.station_status` và `gbfs.station_information`; `gbfs.invalid` dùng `station_id` nếu payload có, ngược lại key rỗng.
- **Producer:** `enable.idempotence=true`, `acks=all` (`ingestion/gbfs/producer.py`). Delivery là at-least-once theo lần polling: một lần polling bị giữ lại được gửi lại nguyên vẹn (`--replay`) với cùng `poll_id` và `collected_at`, nên downstream phải dedup theo `(station_id, poll_id)`.

**Ba Kafka topics:**

| Topic | Mục đích |
|---|---|
| `gbfs.station_status` | Observation của trạng thái trạm |
| `gbfs.station_information` | Thông tin trạm |
| `gbfs.invalid` | Message không parse được hoặc vi phạm validation contract |

#### 2.3.2. `gbfs.invalid` contract

| Field | Ý nghĩa |
|---|---|
| `source` | Topic/feed nguồn |
| `error_type` | Loại lỗi |
| `error_message` | Diễn giải lỗi |
| `raw_payload` | Payload gây lỗi |
| `detected_at` | Thời điểm phát hiện lỗi |

**Ví dụ:**

```json
{
  "source": "gbfs.station_status",
  "error_type": "VALIDATION_ERROR",
  "error_message": "Missing station_id",
  "raw_payload": {
    "num_bikes_available": 5
  },
  "detected_at": "2026-11-16T08:00:00Z"
}
```

**Quy tắc:**

- Chỉ gửi vào `gbfs.invalid` khi parse/validation thất bại.
- Lỗi network hoặc service tạm mất kết nối cần đi qua luồng **retry**, không coi là payload invalid.
- Retry phải có số lần giới hạn và **jitter**; nếu hết giới hạn thì ghi log và phát alert.
- **Hiện thực:** schema `InvalidMessage` (`ingestion/gbfs/schemas.py`, `additionalProperties: false`). `error_type`: `VALIDATION_ERROR` (poller/producer), `CONSUMER_VALIDATION_ERROR` và `DATABASE_REJECTED` (latest-state consumer). Consumer thêm Kafka headers `source_topic`, `source_partition`, `source_offset`.
- Không có consumer nào đọc topic này vào Iceberg; message giữ 7 ngày. Một message invalid có thể xuất hiện nhiều lần khi consumer đọc lại một lô sau crash.
- Message sai mà Spark phát hiện không đi qua topic này (mục 1.8).

#### 2.3.3. Polling và Spark micro-batch

| Hạng mục | Cấu hình thống nhất trong ghi chú |
|---|---|
| Poll `station_status` | **60 giây** |
| Poll `station_information` | **24 giờ** |
| Spark micro-batch trigger | **5 phút** |
| `poll_id` | Hai feed dùng poll IDs độc lập |
| Source GBFS | GBFS v2.3 thông qua discovery URL trong Compose |

#### 2.3.4. Kafka offset, Spark checkpoint và idempotency

**Python Consumer → PostgreSQL**

- **Commit Kafka offset (hiện thực):** tắt auto-commit; chỉ commit offset của một message sau khi transaction PostgreSQL chứa message đó đã commit, và sau khi các message invalid đã được broker xác nhận.
- Dùng `UPSERT` thay vì `INSERT` để chịu được message replay (quy tắc ở mục 1.7).
- Nếu đã ghi PostgreSQL nhưng crash trước khi commit offset: sau restart có thể đọc lại; `UPSERT` phải idempotent.
- Mất kết nối PostgreSQL: consumer không tự retry mà dừng với lỗi; Docker restart với backoff (100 ms, nhân đôi, tối đa 1 phút) và **không commit offset** của record chưa ghi thành công.

**Spark Structured Streaming → Iceberg**

- Một ứng dụng Spark (service `spark-streaming`) chạy **4 query**, mỗi query một checkpoint riêng trên MinIO; job dừng trước khi start nếu thiếu checkpoint hoặc hai query dùng chung / lồng checkpoint:

| Query | Đọc | Ghi | Checkpoint |
|---|---|---|---|
| `gbfs_status_bronze` | Kafka `gbfs.station_status` | `bronze.gbfs_status` | `s3a://checkpoints/status-history/` (`STATUS_CHECKPOINT_LOCATION`) |
| `gbfs_information_bronze` | Kafka `gbfs.station_information` | `bronze.gbfs_station_information` | `s3a://checkpoints/information-history/` (`INFORMATION_CHECKPOINT_LOCATION`) |
| `gbfs_status_silver` | `bronze.gbfs_status` | `silver.gbfs_status` | `s3a://checkpoints/silver-status/` (`SILVER_STATUS_CHECKPOINT_LOCATION`) |
| `gbfs_information_silver` | `bronze.gbfs_station_information` | `silver.gbfs_station_information` | `s3a://checkpoints/silver-information/` (`SILVER_INFORMATION_CHECKPOINT_LOCATION`) |

- Hai query "history" trong ghi chú ban đầu là hai query Bronze; hai query Silver đọc tiếp từ Bronze.
- Offset Kafka nằm trong checkpoint; Spark không commit offset lên Kafka (group-id prefix `<KAFKA_GROUP_ID>-<feed>`). `startingOffsets=earliest` chỉ áp dụng cho checkpoint mới; `maxOffsetsPerTrigger=100000`; `failOnDataLoss=true`.
- Bronze ghi bằng `foreachBatch` và chỉ append message chưa có theo vị trí Kafka, nên batch chạy lại sau Iceberg commit không thêm dòng (kịch bản 2.4 đã kiểm thử trong `tests/integration/test_spark_recovery.py`; kết quả ở `streaming/README.md`).

#### 2.3.5. Raw GBFS trên MinIO

Poller lưu toàn bộ phản hồi của mỗi lần polling (STR-04), bật bằng `GBFS_ARCHIVE_RAW=true` (đã bật cho `gbfs-poller`):

```text
s3://raw/gbfs/<feed_name>/date=<YYYY-MM-DD>/<poll_id>/raw.json
```

| Trường trong `raw.json` | Ý nghĩa |
|---|---|
| `poll_id` | Mã lần polling (mục 1.4, 1.5) |
| `source_url` | URL feed đã gọi |
| `collected_at` | Thời điểm nhận phản hồi, UTC; `date=` lấy theo giá trị này |
| `payload` | JSON nguồn nguyên vẹn, gồm cả các field không có trong message Kafka |

Một lần polling chỉ bị xóa khỏi volume `gbfs-spool` khi cả Kafka và MinIO đã nhận; nếu không, thư mục được giữ để `--replay`.

### 2.4. Fault tolerance — Spark → Iceberg

| Kịch bản | Điều cần xác minh |
|---|---|
| Spark chạy bình thường với **100 messages** | Có ghi đầy đủ 100 observation logic vào Iceberg không? |
| Crash **trước khi ghi Iceberg** | Restart có đọc lại các message chưa xử lý không? |
| Crash **sau Iceberg commit nhưng trước checkpoint completion** | Replay có gây ghi trùng dữ liệu không? |
| Crash **sau khi checkpoint hoàn tất** | Spark có bỏ qua micro-batch đã hoàn tất không? |
| **MinIO tạm thời không truy cập được** | Spark báo lỗi, retry/dừng như thế nào, rồi phục hồi ra sao? |

**Các tiêu chí đánh giá:**

| Tiêu chí | Câu hỏi |
|---|---|
| Fault tolerance | Spark có phục hồi sau crash được không? |
| Offset recovery | Spark có đọc tiếp đúng vị trí trong Kafka không? |
| Replay behavior | Micro-batch chưa hoàn thành có được xử lý lại đúng không? |
| Idempotency | Iceberg sink có tránh ghi trùng khi retry không? |
| Data integrity | Sau restart có bị mất hoặc nhân đôi observation logic không? |

**Thực hiện sau:** Rebuild/reconciliation PostgreSQL latest state được lên kế hoạch **sau khi hai sink Streaming đã hoạt động ổn định**.

---

## 3. CI, kiểm thử và tiêu chí tích hợp

### 3.1. Phạm vi CI giai đoạn đầu

CI ban đầu gồm đúng **ba nhóm kiểm tra**:

1. **Lint:** Kiểm tra quy tắc mã nguồn.
2. **Unit tests:** Kiểm tra logic ở cấp hàm/module.
3. **Contract tests:** Kiểm tra schema, field, quy tắc grain và giao tiếp giữa các module.

**Integration tests:** Bổ sung sau khi có module đầu tiên chạy được; không yêu cầu khởi động toàn bộ Docker stack trong mọi PR ngay từ đầu.

### 3.2. Những kiểm tra tối thiểu cần có

| Khu vực | Test cần phát triển |
|---|---|
| Trip History | Dedup cùng `ride_id`; conflict audit; giữ `NULL station_id` |
| Timezone | DST, ambiguous timestamp, invalid local time |
| GBFS status | Validate schema; dedup `(station_id, poll_id)`; xử lý dữ liệu thiếu |
| Kafka consumer | Replay sau crash và PostgreSQL UPSERT idempotent |
| Spark streaming | Checkpoint recovery, replay và Iceberg dedup/idempotency |
| Batch integration | Downloader → MinIO → Spark → Iceberg → Trino |

> Bảng này tổng hợp các bài test được ngụ ý hoặc nêu trực tiếp trong ghi chú; **không phải báo cáo các test đã chạy thành công**.

---

## 4. Phân công và phụ thuộc giữa các thành viên

| Người phụ trách | Công việc bắt đầu | Contract cần phối hợp |
|---|---|---|
| **Khang — Batch** | Downloader + manifest trước; không phụ thuộc Streaming | Với **Kien**: `silver.trips`, timezone, dedup và quality rules. Với **Nguyen**: entry points, raw storage và runtime. |
| **Quan — Streaming** | JSON Schema + Poller prototype, sau đó Kafka Producer | Với **Kien**: GBFS message schema, Iceberg observation grain và PostgreSQL latest-state semantics. Với **Nguyen**: Kafka/Spark runtime, checkpoint, recovery. |
| **Kien — Data Modeling** | Hoàn thiện schema contracts cùng fixtures; mở issue riêng nghiên cứu station mapping | Review schemas và data-quality rules với Khang/Quan. |
| **Nguyen — Infrastructure** | MinIO, Kafka, Spark, Catalog smoke tests; CI cơ bản | Review runtime contracts, kết nối hệ thống và Airflow job-launch. |

**Nguyên tắc tích hợp:** Batch và Streaming có thể phát triển song song, nhưng giao diện dữ liệu ở ranh giới producer/consumer phải được thống nhất trước khi tích hợp.

---

## 5. Danh sách quyết định còn mở

| Mục | Trạng thái | Người phối hợp |
|---|---|---|
| Tập field trong message Kafka `station_status` (hiện thực: 12 field ở mục 1.4, field nguồn khác chỉ trong raw JSON) | **Đã hiện thực; chờ Kien + Quan xác nhận** | Quan + Kien |
| Vị trí feed metadata (`last_updated`) | **Đã hiện thực:** lấy từ cấp feed (mục 1.4) | Quan + Kien |
| Chính sách `num_bikes_available = NULL` | **Đã hiện thực reject; chờ Kien xác nhận** | Quan + Kien |
| Grain/snapshot policy của `station_information` | **Đã hiện thực** `(station_id, poll_id)`, mỗi poll một snapshot, có `poll_id` trong message; **chờ Kien + Quan xác nhận** | Quan + Kien + Nguyen |
| Điều kiện chính xác của `UNAVAILABLE`; `opened_at` theo `collected_at` hay `now()` (mục 1.7) | **Pending** (#39) | Quan + Kien |
| Công thức và fixture đối chiếu Gold metrics | **Pending** | Kien + Khang/Quan |
| Chính sách xử lý `ended_at < started_at` | **Đề xuất; cần chốt** | Khang + Kien |
| Nơi lưu/định dạng audit file và xử lý `ride_id` conflict vào Silver | **Pending** | Khang + Kien |
| Mapping giữa Trip History station ID và GBFS station ID | **Issue nghiên cứu** | Kien + Khang/Quan |
| Số Kafka partitions | **Đã hiện thực 3; chờ benchmark** | Nguyen + Quan |
| Quy tắc Kafka offset commit và Spark checkpoint/replay khi crash | **Đã hiện thực + test** (mục 2.3.4; `tests/integration/test_latest_state_recovery.py`, `tests/integration/test_spark_recovery.py`) | Quan + Nguyen |
| Airflow job-launch mechanism (Docker CLI mới là đề xuất) | **Pending** | Nguyen |
| Mức HA thực tế: local simulation so với triển khai nhiều host | **Định hướng; chưa nghiệm thu** | Nguyen + cả nhóm |
| Rebuild PostgreSQL latest state | **Giai đoạn sau** | Quan + Nguyen |

---

**Tài liệu tham chiếu cho nhóm:** Đây là bản **chuẩn hóa trình bày** của ghi chú contract, không phải bằng chứng Docker services hoặc các data pipelines đã chạy thành công. Khi một mục Pending được chốt qua PR, cập nhật tài liệu này đồng thời với schema/code/test liên quan.
