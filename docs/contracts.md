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

**Ví dụ message chuẩn hóa trên topic `gbfs.station_status`** (tập trường hiện tại; giữ thêm trường nguồn là quyết định còn mở, xem mục 5):

```json
{
  "schema_version": 1,
  "poll_id": "station_status-3f2b8c1e-5d7a-4e0b-9c61-2a4f0e8d9b17",
  "station_id": "abc",
  "num_bikes_available": 5,
  "num_docks_available": 12,
  "is_installed": true,
  "is_renting": true,
  "is_returning": true,
  "last_reported": 1794796740,
  "last_updated": 1794796800,
  "collected_at": "2026-11-16T02:40:00Z"
}
```

**Schema hiện thực:** Pydantic `ingestion/gbfs/schemas.py`, xuất ra `ingestion/gbfs/json_schemas/station_status.schema.json` (có trên `main` sau khi PR #1 merge). Khi schema đổi, sửa mục này trong cùng PR.

- `poll_id`: `station_status-<uuid4>`, mỗi lần polling một giá trị, không dùng chung với `station_information`.
- `last_updated`: metadata cấp feed (epoch giây); `last_reported`: của từng trạm (epoch giây); `collected_at`: thời điểm poller nhận phản hồi, UTC có timezone.
- Cờ `is_*` nhận 0/1 hoặc boolean từ nguồn, xuất ra boolean.
- `vehicle_types_available`: array hoặc `null`.

**Iceberg table:** `silver.gbfs_status` (scope Slide 10).

**Grain / khóa logic:** `(station_id, poll_id)`.

**Quy tắc:**

- Lưu observation cho từng lần polling khác nhau, **kể cả khi trạng thái không đổi**.
- Nếu retry phát lại **cùng `(station_id, poll_id)`**, dedup để chỉ còn một observation logic.
- Message Kafka hiện chỉ chứa tập trường đã chuẩn hóa ở trên; các trường nguồn khác (ví dụ `num_bikes_disabled`, `num_docks_disabled`, `num_ebikes_available`) chỉ có trong raw JSON (mục 2.3.5). Giữ thêm trường nào trong message là **Pending** (mục 5), phải chốt trước STR-07.
- Spark jobs phải xử lý dedup và có Data Quality tests kiểm chứng.
- `num_bikes_available = NULL`: hiện thực đang **reject** (ghi vào `gbfs.invalid`); chờ Kien xác nhận (mục 5).

### 1.5. GBFS — `station_information`

**Mục tiêu:** Lưu dữ liệu thông tin trạm để liên kết với status, hiển thị dashboard và hỗ trợ mapping.

| Trường | Mục đích | Yêu cầu |
|---|---|---|
| `station_id` | Join với station status | Bắt buộc |
| `name` | Tên trạm trên dashboard | Bắt buộc |
| `lat`, `lon` | Vị trí trạm trên bản đồ | Bắt buộc |
| `short_name` | Mapping với Trip History station ID | Nullable |
| `capacity` | Sức chứa trạm | Nullable |
| `is_virtual_station` | Phân biệt trạm ảo/vật lý | Nullable |

Message trên topic `gbfs.station_information` gồm các trường trên cùng `last_updated` và `collected_at` (schema: `ingestion/gbfs/json_schemas/station_information.schema.json`, có trên `main` sau khi PR #1 merge). Các trường nguồn khác (ví dụ `region_id`, `rental_uris`, `is_charging`) chỉ có trong raw JSON.

**Iceberg table:** `silver.gbfs_station_information` (scope Slide 10).

**Polling:** Mỗi **24 giờ**; `poll_id` của `station_information` (`station_information-<uuid4>`) **không dùng chung** với `station_status`. Hiện `poll_id` này chỉ dùng cho đường dẫn raw trên MinIO, **chưa có trong message**; một snapshot được nhận diện bằng `collected_at`.

**Pending (mục 5):** Danh sách field giữ trong message (STR-10 yêu cầu giữ đầy đủ), có xuất `poll_id` vào message hay không, chính sách snapshot/grain của bảng thông tin trạm, và lịch refresh nếu metadata thay đổi giữa hai lần poll.

### 1.6. Gold metrics và station mapping

- **Gold:** Chốt **grain của từng metric** trước khi viết SQL; công thức và số liệu phải đối chiếu với **fixture mẫu**.
- **Station mapping:** Kien mở issue nghiên cứu riêng; chưa tự coi các quy tắc mapping là đã hoàn tất.

### 1.7. PostgreSQL serving — `station_status_latest` và `active_alerts`

**Phụ trách:** Quan (ghi dữ liệu: STR-05 #7, STR-05b #39); Kien (đọc trên Grafana: VIS-01 #31). **Schema:** `infra/postgres/init.sql` (Nguyen).

PostgreSQL được cập nhật trực tiếp bởi Latest State Consumer đọc `gbfs.station_status`, không dùng CDC (scope Slide 7, 10).

#### `station_status_latest`

**Grain:** `1 dòng = trạng thái mới nhất của 1 station_id` (khóa chính `station_id`).

| Cột | Kiểu | Nguồn |
|---|---|---|
| `station_id` | `TEXT` PK | message |
| `poll_id` | `TEXT NOT NULL` | message |
| `num_bikes_available` | `INTEGER NOT NULL`, ≥ 0 | message |
| `num_docks_available` | `INTEGER`, ≥ 0 | message |
| `is_installed`, `is_renting`, `is_returning` | `BOOLEAN NOT NULL` | message |
| `last_updated`, `last_reported` | `TIMESTAMPTZ` | epoch giây trong message, đổi bằng `to_timestamp` |
| `collected_at` | `TIMESTAMPTZ NOT NULL` | message |

**Quy tắc:** UPSERT chỉ ghi đè khi `collected_at` của message **mới hơn** dòng hiện có; message cũ hoặc replay cùng `collected_at` không thay đổi dòng. Thứ tự commit và offset xem mục 2.3.4.

#### `active_alerts`

**Grain:** `1 dòng = 1 cảnh báo đang mở của 1 trạm` (khóa chính `(station_id, alert_type)`); trạm không có dòng nào được coi là `NORMAL`.

| Cột | Kiểu |
|---|---|
| `station_id` | `TEXT NOT NULL`, tham chiếu `station_status_latest` |
| `alert_type` | `LOW_BIKES` \| `EMPTY` \| `UNAVAILABLE` \| `STALE` |
| `opened_at` | `TIMESTAMPTZ NOT NULL`, giữ nguyên khi cảnh báo còn mở |
| `updated_at` | `TIMESTAMPTZ NOT NULL` |

| Cảnh báo | Điều kiện (scope Slide 6) | Cập nhật khi |
|---|---|---|
| `LOW_BIKES` | `0 < num_bikes_available <= LOW_BIKES_THRESHOLD` | Consumer ghi một observation mới hơn |
| `EMPTY` | `num_bikes_available = 0` và `is_renting = true` | Consumer ghi một observation mới hơn |
| `UNAVAILABLE` | Trạm không hoạt động hoặc không cho thuê/trả theo cờ `is_*` (điều kiện chính xác: **Pending**, mục 5) | Consumer ghi một observation mới hơn |
| `STALE` | `now() - last_reported > STALE_AFTER_SECONDS` | Job định kỳ, chạy cả khi không có message mới |

- **Ngưỡng:** `LOW_BIKES_THRESHOLD` (mặc định 2) và `STALE_AFTER_SECONDS` (mặc định 180), đặt trong `.env.example` / `docker-compose.yml`.
- **Staleness** luôn tính từ thời gian hiện tại và `last_reported`, không dùng `collected_at`.
- Cảnh báo theo message được mở/đóng trong cùng transaction với UPSERT, trước khi commit Kafka offset; replay không được đổi `opened_at` hay tạo dòng trùng.
- Chi tiết hiện thực và tiêu chí nghiệm thu: issue #39.

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
- **Partitions:** Đã hiện thực **3 partitions/topic** trong `infra/kafka/init_topics.sh`; con số cuối cùng vẫn chờ benchmark (mục 5).
- **Retention:** 7 ngày (`retention.ms=604800000`), `cleanup.policy=delete`.
- **Message key:** `station_id` cho cả `gbfs.station_status` và `gbfs.station_information`; message trên `gbfs.invalid` dùng `station_id` nếu payload có, ngược lại để key rỗng.

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

- **Commit Kafka offset** (đã chốt trong STR-05): tắt auto-commit; chỉ commit offset của message sau khi transaction PostgreSQL của message đó đã commit.
- Dùng `UPSERT` thay vì `INSERT` để chịu được message replay (quy tắc ở mục 1.7).
- Nếu đã ghi PostgreSQL nhưng crash trước khi commit offset: sau restart có thể đọc lại; `UPSERT` phải idempotent.
- Nếu PostgreSQL mất kết nối từ 2 phút trở lên: **đề xuất** exponential backoff, delay tối đa **45 giây**, có jitter; **không commit offset** của record chưa ghi thành công.

**Spark Structured Streaming → Iceberg**

- Spark sử dụng checkpoint để khôi phục tiến độ đọc Kafka.
- Mỗi query có checkpoint riêng:
  - History của `station_status`.
  - History của `station_information`.
- Spark có consumer group phục vụ query; cần chốt/cấu hình chính xác trong implementation, không dùng checkpoint chung cho hai queries.
- Việc commit offset/checkpoint và ghi Iceberg phải được kiểm thử trong tình huống crash/replay.

#### 2.3.5. Raw GBFS trên MinIO

Poller lưu toàn bộ phản hồi của mỗi lần polling (scope Slide 7, 9 — STR-04):

```text
s3://raw/gbfs/<feed_name>/date=<YYYY-MM-DD>/<poll_id>/raw.json
```

| Trường trong `raw.json` | Ý nghĩa |
|---|---|
| `poll_id` | Mã lần polling (mục 1.4, 1.5) |
| `source_url` | URL feed đã gọi |
| `collected_at` | Thời điểm nhận phản hồi, UTC; `date=` lấy theo giá trị này |
| `payload` | JSON nguồn nguyên vẹn, gồm cả các trường không có trong message Kafka |

- Bật bằng `GBFS_ARCHIVE_RAW=true` (đã bật cho service `gbfs-poller` trong Compose); bucket lấy từ `RAW_BUCKET`.
- Đây là nguồn để kiểm tra và xử lý lại khi cần các trường nguồn không có trong message Kafka.

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

Bảng này là nguồn chuẩn về người phụ trách; `docs/scope.md` Slide 12 và `infra/README.md` mục 3 phải khớp với bảng này.

| Người phụ trách | Phạm vi | Issue | Contract cần phối hợp |
|---|---|---|---|
| **Khang — Batch** | Downloader, manifest, Spark Bronze/Silver cho Trip History | BAT-01..BAT-12 (#15–#26, issue cha #28) | Với **Kien**: `silver.trips`, timezone, dedup và quality rules. Với **Nguyen**: entry points, raw storage và runtime. |
| **Quan — GBFS Ingestion & PostgreSQL serving** | Poller, Kafka Producer, raw GBFS trên MinIO, Latest State Consumer, `station_status_latest`, `active_alerts` | STR-01..STR-06 (#2, #4–#8), STR-05b (#39) | Với **Kien**: GBFS message schema, Iceberg observation grain, PostgreSQL latest-state và quy tắc cảnh báo. Với **Nguyen**: Kafka runtime, schema message cho Spark, recovery. |
| **Kien — Data Modeling & Dashboard** | dbt Gold, station mapping, Grafana dashboard | MOD-01 (#29), MOD-02 (#30), VIS-01 (#31) | Review schemas và data-quality rules với Khang/Quan. |
| **Nguyen — Infrastructure & Spark Streaming** | Docker Compose, MinIO, Kafka, Spark, Catalog, Trino, CI, Airflow, HA; Spark Streaming → Iceberg | INF-01..INF-04 (#32–#35), STR-07..STR-12 (#9–#14) | Review runtime contracts, kết nối hệ thống và Airflow job-launch. Với **Quan**: schema message Kafka (mục 1.4, 1.5). |
| **Cả nhóm** | Tích hợp end-to-end và demo | INT-01 (#36) | Mục 3. |

**Nguyên tắc tích hợp:** Batch và Streaming có thể phát triển song song, nhưng giao diện dữ liệu ở ranh giới producer/consumer phải được thống nhất trước khi tích hợp.

### 4.1. Truy vết scope → contract → issue

Mỗi sản phẩm bàn giao trong `docs/scope.md` Slide 13 phải có ít nhất một issue. Dòng **Chưa có issue** là việc còn thiếu trong kế hoạch.

| Sản phẩm bàn giao / yêu cầu trong scope | Mục contract | Issue |
|---|---|---|
| Repository có cấu trúc rõ ràng và hướng dẫn triển khai | — | INF-01 (#32), INT-01 (#36) |
| Batch pipeline Trip History NYC 01–10/2026 | 1.1, 1.2, 1.3, 2.2 | BAT-01..BAT-12 |
| Streaming pipeline GBFS 2.3 → Kafka | 1.4, 1.5, 2.3 | STR-01..STR-04 |
| Lakehouse Bronze/Silver/Gold trên Iceberg | 1.3, 1.4, 1.5, 1.6, 2.4 | BAT-04, BAT-07, STR-07..STR-12, MOD-01 |
| Data models và data quality tests | 1.6, 3.2 | MOD-01, MOD-02, BAT-12, STR-09 |
| Grafana dashboard nhu cầu và trạng thái trạm | 1.6, 1.7 | VIS-01 (#31) |
| PostgreSQL latest state | 1.7, 2.3.4 | STR-05, STR-06 |
| Cảnh báo thiếu xe, hết xe, ngừng hoạt động, stale | 1.7 | STR-05b (#39), VIS-01 |
| Airflow DAGs | 2.2.3 | INF-03 (#34) |
| CI: lint, unit, contract tests | 3.1 | INF-02 (#33) |
| HA và fault tolerance | 2.1, 2.4 | INF-04 (#35), STR-12 |
| Iceberg maintenance / compaction (scope Slide 7, 9) | — | **Chưa có issue** |
| Khôi phục / đối soát PostgreSQL từ Iceberg (scope Slide 7, 9, 10) | 2.4 (thực hiện sau) | **Chưa có issue** (giai đoạn sau, mục 5) |
| Tài liệu vận hành | — | `docs/runbook.md`; **chưa có issue riêng** |

---

## 5. Danh sách quyết định còn mở

| Mục | Trạng thái | Người phối hợp |
|---|---|---|
| Tập trường giữ trong message Kafka `station_status` / `station_information` (STR-07, STR-10 phụ thuộc; mục 1.4, 1.5) | **Pending — chốt trước STR-07** | Quan + Kien + Nguyen |
| Vị trí metadata cấp feed (`last_updated`) | **Đã chốt:** lấy từ cấp feed (mục 1.4) | Quan + Kien |
| Chính sách `num_bikes_available = NULL` | **Đã hiện thực reject; chờ Kien xác nhận** | Quan + Kien |
| Grain/snapshot policy của `station_information`; có xuất `poll_id` vào message không | **Pending** | Quan + Kien + Nguyen |
| Công thức STALE | **Đã chốt:** `now() - last_reported` (mục 1.7) | Quan + Kien |
| Điều kiện chính xác của `UNAVAILABLE` | **Pending** (#39) | Quan + Kien |
| `opened_at` / `updated_at` của cảnh báo theo `collected_at` hay `now()` | **Pending** (#39) | Quan + Kien |
| Công thức và fixture đối chiếu Gold metrics | **Pending** | Kien + Khang/Quan |
| Chính sách xử lý `ended_at < started_at` | **Đề xuất; cần chốt** | Khang + Kien |
| Nơi lưu/định dạng audit file và xử lý `ride_id` conflict vào Silver | **Pending** | Khang + Kien |
| Mapping giữa Trip History station ID và GBFS station ID | **Issue nghiên cứu** | Kien + Khang/Quan |
| Số Kafka partitions | **Đã hiện thực 3; chờ benchmark** | Nguyen + Quan |
| Commit Kafka offset của Python consumer | **Đã chốt:** sau khi PostgreSQL commit (mục 2.3.4) | Quan |
| Spark checkpoint/replay khi crash | **Cần hiện thực + test** (STR-11, STR-12) | Nguyen |
| Airflow job-launch mechanism (Docker CLI mới là đề xuất) | **Pending** | Nguyen |
| Mức HA thực tế: local simulation so với triển khai nhiều host | **Định hướng; chưa nghiệm thu** | Nguyen + cả nhóm |
| Rebuild PostgreSQL latest state | **Giai đoạn sau; chưa có issue** | Quan + Nguyen |
| Iceberg maintenance / compaction | **Chưa có issue** | Nguyen + Kien |

---

**Tài liệu tham chiếu cho nhóm:** Đây là bản **chuẩn hóa trình bày** của ghi chú contract, không phải bằng chứng Docker services hoặc các data pipelines đã chạy thành công. Khi một mục Pending được chốt qua PR, cập nhật tài liệu này đồng thời với schema/code/test liên quan.
