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

**Phối hợp:** Max (Batch) và John (Data Modeling / Data Quality).

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

**Phối hợp:** Tom (Streaming) và John (Schema / Data Quality).

**Ví dụ message chuẩn hóa được nhóm đề xuất** (chưa xác nhận toàn bộ feed fields chính thức):

```json
{
  "schema_version": 1,
  "poll_id": "poll-20261116T024000Z",
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

**Iceberg table:** `gbfs_status` (tên namespace cụ thể cần xác nhận khi code).

**Grain / khóa logic:** `(station_id, poll_id)`.

**Quy tắc:**

- Lưu observation cho từng lần polling khác nhau, **kể cả khi trạng thái không đổi**.
- Nếu retry phát lại **cùng `(station_id, poll_id)`**, dedup để chỉ còn một observation logic.
- Dự kiến giữ **toàn bộ trường của `station_status`** trong schema đã xác minh; danh sách đầy đủ các feed fields vẫn **Pending**.
- Spark jobs phải xử lý dedup và có Data Quality tests kiểm chứng.
- **Đề xuất:** Nếu `num_bikes_available = NULL`, reject message hoặc đưa vào luồng invalid; chính sách cuối cùng chưa chốt.

> `last_updated` trong ví dụ hiện được giữ đúng tên từ ghi chú. Cần xác minh vị trí metadata cấp feed và quy tắc chuẩn hóa trước khi khóa JSON Schema chính thức.

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

**Polling:** Mỗi **24 giờ**; `poll_id` của `station_information` **không dùng chung** với `station_status`.

**Pending:** Danh sách đầy đủ field từ GBFS v2.3, chính sách snapshot/grain của bảng thông tin trạm, và lịch refresh nếu metadata thay đổi giữa hai lần poll.

### 1.6. Gold metrics và station mapping

- **Gold:** Chốt **grain của từng metric** trước khi viết SQL; công thức và số liệu phải đối chiếu với **fixture mẫu**.
- **Station mapping:** John mở issue nghiên cứu riêng; chưa tự coi các quy tắc mapping là đã hoàn tất.

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

### 2.2. Alex ↔ Max — Batch Runtime & Storage

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

### 2.3. Alex ↔ Tom — Kafka Runtime & Ingestion

#### 2.3.1. Kafka local

- **Phiên bản:** Kafka `3.9.1`.
- **Số broker local trong ghi chú:** `1`.
- **Replication factor local:** `1`.
- **Trade-off:** Tiết kiệm tài nguyên nhưng khả năng chịu lỗi kém hơn HA cluster.
- **Partitions:** **Đề xuất 3 partitions/topic**, chưa chốt; phải thử nghiệm để chọn cấu hình phù hợp.

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

- Cần chốt rõ thời điểm **commit Kafka offset**.
- Dùng `UPSERT` thay vì `INSERT` để chịu được message replay.
- Nếu đã ghi PostgreSQL nhưng crash trước khi commit offset: sau restart có thể đọc lại; `UPSERT` phải idempotent.
- Nếu PostgreSQL mất kết nối từ 2 phút trở lên: **đề xuất** exponential backoff, delay tối đa **45 giây**, có jitter; **không commit offset** của record chưa ghi thành công.

**Spark Structured Streaming → Iceberg**

- Spark sử dụng checkpoint để khôi phục tiến độ đọc Kafka.
- Mỗi query có checkpoint riêng:
  - History của `station_status`.
  - History của `station_information`.
- Spark có consumer group phục vụ query; cần chốt/cấu hình chính xác trong implementation, không dùng checkpoint chung cho hai queries.
- Việc commit offset/checkpoint và ghi Iceberg phải được kiểm thử trong tình huống crash/replay.

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
| **Max — Batch** | Downloader + manifest trước; không phụ thuộc Streaming | Với **John**: `silver.trips`, timezone, dedup và quality rules. Với **Alex**: entry points, raw storage và runtime. |
| **Tom — Streaming** | JSON Schema + Poller prototype, sau đó Kafka Producer | Với **John**: GBFS message schema, Iceberg observation grain và PostgreSQL latest-state semantics. Với **Alex**: Kafka/Spark runtime, checkpoint, recovery. |
| **John — Data Modeling** | Hoàn thiện schema contracts cùng fixtures; mở issue riêng nghiên cứu station mapping | Review schemas và data-quality rules với Max/Tom. |
| **Alex — Infrastructure** | MinIO, Kafka, Spark, Catalog smoke tests; CI cơ bản | Review runtime contracts, kết nối hệ thống và Airflow job-launch. |

**Nguyên tắc tích hợp:** Batch và Streaming có thể phát triển song song, nhưng giao diện dữ liệu ở ranh giới producer/consumer phải được thống nhất trước khi tích hợp.

---

## 5. Danh sách quyết định còn mở

| Mục | Trạng thái | Người phối hợp |
|---|---|---|
| Xác nhận toàn bộ field của GBFS v2.3 và vị trí feed metadata | **Pending** | Tom + John |
| Chính sách `num_bikes_available = NULL` | **Đề xuất reject; cần chốt** | Tom + John |
| Grain/snapshot policy của `station_information` | **Pending** | Tom + John |
| Công thức và fixture đối chiếu Gold metrics | **Pending** | John + Max/Tom |
| Chính sách xử lý `ended_at < started_at` | **Đề xuất; cần chốt** | Max + John |
| Nơi lưu/định dạng audit file và xử lý `ride_id` conflict vào Silver | **Pending** | Max + John |
| Mapping giữa Trip History station ID và GBFS station ID | **Issue nghiên cứu** | John + Max/Tom |
| Số Kafka partitions (đang đề xuất 3) | **Pending benchmark** | Alex + Tom |
| Quy tắc Kafka offset commit và Spark checkpoint/replay khi crash | **Cần hiện thực + test** | Tom + Alex |
| Airflow job-launch mechanism (Docker CLI mới là đề xuất) | **Pending** | Alex |
| Mức HA thực tế: local simulation so với triển khai nhiều host | **Định hướng; chưa nghiệm thu** | Alex + cả nhóm |
| Rebuild PostgreSQL latest state | **Giai đoạn sau** | Tom + Alex |

---

**Tài liệu tham chiếu cho nhóm:** Đây là bản **chuẩn hóa trình bày** của ghi chú contract, không phải bằng chứng Docker services hoặc các data pipelines đã chạy thành công. Khi một mục Pending được chốt qua PR, cập nhật tài liệu này đồng thời với schema/code/test liên quan.
