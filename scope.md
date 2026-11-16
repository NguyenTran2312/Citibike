# ĐỀ TÀI: CITI BIKE BIG DATA PIPELINE
**Xây dựng hệ thống xử lý dữ liệu Batch và Streaming để phân tích nhu cầu sử dụng và giám sát trạng thái trạm xe đạp Citi Bike**

## Slide 1 — Giới thiệu đề tài

**Mục tiêu:** Xây dựng hệ thống Big Data tích hợp hai luồng xử lý:

- **Batch Processing:** Phân tích lịch sử các chuyến đi Citi Bike.
- **Near-real-time Streaming:** Theo dõi tình trạng xe và dock tại các trạm.
- **Lakehouse:** Lưu trữ và chuẩn hóa dữ liệu bằng Apache Iceberg.
- **Grafana Dashboard:** Trực quan hóa nhu cầu sử dụng và cảnh báo tình trạng trạm.

**Kết quả mong đợi:** Một dashboard tích hợp, hỗ trợ phân tích nhu cầu lịch sử và giám sát trạng thái trạm gần thời gian thực.

---

## Slide 2 — Bài toán và mục tiêu phân tích

Hệ thống cần trả lời hai nhóm câu hỏi chính.

**1. Phân tích nhu cầu sử dụng**

- Số chuyến đi thay đổi như thế nào theo giờ?
- Ngày nào trong tuần có nhu cầu sử dụng cao?
- Top 10 trạm có nhiều lượt xuất phát nhất?
- Top 10 trạm có nhiều lượt kết thúc nhất?
- Khung giờ cao điểm tại các trạm là khi nào?

**2. Giám sát trạng thái trạm**

- Hiện tại mỗi trạm còn bao nhiêu xe?
- Có bao nhiêu dock còn trống?
- Trạm nào đang thiếu xe hoặc hết xe?
- Trạm nào ngừng cho thuê/trả xe?
- Dữ liệu trạm nào đã quá cũ và cần cảnh báo?

---

## Slide 3 — Phạm vi dự án (Project Scope)

### In Scope — Thực hiện

**Trip History**
- Khu vực: New York City (NYC).
- Thời gian: 01/2026 đến 10/2026.
- Nguồn: Citi Bike Trip History ZIP/CSV.
- Phân tích các chuyến đi công khai trong dữ liệu nguồn.

**GBFS**
- Phiên bản Discovery: GBFS 2.3.
- Sử dụng `station_information` và `station_status`.
- Polling theo chu kỳ mục tiêu 60 giây, phù hợp với TTL của feed.
- Theo dõi trạng thái trạm và độ mới của dữ liệu.

**Công nghệ**
- Python, PySpark, Kafka, Spark Structured Streaming.
- S3-compatible Object Storage, Apache Iceberg, REST Catalog.
- Trino, dbt, Grafana, Airflow, Docker Compose.

### Out of Scope — Chưa thực hiện

- Không phân tích Jersey City (JC) trong phiên bản đầu.
- Không dự báo nhu cầu bằng Machine Learning.
- Không tối ưu điều phối hoặc vận chuyển xe giữa các trạm.
- Không xây dựng ứng dụng đặt xe hoặc điều khiển trạm.
- Không coi Trip History là nguồn chuyến đi real-time.

---

## Slide 4 — Nguồn dữ liệu đầu vào

### Dataset 1: Trip History — Batch

Mỗi chuyến đi có 13 trường:

1. `ride_id`
2. `rideable_type`
3. `started_at`
4. `ended_at`
5. `start_station_name`
6. `start_station_id`
7. `end_station_name`
8. `end_station_id`
9. `start_lat`
10. `start_lng`
11. `end_lat`
12. `end_lng`
13. `member_casual`

**Yêu cầu thu thập:**
- Kiểm tra đầy đủ ZIP của từng tháng.
- Liệt kê tất cả CSV bên trong từng ZIP.
- Ghi nhận tên file, kích thước, checksum và số dòng.
- Đối chiếu header/schema của tất cả CSV.
- Không bỏ sót file khi một ZIP chứa nhiều CSV.
- Lưu manifest để kiểm soát file đã tải và xử lý.

### Dataset 2: GBFS — Streaming

- `station_information`: Thông tin và định danh trạm.
- `station_status`: Số xe, số dock và trạng thái hoạt động.
- `last_updated`: Thời điểm cập nhật của feed.
- `last_reported`: Thời điểm cập nhật của từng trạm.
- `collected_at`: Thời điểm hệ thống thu thập dữ liệu.

---

## Slide 5 — Đầu ra 1: Dashboard phân tích nhu cầu

**Nguồn:** Trip History Batch.

**Các thành phần dashboard:**

| Biểu đồ | Nội dung |
|---|---|
| Trips by Hour | Số chuyến theo giờ trong ngày |
| Trips by Weekday | Số chuyến theo thứ trong tuần |
| Top 10 Start Stations | 10 trạm có nhiều lượt xuất phát nhất |
| Top 10 End Stations | 10 trạm có nhiều lượt kết thúc nhất |
| Station Demand | Số lượt START và END tại từng trạm |

### Quy ước KPI

- **Số chuyến:** Đếm chuyến đi, không cộng gộp cả START và END.
- **Giờ cao điểm nhu cầu thuê xe:** Xác định theo số lượt `START`.
- **Giờ cao điểm trả xe:** Xác định riêng theo số lượt `END`.
- **Thời gian phân tích:** Giờ địa phương `America/New_York`.
- **Bộ lọc:** Khoảng ngày, trạm, hướng START/END và loại người dùng khi cần.

**Lưu ý:** Dashboard phải ghi rõ đây là số chuyến công khai trong Trip History, không khẳng định bao gồm mọi chuyến đi thực tế.

---

## Slide 6 — Đầu ra 2: Dashboard giám sát trạm và cảnh báo

**Nguồn:** GBFS Station Status.

### Bảng trạng thái hiện tại

| Trường | Ý nghĩa |
|---|---|
| `station_id` | Định danh trạm GBFS |
| `num_bikes_available` | Số xe hiện có |
| `num_docks_available` | Số dock còn trống |
| `last_reported` | Lần cập nhật cuối của trạm |
| `collected_at` | Lần hệ thống thu thập |
| `is_renting` | Có cho thuê xe không |
| `is_returning` | Có nhận trả xe không |
| `is_stale` | Dữ liệu đã quá cũ chưa |

### Quy tắc cảnh báo đề xuất

- **LOW_BIKES:** Số xe lớn hơn 0 nhưng nhỏ hơn hoặc bằng threshold.
- **EMPTY:** Số xe bằng 0 và trạm vẫn cho thuê.
- **UNAVAILABLE:** Trạm không hoạt động hoặc không cho thuê/trả xe theo các cờ trạng thái.
- **STALE:** `collected_at - last_reported` vượt ngưỡng quy định.
- **NORMAL:** Trạm hoạt động, dữ liệu đủ mới và số xe vượt threshold.

**Threshold mặc định đề xuất:** 2 xe, có thể cấu hình.

**Nguyên tắc:** Trạm có 0 xe không đồng nghĩa trạm đã ngừng hoạt động. Cần kiểm tra thêm `is_installed`, `is_renting`, `is_returning` và độ mới dữ liệu.

---

## Slide 7 — Kiến trúc tổng thể

### Pipeline Batch

**Citi Bike Trip History S3 → Python Downloader + Manifest → Raw Storage (MinIO) → PySpark Batch → Iceberg Bronze/Silver**

### Pipeline Streaming

**GBFS API → Python Poller → Pydantic Validation → Kafka**

Dữ liệu từ Kafka được xử lý theo hai nhánh:

- **Nhánh 1 — Historical Storage:** Kafka → Spark Structured Streaming → Iceberg Bronze/Silver trên MinIO.
- **Nhánh 2 — Latest State Serving:** Kafka → Latest State Consumer → PostgreSQL (`station_status_latest`, `active_alerts`).

Raw JSON từ GBFS được lưu riêng trên MinIO để kiểm tra và xử lý lại.

### Analytics & Serving Layer

- **Historical Analytics:** Iceberg Silver → dbt Gold Models → Trino → Grafana.
- **Current Station Monitoring:** PostgreSQL → Grafana.
- **Iceberg** lưu lịch sử các lần polling và dữ liệu phân tích.
- **PostgreSQL** lưu trạng thái mới nhất của từng trạm và cảnh báo đang hoạt động.

### Orchestration

**Airflow** điều phối:
- Batch ingestion và processing.
- dbt transformations và data quality tests.
- Iceberg maintenance và các tác vụ định kỳ.
- Tác vụ đối soát hoặc khôi phục PostgreSQL từ Iceberg khi cần.

Airflow không điều phối trực tiếp luồng streaming liên tục.

**Hạ tầng dùng chung:** Docker Compose, MinIO, Kafka, Spark, Apache Iceberg, Iceberg REST Catalog, PostgreSQL, Trino, dbt, Grafana và Airflow.

**Kiến trúc không sử dụng CDC/Debezium.**

---

## Slide 8 — Thiết kế Batch Pipeline

**Mục tiêu:** Xử lý toàn bộ Trip History NYC từ 01/2026 đến 10/2026.

### Các bước

1. Liệt kê và tải các ZIP từ nguồn Trip History.
2. Kiểm tra tính đầy đủ của từng archive.
3. Ghi manifest và lưu raw ZIP/CSV lên MinIO.
4. PySpark đọc dữ liệu và ghi Bronze.
5. Chuẩn hóa schema, timestamp, station ID và kiểm tra chất lượng dữ liệu.
6. Ghi bảng `silver.trips`.
7. Tạo Gold aggregates phục vụ dashboard.

### Yêu cầu kỹ thuật

- Không bỏ sót CSV trong ZIP.
- Không phụ thuộc cứng vào cách đặt tên file.
- Một `ride_id` hợp lệ tương ứng một chuyến đi trong Silver.
- Retry và backfill không được làm tăng số chuyến do xử lý trùng.
- Giữ raw data để có thể xử lý lại khi cần.

---

## Slide 9 — Thiết kế Streaming Pipeline

**Mục tiêu:** Thu thập trạng thái trạm Citi Bike gần thời gian thực, lưu lịch sử quan sát và cập nhật trạng thái mới nhất phục vụ dashboard.

### Các bước

1. **Polling:** Python Poller gọi GBFS API theo chu kỳ mục tiêu 60 giây, lấy `station_status` và `station_information`.
2. **Raw Storage:** Lưu raw JSON cùng metadata thu thập vào MinIO.
3. **Validation:** Pydantic kiểm tra schema, kiểu dữ liệu và các trường bắt buộc.
4. **Kafka Producer:** Gửi message theo từng trạm vào Kafka, sử dụng `station_id` làm message key.
5. **Historical Consumer:** Spark Structured Streaming đọc Kafka, xử lý và ghi lịch sử vào Iceberg Bronze/Silver.
6. **Latest State Consumer:** Consumer độc lập đọc Kafka và thực hiện UPSERT vào PostgreSQL, giữ một bản ghi mới nhất cho mỗi `station_id`.
7. **Dashboard:** Grafana truy vấn PostgreSQL để hiển thị trạng thái trạm và cảnh báo; truy vấn Trino/Iceberg để phân tích lịch sử.

### Quy tắc xử lý

- **Lịch sử polling:** Iceberg lưu các quan sát theo từng trạm và lần thu thập, kể cả khi số xe không thay đổi.
- **Chống duplicate:** Nhận diện observation bằng `(station_id, poll_id)` hoặc `(station_id, collected_at)` nếu không sử dụng `poll_id`.
- **Latest state:** PostgreSQL sử dụng `station_id` làm khóa chính; UPSERT chỉ chấp nhận observation mới hơn, tránh dữ liệu cũ ghi đè.
- **Timestamp:** Phân biệt `last_updated`, `last_reported` và `collected_at`.
- **Staleness:** Tính độ cũ của dữ liệu dựa trên thời gian hiện tại và `last_reported`.
- **Streaming trigger:** Spark sử dụng micro-batch phù hợp, không bắt buộc commit Iceberg mỗi 60 giây; thực hiện compaction để hạn chế small files.
- **Fault tolerance:** Hỗ trợ Kafka replay, Spark checkpoint và khôi phục PostgreSQL từ lịch sử Iceberg.
- **Data Quality:** Bản ghi không hợp lệ được ghi log hoặc chuyển vào vùng lỗi để kiểm tra.

### Nguyên tắc lưu trữ

- **Iceberg trên MinIO:** Lưu lịch sử quan sát, phục vụ phân tích và tái xử lý.
- **PostgreSQL:** Lưu latest state và active alerts để truy vấn nhanh.
- **Kafka:** Truyền dữ liệu cho hai consumer độc lập.
- **Không sử dụng CDC:** PostgreSQL được cập nhật trực tiếp bằng UPSERT, không thông qua Debezium.

---

## Slide 10 — Thiết kế Lakehouse và Data Models

**Kiến trúc lưu trữ:** Apache Iceberg (Bronze → Silver → Gold) kết hợp PostgreSQL Serving Layer.

### 1. Iceberg Lakehouse — Lưu trữ lịch sử và phân tích

**Bronze Layer**
- Lưu dữ liệu đã ingest từ Trip History và GBFS.
- Giữ thông tin nguồn để hỗ trợ kiểm tra và tái xử lý.

**Silver Layer**

| Bảng | Grain |
|---|---|
| `silver.trips` | Một dòng = một `ride_id` |
| `silver.gbfs_status` | Một dòng = một trạm tại một lần polling |
| `silver.gbfs_station_information` | Thông tin định danh trạm theo bản ghi/phiên bản được quan sát |

**Gold Layer**

| Bảng | Mục đích |
|---|---|
| `gold.station_hourly_metrics` | Thống kê số lượt START/END theo ngày, giờ và trạm |
| `gold.station_id_bridge_current` | Ánh xạ station ID giữa Trip History và GBFS |

**Grain `station_hourly_metrics`:** Một trạm × một ngày × một giờ × một hướng START/END.

### 2. PostgreSQL Serving Layer — Trạng thái hiện tại

PostgreSQL được cập nhật trực tiếp từ Kafka thông qua **Latest State Consumer**, sử dụng UPSERT.

| Bảng | Grain / Mục đích |
|---|---|
| `station_status_latest` | Một dòng = trạng thái mới nhất của một `station_id` |
| `active_alerts` | Các cảnh báo hiện đang hoạt động tại trạm |

**`station_status_latest`** lưu:
- `station_id`, `num_bikes_available`, `num_docks_available`.
- `last_reported`, `collected_at`.
- `is_installed`, `is_renting`, `is_returning`.

**Nguyên tắc cập nhật:**
- Sử dụng `station_id` làm khóa chính.
- UPSERT chỉ chấp nhận observation mới hơn, tránh ghi đè bởi dữ liệu cũ.
- Trạng thái `STALE` được xác định dựa trên thời gian hiện tại và `last_reported`.
- PostgreSQL có thể được khôi phục từ lịch sử GBFS trong Iceberg.

### 3. Analytics và Dashboard

- **Iceberg Gold → Trino → Grafana:** Phân tích nhu cầu sử dụng lịch sử.
- **PostgreSQL → Grafana:** Hiển thị trạng thái trạm mới nhất và cảnh báo.

### 4. Nguyên tắc thiết kế

- Iceberg là nguồn lưu trữ lịch sử chính; PostgreSQL là cơ sở dữ liệu phục vụ truy vấn trạng thái hiện tại.
- Không JOIN trực tiếp dữ liệu chuyến đi với toàn bộ lịch sử polling vì có thể làm nhân bản số liệu.
- Không sử dụng CDC/Debezium; PostgreSQL được cập nhật bằng UPSERT từ Kafka consumer.
- Đảm bảo khả năng retry, replay và đối soát dữ liệu giữa Iceberg và PostgreSQL.

---

## Slide 11 — Data Quality, Timezone và Station Mapping

### 1. Timezone

- Trip History chứa timestamp không có UTC offset.
- Nhóm áp dụng `America/New_York` dựa trên địa bàn và bằng chứng DST.
- Giữ timestamp gốc, local timestamp và UTC khi xác định được.
- Đánh dấu `AMBIGUOUS` và `NONEXISTENT` quanh thời điểm đổi DST.
- Dashboard phân tích nhu cầu theo giờ địa phương New York.

### 2. Station ID Mapping

- ID của Trip History và GBFS không thể JOIN trực tiếp.
- Dùng `station_information.short_name` làm cầu nối giữa hai hệ ID.
- Tạo bảng `station_id_bridge_current`.
- Giữ các trạm unmatched, không âm thầm loại bỏ.
- Lưu lịch sử thay đổi mapping từ thời điểm bắt đầu quan sát.
- Không giả định mapping hiện tại luôn đúng cho toàn bộ dữ liệu lịch sử.

### 3. Data Quality Tests

- Kiểm tra schema và số dòng đầu vào.
- Kiểm tra tính duy nhất của `ride_id`.
- Kiểm tra grain của các bảng Silver/Gold.
- Kiểm tra duplicate polling.
- Kiểm tra timezone/DST.
- Kiểm tra tỷ lệ station mapping và các bản ghi unmatched.

---

## Slide 12 — Tổ chức GitHub Repository và phân công nhóm

### Repository

`citibike-bigdata/`

| Thư mục | Chức năng |
|---|---|
| `ingestion/` | Downloader, GBFS Poller, Pydantic, Kafka Producer |
| `batch/` | PySpark Batch jobs và timezone utilities |
| `streaming/` | Spark Streaming jobs và deduplication |
| `models/` | dbt models, Silver sources và Gold SQL |
| `tests/` | Unit, integration, data quality, end-to-end |
| `infra/` | Docker và cấu hình các dịch vụ |
| `docs/` | Kiến trúc, data models, timezone policy, runbook |

**File gốc repository:** `README.md`, `docker-compose.yml`, `.env.example`, `.gitignore`.

### Phân công 4 thành viên

- **Thành viên 1:** Batch ingestion và Trip History processing.
- **Thành viên 2:** GBFS Poller, Kafka và Spark Streaming.
- **Thành viên 3:** Iceberg, dbt, Data Modeling và Station Bridge.
- **Thành viên 4:** Docker Compose, Trino, Grafana và Airflow.

Tất cả thành viên cùng tham gia integration tests, tài liệu và chạy end-to-end.

---

## Slide 13 — Deliverables và tiêu chí hoàn thành

### Sản phẩm bàn giao

1. GitHub repository có cấu trúc rõ ràng và hướng dẫn triển khai.
2. Batch pipeline xử lý Trip History NYC 01–10/2026.
3. Streaming pipeline thu thập GBFS 2.3 gần thời gian thực.
4. Lakehouse Bronze/Silver/Gold trên Apache Iceberg.
5. Các data models và data quality tests.
6. Grafana dashboard phân tích nhu cầu và trạng thái trạm.
7. Cơ chế cảnh báo thiếu xe, hết xe, ngừng hoạt động và dữ liệu stale.
8. Airflow DAGs và tài liệu vận hành.

### Tiêu chí nghiệm thu

- Batch xử lý đủ các file đã xác minh, không thiếu CSV.
- Chạy lại pipeline không làm tăng số chuyến do duplicate.
- Streaming ghi nhận được nhiều lần polling liên tiếp.
- Silver/Gold tuân thủ grain đã định nghĩa.
- Dashboard hiển thị đúng biểu đồ theo giờ, ngày trong tuần, Top 10 START/END.
- Bảng trạng thái trạm hiển thị số xe, số dock và lần cập nhật cuối.
- Cảnh báo phân biệt được hết xe, ngừng hoạt động và dữ liệu stale.
- Hệ thống có thể khởi động, chạy thử và kiểm tra end-to-end theo tài liệu.

### Kết luận

**Dự án xây dựng một nền tảng Big Data tích hợp Batch và Near-real-time Streaming, giúp phân tích nhu cầu sử dụng Citi Bike từ dữ liệu lịch sử và giám sát tình trạng trạm hiện tại trên một dashboard thống nhất.**

Phạm vi ưu tiên tính đúng đắn của dữ liệu, khả năng tái xử lý, khả năng tích hợp giữa các module và khả năng vận hành ổn định.