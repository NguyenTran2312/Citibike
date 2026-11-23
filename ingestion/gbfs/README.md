# GBFS Ingestion — STR-01 đến STR-04

Module thu thập, chuẩn hóa và gửi dữ liệu Citi Bike GBFS 2.3 vào Kafka,
đồng thời lưu raw JSON lên MinIO.

## Trạng thái hiện tại

- Đã xây dựng Pydantic models và xuất JSON Schema (STR-01).
- Poller lấy `station_status` mỗi 60 giây và `station_information`
  mỗi 24 giờ, mỗi feed có `poll_id` riêng (STR-02).
- Đã tích hợp Kafka Producer, key là `station_id`; bản ghi lỗi
  được định tuyến sang `gbfs.invalid` (STR-03).
- Đã lưu raw JSON lên MinIO và thêm retry có giới hạn, backoff,
  jitter khi gọi GBFS API (STR-04).
- Đã chạy thành công 32 unit tests:
  16 schema, 4 vòng polling, 9 producer và 3 retry.
- Đã đối soát một lần polling của mỗi feed trên Kafka local:
  2.519 message/feed, khớp key, nội dung và số lượng.
- Latest State Consumer ghi PostgreSQL nằm ở
  `serving/latest_state_consumer.py` (STR-05), không thuộc module này.

Schema hiện tại sử dụng danh sách trường chuẩn hóa do nhóm lựa chọn,
không đại diện cho toàn bộ các trường của GBFS 2.3.

## Cài thư viện

Chỉ cần khi chạy trên máy host. Chạy từ thư mục gốc repository,
trong môi trường Python riêng (Python 3.11, giống image Docker):

```bash
python -m pip install -r ingestion/gbfs/requirements.txt
```

Khi chạy bằng Docker Compose, image `citibike/python:local`
đã cài sẵn các thư viện trong `infra/python/requirements.txt`.

## Các file chính

| File | Vai trò |
|---|---|
| `schemas.py` | Kiểm tra và chuẩn hóa bản ghi, metadata và message lỗi |
| `poller.py` | Tìm URL feed, thu thập, chuẩn hóa, lưu file, gọi archive và producer |
| `producer.py` | Gửi message vào Kafka và xác nhận số message broker đã nhận |
| `raw_storage.py` | Upload `raw.json` lên MinIO |
| `inspect_sample.py` | Kiểm kê trường và kiểu dữ liệu trong file mẫu |
| `export_schemas.py` | Xuất JSON Schema của message chuẩn hóa |
| `json_schemas/` | Các JSON Schema để producer và consumer tham chiếu |
| `requirements.txt` | Thư viện Python cần cài khi chạy trên host |

## Schema message chuẩn hóa

### station_status

| Trường | Kiểu |
|---|---|
| `poll_id` | string |
| `last_updated` | integer |
| `collected_at` | datetime có timezone |
| `station_id` | string |
| `num_bikes_available` | integer không âm |
| `num_docks_available` | integer không âm |
| `is_installed` | boolean |
| `is_renting` | boolean |
| `is_returning` | boolean |
| `last_reported` | integer |
| `schema_version` | integer, cố định bằng 1 |
| `vehicle_types_available` | array hoặc null |

Mỗi phần tử của `vehicle_types_available` gồm:

- `vehicle_type_id`: string không rỗng.
- `count`: integer không âm.

Khóa logic của observation trạng thái là `(station_id, poll_id)`.

### station_information

| Trường | Kiểu |
|---|---|
| `last_updated` | integer |
| `collected_at` | datetime có timezone |
| `station_id` | string |
| `name` | string |
| `lat` | number, từ -90 đến 90 |
| `lon` | number, từ -180 đến 180 |
| `short_name` | string hoặc null |
| `capacity` | integer không âm hoặc null |
| `is_virtual_station` | boolean hoặc null |

Message chuẩn hóa không chứa `poll_id` hoặc `schema_version`.

`is_virtual_station` phân biệt điểm đỗ không có dock thông minh
với trạm có dock thông minh. Nếu nguồn không cung cấp, giữ null.

### invalid

| Trường | Kiểu |
|---|---|
| `source` | string |
| `error_type` | string |
| `error_message` | string |
| `raw_payload` | dữ liệu gốc gây lỗi |
| `detected_at` | datetime có timezone |

Message lỗi không được có trường ngoài schema đã định nghĩa.

## Quy tắc chuẩn hóa

- Các trường message nằm cùng cấp, không bọc trong object `station`.
- Chỉ xuất những trường được định nghĩa trong schema.
- JSON nguồn đầy đủ được lưu riêng trong `raw.json` và trên MinIO.
- Các cờ hoạt động đầu vào chỉ nhận 0/1 hoặc boolean.
- Cờ hoạt động được chuẩn hóa thành boolean.
- Số lượng xe và dock không được âm.
- Không tự chuyển số lượng bị thiếu thành 0.
- Xuất message bằng `model_dump_json()`, để trường nullable bị thiếu
  có giá trị null trong đầu ra.
- Không thay `last_reported` cũ bằng giờ hiện tại.
- Poller tạo `collected_at` bằng thời gian UTC có timezone,
  ngay sau khi nhận phản hồi từ API.
- `last_updated` được lấy từ metadata cấp feed.

## Polling

| Feed | Chu kỳ mặc định |
|---|---|
| `station_status` | 60 giây |
| `station_information` | 86.400 giây, tương đương 24 giờ |

- Cả hai feed được lấy ngay khi chương trình khởi động,
  mỗi feed chạy trong một luồng riêng.
- URL của feed được tìm một lần qua discovery URL lúc khởi động.
- Chu kỳ tính cả thời gian gọi API và xử lý; nếu một lần polling
  chạy quá chu kỳ, lần tiếp theo chờ tối thiểu 1 giây,
  không chạy dồn các lần đã lỡ.
- Mỗi lần polling có `poll_id` dạng `<feed_name>-<uuid4>`,
  hai feed không dùng chung `poll_id`.
  Mã polling của information chỉ dùng cho đường dẫn raw,
  không được xuất vào message chuẩn hóa.
- Các lần polling khác nhau vẫn được ghi nhận khi trạng thái không đổi.
  Khi bổ sung replay, phải giữ nguyên metadata của observation gốc.

### Thứ tự xử lý một lần polling

1. Gọi API (có retry), ghi nhận `collected_at`.
2. Ghi `raw.json` vào thư mục đầu ra trên máy.
3. Validate từng trạm, ghi `valid.jsonl` và `invalid.jsonl`.
4. Nếu `GBFS_PUBLISH_KAFKA=true`: gửi message vào Kafka,
   chờ broker xác nhận rồi ghi `kafka_delivery.json`.
5. Nếu `GBFS_ARCHIVE_RAW=true`: upload `raw.json` lên MinIO.
   Bước này chạy sau Kafka và chạy cả khi bước 3–4 lỗi;
   MinIO lỗi chỉ được log, không làm lần polling thất bại.
6. Nếu đã bật gửi Kafka và không đặt `GBFS_KEEP_LOCAL=true`:
   xóa thư mục đầu ra **chỉ khi mọi bước đã bật đều thành công**
   (Kafka xác nhận, và upload MinIO nếu có bật). Nếu không, thư mục
   được giữ lại, log ghi `Giữ lại ... để replay`, và có thể gửi lại
   bằng `--replay` (xem mục Replay).

### Biến cấu hình

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `GBFS_DISCOVERY_URL` | `https://gbfs.lyft.com/gbfs/2.3/bkn/gbfs.json` | Discovery URL của GBFS 2.3 |
| `POLL_INTERVAL_SECONDS` | `60` | Chu kỳ `station_status` |
| `INFORMATION_POLL_INTERVAL_SECONDS` | `86400` | Chu kỳ `station_information`; nếu không đặt, dùng `GBFS_INFORMATION_INTERVAL_SECONDS` |
| `GBFS_PUBLISH_KAFKA` | `false` | Gửi message vào Kafka |
| `GBFS_ARCHIVE_RAW` | `false` | Upload `raw.json` lên MinIO |
| `GBFS_DATA_DIR` | `data/gbfs` | Thư mục đầu ra trên máy |
| `GBFS_KEEP_LOCAL` | `false` | Giữ thư mục đầu ra sau khi đã gửi Kafka (để đối soát) |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker |
| `GBFS_STATUS_TOPIC` | `gbfs.station_status` | Topic status |
| `GBFS_INFORMATION_TOPIC` | `gbfs.station_information` | Topic information |
| `GBFS_INVALID_TOPIC` | `gbfs.invalid` | Topic bản ghi lỗi |
| `S3_ENDPOINT_URL` | bắt buộc khi archive | Endpoint MinIO |
| `AWS_ACCESS_KEY_ID` | bắt buộc khi archive | MinIO user (`MINIO_ROOT_USER`) |
| `AWS_SECRET_ACCESS_KEY` | bắt buộc khi archive | MinIO password (`MINIO_ROOT_PASSWORD`) |
| `RAW_BUCKET` | `raw` | Bucket lưu raw JSON |

Trong Docker Compose, service `gbfs-poller` đã đặt sẵn các biến trên,
bao gồm `GBFS_PUBLISH_KAFKA=true` và `GBFS_ARCHIVE_RAW=true`.

## Cách chạy polling

### Cách 1 — Docker Compose (khuyến nghị)

Khởi động stack mặc định một lần để tạo bucket MinIO và Kafka topics:

```bash
docker compose up -d
```

Chạy poller liên tục (gửi Kafka và lưu raw lên MinIO):

```bash
docker compose --profile pipeline up -d --build gbfs-poller
```

Xem log:

```bash
docker compose logs -f gbfs-poller
```

Mỗi lần polling thành công có các dòng `feed=... valid=... invalid=...`,
`Raw archived feed=... uri=s3://raw/...` và `Kafka confirmed feed=...`.

Dừng poller:

```bash
docker compose --profile pipeline stop gbfs-poller
```

File đầu ra trong container nằm ở `/app/data/gbfs/`, trên volume
`gbfs-spool`. Thư mục bị xóa sau mỗi lần polling thành công; thư mục của
lần polling lỗi được giữ lại để replay, kể cả khi container bị tạo lại
(`up --force-recreate`). Chỉ `docker compose down -v` mới xóa volume này.

### Cách 2 — Trên máy host, chỉ ghi file

Không cần Kafka hoặc MinIO. Phù hợp để kiểm tra schema và dữ liệu nguồn.

Chạy một lần:

```bash
python -m ingestion.gbfs.poller --once
```

Chạy thử trong 130 giây:

```bash
python -m ingestion.gbfs.poller --duration 130
```

Chạy liên tục:

```bash
python -m ingestion.gbfs.poller
```

### Cách 3 — Trên máy host, gửi Kafka và lưu MinIO

Cần stack Docker Compose đang chạy (`docker compose up -d`).
Python trên host dùng `localhost:9092` cho Kafka và
`http://localhost:9000` cho MinIO; ứng dụng trong mạng Compose
dùng `kafka:29092` và `http://minio:9000`.

Thay `<MINIO_ROOT_PASSWORD>` bằng giá trị trong `.env`
(mặc định của Compose là `citibike-local-secret`):

```bash
GBFS_PUBLISH_KAFKA=true GBFS_ARCHIVE_RAW=true S3_ENDPOINT_URL=http://localhost:9000 AWS_ACCESS_KEY_ID=citibike AWS_SECRET_ACCESS_KEY=<MINIO_ROOT_PASSWORD> python -m ingestion.gbfs.poller --once
```

Bỏ `--once` để chạy liên tục, hoặc dùng `--duration <giây>`.

### Dừng chương trình

Ở chế độ liên tục, nhấn Ctrl+C hoặc gửi SIGTERM (`docker stop`)
để yêu cầu dừng:

- Đang gọi GBFS: lần thử hiện tại chạy xong (tối đa 30 giây) nhưng không
  retry nữa; lần polling đó bị bỏ vì chưa có gì được ghi.
- Đã lấy được dữ liệu: chạy nốt validate, gửi Kafka (tối đa 35 giây) và
  upload MinIO rồi mới dừng.

Compose cho poller tối đa 120 giây (`stop_grace_period`) trước khi buộc
dừng. Nếu bị buộc dừng sau khi đã ghi `raw.json`, thư mục được giữ lại
để `--replay`. Kiểm thử: `tests/integration/test_poller_shutdown.py`.

## Đầu ra trên máy

Mỗi lần thu thập lưu tại:

`data/gbfs/<feed_name>/<poll_id>/`

| File | Nội dung |
|---|---|
| `raw.json` | JSON nguồn, URL, poll_id và collected_at |
| `valid.jsonl` | Mỗi dòng là một message hợp lệ |
| `invalid.jsonl` | Mỗi dòng là một message mô tả bản ghi lỗi |
| `kafka_delivery.json` | Số message broker đã xác nhận, chỉ có khi gửi Kafka thành công |

Thư mục `data/` không được commit lên GitHub.
Mỗi lần polling `station_status` chiếm khoảng 2,8 MB. Khi gửi Kafka,
thư mục chỉ bị xóa nếu lần polling thành công trọn vẹn (trừ khi đặt
`GBFS_KEEP_LOCAL=true`); thư mục của lần polling lỗi được giữ lại, nên
dung lượng chỉ tăng trong thời gian Kafka hoặc MinIO gặp sự cố.
Khi không gửi Kafka, file luôn được giữ lại.

## Replay thư mục polling bị giữ lại

```bash
# Trong container (biến Kafka/MinIO đã có sẵn):
docker compose --profile pipeline exec gbfs-poller \
  python -m ingestion.gbfs.poller --replay /app/data/gbfs/station_status/<poll_id>

# Liệt kê các thư mục đang bị giữ lại:
docker compose --profile pipeline exec gbfs-poller ls /app/data/gbfs/station_status
```

- Gửi lại `valid.jsonl` và `invalid.jsonl` với **đúng `poll_id` cũ**;
  UPSERT ở PostgreSQL và dedup `(station_id, poll_id)` ở Silver
  nên gửi lại không tạo bản ghi trùng.
- Bỏ qua bước Kafka nếu thư mục đã có `kafka_delivery.json`
  (trường hợp chỉ MinIO lỗi), rồi upload lại raw nếu `GBFS_ARCHIVE_RAW=true`.
- Thành công trọn vẹn thì thư mục bị xóa; lỗi thì thư mục vẫn được giữ.
- Thư mục của feed sai cấu trúc (không có `valid.jsonl`) không replay được.

## Lưu raw JSON lên MinIO (STR-04)

Bật bằng `GBFS_ARCHIVE_RAW=true`. Sau bước gửi Kafka, poller upload
`raw.json` lên địa chỉ dưới đây. Upload là best-effort: nếu MinIO lỗi,
log ghi `Raw archive thất bại` và dữ liệu vẫn đi vào Kafka.

```text
s3://<RAW_BUCKET>/gbfs/<feed_name>/date=<YYYY-MM-DD>/<poll_id>/raw.json
```

- `date` lấy theo `collected_at` (UTC).
- Nội dung gồm JSON nguồn đầy đủ cùng `poll_id`, `source_url`
  và `collected_at`, đủ để kiểm tra và xử lý lại.
- Client S3 dùng path-style addressing, timeout kết nối 5 giây,
  timeout đọc 15 giây và tối đa 3 lần thử (retry mode `standard` của boto3).

Kiểm tra file đã lưu: mở MinIO console tại http://localhost:9001,
đăng nhập bằng `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`,
vào bucket `raw`, thư mục `gbfs/`.

## Retry và xử lý lỗi

### Gọi GBFS API

| Tình huống | Xử lý |
|---|---|
| HTTP 429, 500, 502, 503, 504 | Retry |
| Lỗi mạng (`URLError`, `ConnectionError`) hoặc timeout | Retry |
| HTTP khác (ví dụ 404) | Không retry, báo lỗi ngay |

- Tối đa 3 lần gọi cho mỗi request, timeout 30 giây mỗi lần.
- Thời gian chờ dùng exponential backoff với full jitter:
  ngẫu nhiên trong `[0, 1]` giây sau lần lỗi thứ nhất,
  `[0, 2]` giây sau lần lỗi thứ hai.
- Mỗi lần retry ghi log mức WARNING với URL, lý do và thời gian chờ.
- Khi hết số lần thử, lần polling đó thất bại. Ở chế độ liên tục,
  lỗi được log (`Polling thất bại`) và thử lại ở chu kỳ tiếp theo.
- Lỗi mạng không được biến thành message invalid.

### Validation

- Bản ghi trạm sai cấu trúc hoặc vi phạm schema được ghi vào
  `invalid.jsonl` và gửi sang `gbfs.invalid`.
- Lỗi cấp feed (thiếu `last_updated`, `data.stations` không phải
  danh sách) làm cả lần polling thất bại và chỉ được ghi log.

### Kafka Producer

- `enable.idempotence=true`, `acks=all`, `message.timeout.ms=30000`.
- Khi hàng đợi producer đầy, chờ tối đa 10 giây để có chỗ trống.
- Sau khi gửi, chờ broker xác nhận tối đa 35 giây; nếu còn message
  chưa xác nhận hoặc có lỗi delivery, lần polling báo lỗi
  và không ghi `kafka_delivery.json`.

## Kafka

Ba topic do `kafka-init` tạo (3 partitions/topic, replication factor 1,
giữ 7 ngày; số partition còn chờ nhóm benchmark):

- `gbfs.station_status`
- `gbfs.station_information`
- `gbfs.invalid`

Nếu chỉ cần Kafka mà không chạy cả stack:

```bash
docker compose up -d kafka
docker compose run --rm kafka-init
```

### Xác nhận gửi dữ liệu

Sau khi Kafka xác nhận toàn bộ message của lần polling,
poller ghi `kafka_delivery.json` trong thư mục đầu ra.

File này ghi nhận số message được broker xác nhận.
Nó không chứng minh consumer phía sau đã xử lý thành công.

### Đối soát dữ liệu

Chạy poller với `GBFS_KEEP_LOCAL=true` để giữ thư mục polling,
rồi thay đường dẫn dưới đây bằng thư mục cần kiểm tra:

```bash
python tests/integration/check_kafka_delivery.py data/gbfs/station_status/<poll_id>
```

Với information:

```bash
python tests/integration/check_kafka_delivery.py data/gbfs/station_information/<poll_id>
```

Đối soát kiểm tra key, nội dung và số lượng của các message hợp lệ.
Status được nhận diện bằng poll_id; information bằng collected_at.

## Chạy unit tests GBFS

```bash
python -m unittest discover -s tests/unit -p "test_gbfs*.py" -v
```

Kết quả đã ghi nhận: 56 tests thành công
(cần thêm `psycopg[binary]` cho test của consumer).

Các test kiểm tra schema, logic vòng polling, retry khi gọi API,
producer với Kafka client giả lập, thứ tự Kafka → MinIO, chỉ dọn file
khi lần polling thành công, replay, SIGTERM và cách Latest State
Consumer cách ly message hỏng.

Chu kỳ 24 giờ được kiểm tra phép tính thời gian bằng đồng hồ giả;
chưa chạy kiểm thử thực tế kéo dài 24 giờ.

## Xuất JSON Schema

```bash
python -m ingestion.gbfs.export_schemas
```

Các file được tạo trong `ingestion/gbfs/json_schemas/`.

Đây là schema của message sau chuẩn hóa.

## Hạn chế đã biết

- Nếu gửi Kafka thất bại giữa chừng, một phần message có thể đã được ghi;
  replay gửi lại cả lần polling (at-least-once), phía sau phải dedup.
- Replay chạy thủ công bằng `--replay`, chưa tự động.
- Nếu poller bị buộc dừng (SIGKILL, hết `stop_grace_period`) trước khi
  ghi `raw.json`, lần polling đó bị mất (lịch sử thiếu một observation);
  sau khi đã ghi `raw.json`, thư mục được giữ trên volume `gbfs-spool` và
  replay thủ công được nếu đã có `valid.jsonl`.
- Khi hết số lần retry, lỗi chỉ được ghi log, chưa phát alert.
- Chưa có unit test cho `raw_storage.py`.

## Các mục cần hoàn thiện

- Review schema với người phụ trách Data Modeling / Data Quality.
- Đồng bộ danh sách trường mới vào contract chung của nhóm.
- Chốt chính sách xử lý num_bikes_available = null.
- Chốt grain và chính sách snapshot của information,
  bao gồm việc có xuất `poll_id` của information hay không.
- Chốt có giữ thêm các trường nguồn đang bị loại
  (ví dụ `num_bikes_disabled`, `num_ebikes_available`, `region_id`)
  trước khi làm Spark Kafka Source (STR-07).
