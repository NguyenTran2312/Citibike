# GBFS Ingestion — STR-01 và STR-02

Module thu thập và chuẩn hóa dữ liệu Citi Bike GBFS 2.3.

## Trạng thái hiện tại

- Đã xây dựng Pydantic models và xuất JSON Schema.
- Đã chạy thành công 16 unit tests cho schema.
- Poller đã chạy thành công một lần với cả hai feed.
- Đã viết lịch polling độc lập; chưa hoàn tất kiểm thử lịch chạy.
- Chưa tích hợp Kafka, MinIO và PostgreSQL.

Schema hiện tại sử dụng danh sách trường chuẩn hóa do nhóm lựa chọn,
không đại diện cho toàn bộ các trường của GBFS 2.3.

## Cài thư viện

Chạy các lệnh từ thư mục gốc repository, trong môi trường Python riêng:

```bash
python -m pip install -r ingestion/gbfs/requirements.txt
```

## Các file chính

| File | Vai trò |
|---|---|
| `schemas.py` | Kiểm tra và chuẩn hóa bản ghi, metadata và message lỗi |
| `inspect_sample.py` | Kiểm kê trường và kiểu dữ liệu trong file mẫu |
| `export_schemas.py` | Xuất JSON Schema của message chuẩn hóa |
| `poller.py` | Tìm URL feed, thu thập, chuẩn hóa và lưu kết quả trên máy |
| `json_schemas/` | Các JSON Schema để producer và consumer tham chiếu |
| `requirements.txt` | Thư viện Python cần cài |

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
- JSON nguồn đầy đủ được lưu riêng trong `raw.json`.
- Các cờ hoạt động đầu vào chỉ nhận 0/1 hoặc boolean.
- Cờ hoạt động được chuẩn hóa thành boolean.
- Số lượng xe và dock không được âm.
- Không tự chuyển số lượng bị thiếu thành 0.
- Xuất message bằng `model_dump_json()`, để trường nullable bị thiếu
  có giá trị null trong đầu ra.
- Không thay `last_reported` cũ bằng giờ hiện tại.
- Poller tạo `collected_at` bằng thời gian UTC có timezone.
- `last_updated` được lấy từ metadata cấp feed.

## Polling

| Feed | Chu kỳ mặc định |
|---|---|
| `station_status` | 60 giây |
| `station_information` | 86.400 giây, tương đương 24 giờ |

Cả hai feed được lấy khi chương trình khởi động.

Mỗi feed có mã polling riêng để truy vết file raw.
Mã polling của information không được xuất vào message chuẩn hóa.

Các lần polling khác nhau vẫn được ghi nhận khi trạng thái không đổi.
Khi bổ sung replay, phải giữ nguyên metadata của observation gốc.

### Chạy một lần

```bash
python -m ingestion.gbfs.poller --once
```

### Chạy thử trong 130 giây

```bash
python -m ingestion.gbfs.poller --duration 130
```

### Chạy liên tục

```bash
python -m ingestion.gbfs.poller
```

Nhấn Ctrl+C để yêu cầu dừng.
Nếu đang gọi API, chương trình chờ lần gọi đó kết thúc.

### Biến cấu hình

| Biến | Mặc định |
|---|---|
| `GBFS_DISCOVERY_URL` | `https://gbfs.lyft.com/gbfs/2.3/bkn/gbfs.json` |
| `POLL_INTERVAL_SECONDS` | `60` |
| `GBFS_INFORMATION_INTERVAL_SECONDS` | `86400` |

## Đầu ra chạy thử

Mỗi lần thu thập lưu tại:

`data/gbfs/<feed_name>/<poll_id>/`

| File | Nội dung |
|---|---|
| `raw.json` | JSON nguồn, URL, poll_id và collected_at |
| `valid.jsonl` | Mỗi dòng là một message hợp lệ |
| `invalid.jsonl` | Mỗi dòng là một message mô tả bản ghi lỗi |

Thư mục `data/` không được commit lên GitHub.

Đây là lưu trữ trên máy để phát triển.
Tích hợp raw storage trên MinIO thuộc STR-04.

## Xử lý lỗi hiện tại

- Bản ghi trạm sai cấu trúc hoặc vi phạm schema được ghi vào invalid.
- Lỗi mạng không được biến thành message invalid.
- Trong chế độ chạy liên tục, lỗi polling được log và thử lại
  ở chu kỳ tiếp theo.
- Retry có giới hạn, backoff, jitter và xử lý lỗi cấp feed đầy đủ
  chưa được triển khai.

## Chạy test schema

```bash
python -m unittest discover -s tests/unit -p "test_gbfs_schemas.py" -v
```

Kết quả đã ghi nhận: 16 tests thành công.

Các test này chưa xác nhận lịch polling, Kafka, MinIO,
PostgreSQL hoặc khả năng phục hồi sau crash.

## Xuất JSON Schema

```bash
python -m ingestion.gbfs.export_schemas
```

Các file được tạo trong `ingestion/gbfs/json_schemas/`.

Đây là schema của message sau chuẩn hóa.

## Các mục cần hoàn thiện

- Review schema với người phụ trách Data Modeling / Data Quality.
- Đồng bộ danh sách trường mới vào contract chung của nhóm.
- Chốt chính sách xử lý `num_bikes_available = null`.
- Chốt grain và chính sách snapshot của information.
- Kiểm thử lịch polling và metadata đầu ra.
- Tích hợp Kafka Producer và topic invalid.
- Tích hợp MinIO, retry và backoff.
- Xây dựng PostgreSQL consumer và kiểm thử replay/recovery.
