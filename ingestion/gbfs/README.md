# GBFS schema — STR-01

## Trạng thái

Bản triển khai ban đầu, chờ review với người phụ trách
Schema / Data Quality. Chưa xác nhận bao phủ toàn bộ GBFS v2.3.

## Cài thư viện

Chạy từ thư mục gốc repository:

```bash
python -m pip install -r ingestion/gbfs/requirements.txt
```

## Chạy test

```bash
python -m unittest discover -s tests/unit -p "test_gbfs_schemas.py" -v
```

## Xuất JSON Schema của message chuẩn hóa

```bash
python -m ingestion.gbfs.export_schemas
```

## Quy tắc triển khai hiện tại

- Message chuẩn hóa có cấu trúc phẳng.
- Các cờ hoạt động nhận 0/1 hoặc boolean, xuất thành boolean.
- Số lượng xe và dock dùng số nguyên không âm.
- Trường nguồn bổ sung được giữ lại.
- Trường tùy chọn bị thiếu không tự điền thành 0.
- Khi xuất observation, dùng exclude_unset=True.
- last_reported cũ được giữ nguyên, không thay bằng giờ hiện tại.
- collected_at và detected_at phải có thông tin múi giờ.
- Các JSON Schema được xuất cho dữ liệu sau chuẩn hóa.

## Kết quả kiểm tra hiện tại

- Đã kiểm tra 2.519 bản ghi station_status: 0 lỗi.
- Đã kiểm tra 2.519 bản ghi station_information: 0 lỗi.
- Đã chạy thành công 14 unit tests.
- Kết quả trên áp dụng cho bộ mẫu đã tải và các test hiện có.

## Các mục chờ review

- Chính sách reject num_bikes_available = null hiện là bản nháp.
- Tính bắt buộc, nullable và cấu trúc đầy đủ của các trường
  cần đối chiếu tiếp với chuẩn GBFS v2.3.
- Trường ngoài schema được giữ nhưng chưa được validate chi tiết.
- Grain/snapshot policy của station_information chưa chốt.
- Chưa kiểm thử polling, Kafka, MinIO hoặc PostgreSQL.
