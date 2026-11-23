# Latest State Consumer — STR-05

`latest_state_consumer.py` đọc `gbfs.station_status` và giữ một dòng
mới nhất cho mỗi trạm trong `public.station_status_latest`.

## Chạy

```bash
docker compose --profile pipeline up -d latest-state-consumer
docker compose logs -f latest-state-consumer
```

## Cách xử lý từng message

| Tình huống | Xử lý | Commit offset |
|---|---|---|
| Message hợp lệ | UPSERT; chỉ ghi đè khi `collected_at` mới hơn | Sau khi PostgreSQL commit |
| Sai schema, không phải JSON, value rỗng | Gửi sang `gbfs.invalid` (`CONSUMER_VALIDATION_ERROR`) | Sau khi broker xác nhận message invalid |
| PostgreSQL từ chối dữ liệu (`DataError`, `IntegrityError`) | Gửi sang `gbfs.invalid` (`DATABASE_REJECTED`) | Như trên |
| Mất kết nối PostgreSQL hoặc Kafka, gửi `gbfs.invalid` thất bại | Consumer dừng với lỗi | Không commit |

Message invalid theo đúng contract 5 trường của `gbfs.invalid`.
Vị trí gốc nằm trong Kafka headers `source_topic`, `source_partition`,
`source_offset`.

## Phục hồi khi lỗi

Consumer không tự retry kết nối. Compose đặt `restart: unless-stopped`,
nên Docker khởi động lại với độ trễ tăng dần (100 ms, nhân đôi, tối đa
1 phút) cho tới khi PostgreSQL hoạt động lại. Offset chưa commit được
đọc lại; UPSERT bỏ qua observation không mới hơn nên đọc lại không làm
sai dữ liệu.

SIGTERM (`docker stop`) và Ctrl+C dừng vòng lặp sau message hiện tại
và đóng consumer.

## Kiểm thử

```bash
python -m unittest tests.unit.test_gbfs_latest_state_consumer -v
```

Integration test với Kafka và PostgreSQL thật (PostgreSQL dừng/chạy lại,
consumer bị kill, replay, out-of-order, message hỏng): xem `tests/README.md`,
mục Latest State Consumer.
