# ĐỀ TÀI: CITI BIKE BIG DATA PIPELINE
**Xây dựng hệ thống xử lý dữ liệu Batch và Streaming để phân tích nhu cầu sử dụng và giám sát trạng thái trạm xe đạp Citi Bike**

## 📚 Tài liệu

Mỗi thông tin chỉ được định nghĩa ở **một** tài liệu; các tài liệu khác dẫn link tới đó thay vì chép lại.

| Tài liệu | Vai trò | Khi nào sửa |
|---|---|---|
| [docs/scope.md](docs/scope.md) | Mục tiêu, phạm vi, kiến trúc, tiêu chí nghiệm thu | Khi nhóm đổi phạm vi đề tài |
| [docs/contracts.md](docs/contracts.md) | **Chuẩn bắt buộc**: schema, grain, quy tắc dữ liệu, timezone, cấu hình runtime, người phụ trách, truy vết scope → issue, quyết định còn mở | Trong cùng PR với code/schema/test thay đổi chuẩn |
| [docs/runbook.md](docs/runbook.md) | Definition of Done, quy trình issue/PR, chỉ dẫn vận hành | Khi đổi quy trình làm việc |
| [infra/README.md](infra/README.md) | Cách khởi động và vận hành stack Docker Compose | Khi đổi `docker-compose.yml` hoặc `infra/` |
| `<module>/README.md` | Cách chạy và test từng module | Khi đổi cách chạy module; quy tắc dữ liệu thì link về `docs/contracts.md` |

Khi hai tài liệu mâu thuẫn: `docs/contracts.md` quyết định chi tiết kỹ thuật, `docs/scope.md` quyết định phạm vi. Sửa tài liệu sai trong cùng PR.

Sơ đồ: [kiến trúc](docs/architechture.png), [luồng streaming](docs/streaming-process.png), [luồng batch](docs/batch-process.png), [phối hợp data & infra](<docs/Sơ đồ data&infra contract.png>).

## 🚀 Bắt đầu nhanh

```bash
cp .env.example .env
docker compose up -d --build
bash tests/smoke/infra_smoke.sh
```

Chi tiết, lệnh cho từng thành viên và xử lý sự cố: [infra/README.md](infra/README.md).

## 📁 Cấu trúc repository

Đánh dấu *(dự kiến)* là entry point đã khai báo trong `docker-compose.yml` nhưng code chưa có trên `main`.

```text
.
├── README.md
├── docker-compose.yml            # stack local + entry point của từng module
├── .env.example                  # biến môi trường, mật khẩu, port
├── docs/
│   ├── scope.md                  # phạm vi đề tài
│   ├── contracts.md              # data & infra contracts
│   ├── runbook.md                # Definition of Done, quy trình
│   └── *.png                     # sơ đồ kiến trúc và luồng xử lý
├── infra/                        # cấu hình Docker và các dịch vụ (Nguyen)
│   ├── python/  spark/           # Dockerfile + requirements
│   ├── minio/  kafka/  iceberg/  # job khởi tạo bucket, topic, namespace
│   ├── postgres/init.sql         # bảng serving (contracts mục 1.7)
│   ├── trino/  dbt/  grafana/  airflow/
│   └── README.md
├── ingestion/                    # README: ingestion/README.md
│   ├── trip_history/downloader.py        # (dự kiến) Khang — BAT-01..03
│   └── gbfs/                             # Quan — STR-01..04 (nhánh feature/gbfs-streaming, PR #1)
├── batch/
│   └── jobs/trips_bronze.py, trips_silver.py   # (dự kiến) Khang — BAT
├── streaming/
│   └── jobs/gbfs_history.py      # (dự kiến) Nguyen — STR-07..12 (nhánh str-07)
├── serving/
│   ├── latest_state_consumer.py  # Quan — STR-05, STR-05b (PR #1)
│   └── reconcile_latest_state.py # (dự kiến) giai đoạn sau, chưa có issue
├── models/
│   └── dbt/                      # (dự kiến) Kien — MOD-01, MOD-02
└── tests/                        # README: tests/README.md
    ├── smoke/                    # smoke test hạ tầng (INF-01)
    ├── unit/  integration/
    └── streaming/                # (có sau khi nhánh str-07 merge)
```

Data models, timezone policy và quy tắc data quality nằm trong `docs/contracts.md` mục 1 và 3, không tách thành file riêng.
