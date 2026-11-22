# Ingestion

Thu thập dữ liệu nguồn và đưa vào MinIO / Kafka. Quy tắc dữ liệu nằm trong `docs/contracts.md`; README này chỉ là mục lục.

| Thư mục | Nội dung | Phụ trách | Issue | Contract |
|---|---|---|---|---|
| `trip_history/` | Downloader Trip History theo tháng, manifest, giải nén ZIP → CSV staging *(dự kiến)* | Khang | BAT-01..03 | 1.1, 2.2.1, 2.2.2 |
| `gbfs/` | GBFS poller, Pydantic validation, Kafka producer, raw JSON lên MinIO; hướng dẫn chạy ở `gbfs/README.md` *(có sau khi PR #1 merge)* | Quan | STR-01..04 | 1.4, 1.5, 2.3 |
