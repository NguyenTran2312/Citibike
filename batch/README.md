# Batch

PySpark Batch cho Trip History: Bronze → Silver (`silver.trips`) trên Iceberg. Code chưa có trên `main`; entry point đã khai báo trong `docker-compose.yml` (`spark-batch`, `spark-batch-silver`), cách chạy xem `infra/README.md` mục 3.

- **Phụ trách:** Khang (`docs/contracts.md` mục 4).
- **Issue:** BAT-04..BAT-12 (issue cha #28).
- **Contract:** 1.1 (schema nguồn), 1.2 (timezone/DST), 1.3 (`silver.trips`), 2.2 (runtime & storage).
