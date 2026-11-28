
## Quickstart: Raw Staging & Bronze Ingestion

### 1. Start Object Storage & Initialize Buckets
Start MinIO and verify that the bucket initialization job completes cleanly:

```bash
docker compose up -d minio minio-init
docker logs citibike-minio-init-1
```

### 2. Download & Stage Raw Trip Data
Install local dependencies (if running tests/scripts outside the container) and run the downloader tool to stage raw archives into MinIO:

```bash
python -m pip install boto3 requests python-dotenv pyspark==3.5.1 pytest
docker compose --profile tools run --rm trip-downloader
```

### 3. Run Spark Bronze Job (`raw` → `bronze`)
Bring up the Iceberg REST catalog and Spark cluster, then run the Bronze batch job for your target month range (idempotent per `source_month`):

```bash
docker compose up -d minio iceberg-rest catalog-init spark-master spark-worker
docker compose --profile tools run --rm \
  -e TRIP_START_MONTH=2026-01 \
  -e TRIP_END_MONTH=2026-01 \
  spark-batch
```

### Where Outputs Are Stored

| Layer / Service | Location / Identifier | Notes |
| :--- | :--- | :--- |
| **Staged Raw Data** | `s3a://raw/trip_history/staging/<YYYY-MM>/` | Source CSVs in MinIO; left untouched by Bronze |
| **Bronze Iceberg Table** | `lakehouse.bronze.citibike_trips` | Registered in Iceberg REST catalog, partitioned by `source_month` |
| **Bronze Parquet & Metadata** | `s3://warehouse/bronze/citibike_trips/` | Data files and Iceberg snapshots stored in MinIO `warehouse` bucket |