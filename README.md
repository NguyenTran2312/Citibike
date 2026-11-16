# ĐỀ TÀI: CITI BIKE BIG DATA PIPELINE
**Xây dựng hệ thống xử lý dữ liệu Batch và Streaming để phân tích nhu cầu sử dụng và giám sát trạng thái trạm xe đạp Citi Bike**


## 📁 Project Structure

```text
citibike-bigdata/
│
├── README.md
├── docker-compose.yml
├── .env.example
├── .gitignore
│
├── ingestion/
│   ├── __init__.py
│   ├── trip_history/
│   │   ├── __init__.py
│   │   ├── downloader.py
│   │   └── manifest.py
│   └── gbfs/
│       ├── __init__.py
│       ├── poller.py
│       ├── producer.py
│       └── schemas.py
│
├── batch/
│   ├── __init__.py
│   ├── jobs/
│   │   ├── __init__.py
│   │   ├── trips_bronze.py
│   │   └── trips_silver.py
│   └── utils/
│       ├── __init__.py
│       └── timezone.py
│
├── streaming/
│   ├── __init__.py
│   ├── jobs/
│   │   ├── __init__.py
│   │   ├── gbfs_history.py
│   │   ├── gbfs_status_bronze.py
│   │   ├── gbfs_status_silver.py
│   │   └── gbfs_station_information_silver.py
│   └── utils/
│       ├── __init__.py
│       └── deduplication.py
│
├── serving/
│   ├── __init__.py
│   ├── latest_state_consumer.py
│   ├── postgres_repository.py
│   ├── alerts.py
│   └── reconcile_latest_state.py
│
├── models/
│   └── dbt/
│       ├── dbt_project.yml
│       ├── profiles.yml.example
│       ├── models/
│       │   ├── silver/
│       │   │   ├── sources.yml
│       │   │   └── gbfs_station_information_current.sql
│       │   └── gold/
│       │       ├── station_hourly_metrics.sql
│       │       ├── station_id_bridge_current.sql
│       │       └── schema.yml
│       └── snapshots/
│           └── station_mapping_snapshot.sql
│
├── tests/
│   ├── __init__.py
│   ├── unit/
│   │   ├── __init__.py
│   │   ├── test_timezone.py
│   │   ├── test_downloader.py
│   │   ├── test_deduplication.py
│   │   └── test_latest_state_consumer.py
│   ├── integration/
│   │   ├── __init__.py
│   │   ├── test_batch_pipeline.py
│   │   ├── test_streaming_pipeline.py
│   │   ├── test_postgres_serving.py
│   │   └── test_end_to_end.py
│   └── data_quality/
│       ├── __init__.py
│       ├── test_trip_grain.py
│       ├── test_gbfs_status_grain.py
│       ├── test_station_information.py
│       └── test_station_mapping.py
│
├── infra/
│   ├── python/
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   ├── minio/
│   │   └── init_buckets.sh
│   ├── postgres/
│   │   └── init.sql
│   ├── spark/
│   │   ├── Dockerfile
│   │   └── spark-defaults.conf
│   ├── kafka/
│   │   └── init_topics.sh
│   ├── dbt/
│   │   └── profiles.yml
│   ├── trino/
│   │   ├── config.properties
│   │   ├── jvm.config
│   │   └── catalog/
│   │       └── iceberg.properties
│   ├── grafana/
│   │   ├── datasources.yml
│   │   ├── dashboards/
│   │   │   ├── historical_demand.json
│   │   │   └── live_station_status.json
│   │   └── provisioning/
│   │       └── dashboards/
│   │           └── dashboards.yml
│   └── airflow/
│       └── dags/
│           ├── batch_pipeline.py
│           ├── lakehouse_maintenance.py
│           └── latest_state_reconciliation.py
│
└── docs/
    ├── architecture.md
    ├── data_models.md
    ├── timezone_policy.md
    ├── data_quality.md
    ├── station_mapping_policy.md
    └── runbook.md
```
