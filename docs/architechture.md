flowchart TB
    A["Citi Bike Trip History"] --> B["Python Downloader"]
    B --> C["MinIO Raw ZIP/CSV"]
    C --> D["PySpark Batch"]

    E["GBFS API"] --> F["Python Poller"]
    F --> R["MinIO Raw JSON"]
    F --> V["Pydantic Validation"]
    V --> K["Kafka"]

    K --> S["Spark Structured Streaming"]
    K --> L["Latest State Consumer"]

    D --> I["Iceberg Bronze / Silver"]
    S --> I

    I --> G["dbt Gold Models"]
    G --> T["Trino"]

    L --> P["PostgreSQL Latest State + Alerts"]

    T --> Z["Grafana Dashboard"]
    P --> Z

    I -.->|"Rebuild / Reconcile"| P
