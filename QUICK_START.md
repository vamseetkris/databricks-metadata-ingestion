# Quick Start Guide - Metadata-Driven Ingestion

## 5-Minute Setup

### 1. Create Tables (5 min)

```bash
python examples/setup_metadata_tables.py
```

### 2. Define a Job (1 min)

```sql
INSERT INTO main.ingestion_metadata.ingestion_jobs
(job_id, job_name, source_type, source_path, source_format,
 target_catalog, target_schema, target_table, is_active)
VALUES
('ingest_demo', 'Demo Data', 'csv', '/mnt/demo/', 'csv',
 'main', 'bronze', 'demo', true);
```

### 3. Run Ingestion (1 min)

```python
from pyspark.sql import SparkSession
from databricks_metadata_ingestion import MetadataDrivenPipeline, Config

spark = SparkSession.builder.getOrCreate()
config = Config()

pipeline = MetadataDrivenPipeline(spark)
results = pipeline.execute_all_jobs(
    config.metadata_catalog,
    config.metadata_schema,
    config.metadata_table,
)

for r in results:
    print(f"{r.job_name}: {r.execution_status} - {r.records_written} rows")
```

## Common Tasks

### Ingest CSV Files

```sql
INSERT INTO main.ingestion_metadata.ingestion_jobs
VALUES (
    'ingest_csv_sales', 'Sales Data (CSV)', 'path', '/mnt/sales/csv/', 'csv',
    'main', 'bronze', 'sales', 'bronze_', 'overwrite',
    ARRAY(), ARRAY(), TRUE, NULL, 'team', NULL,
    current_timestamp(), current_timestamp(), MAP()
);
```

### Ingest from SQL Server

```sql
INSERT INTO main.ingestion_metadata.ingestion_jobs
VALUES (
    'ingest_sqlserver', 'Orders from SQL Server', 'jdbc',
    NULL, NULL,
    'jdbc:sqlserver://server:1433;database=db',
    'com.microsoft.sqlserver.jdbc.SQLServerDriver',
    '${SQL_USER}', '${SQL_PASS}', 'SELECT * FROM Orders',
    'main', 'bronze', 'orders_sqlserver', 'bronze_', 'overwrite',
    ARRAY(), ARRAY('id'), TRUE, NULL, 'team', NULL,
    current_timestamp(), current_timestamp(), MAP()
);
```

### Monitor Job Success

```sql
SELECT
    job_id,
    COUNT(*) as total_runs,
    SUM(IF(execution_status='succeeded', 1, 0)) as successful,
    ROUND(100.0 * SUM(IF(execution_status='succeeded', 1, 0)) / COUNT(*), 2) as success_rate
FROM main.ingestion_audit.ingestion_audit_log
WHERE execution_start >= DATE_SUB(current_date(), 7)
GROUP BY job_id
ORDER BY success_rate DESC;
```

---

**Next**: Read [METADATA_DRIVEN_INGESTION.md](METADATA_DRIVEN_INGESTION.md)
