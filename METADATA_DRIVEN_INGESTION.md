# Metadata-Driven Ingestion - Complete Guide

## Overview

The Databricks Metadata Ingestion Framework now supports **metadata-driven ingestion**, allowing you to define ingestion jobs in a central metadata table and execute them automatically.

## Key Features

- **Configuration as Code**: Define jobs in metadata tables
- **Multi-Source Support**: CSV, Parquet, Delta, JSON, JDBC sources
- **Bronze Tables**: Auto-created with standardized naming and metadata
- **Audit Logging**: Complete execution trails
- **Monitoring**: Aggregated metrics and statistics
- **Data Quality**: Built-in validation

## Architecture

```
Metadata Table (ingestion_jobs)
         ↓
MetadataDrivenPipeline
         ↓
MultiSourceConnector
         ↓
Bronze Tables (Delta) + Audit Log + Metrics
```

## Setup

### 1. Create Tables

```bash
python examples/setup_metadata_tables.py
```

### 2. Define Job

```sql
INSERT INTO main.ingestion_metadata.ingestion_jobs VALUES (
    'job_id', 'job_name', 'path', '/mnt/path/', 'csv',
    NULL, NULL, NULL, NULL, NULL,
    'main', 'bronze', 'table_name', 'bronze_', 'overwrite',
    ARRAY(), ARRAY(), TRUE, NULL, 'owner', NULL,
    current_timestamp(), current_timestamp(), MAP()
);
```

### 3. Execute

```python
from databricks_metadata_ingestion import MetadataDrivenPipeline

pipeline = MetadataDrivenPipeline(spark)
results = pipeline.execute_all_jobs('main', 'ingestion_metadata', 'ingestion_jobs')
```

## Source Types

### CSV
```sql
source_type: 'path'
source_path: '/mnt/data/'
source_format: 'csv'
```

### Parquet
```sql
source_type: 'path'
source_path: '/mnt/data/'
source_format: 'parquet'
```

### Delta
```sql
source_type: 'path'
source_path: '/mnt/data/'
source_format: 'delta'
```

### JSON
```sql
source_type: 'path'
source_path: '/mnt/data/'
source_format: 'json'
```

### JDBC (SQL Server)
```sql
source_type: 'jdbc'
jdbc_url: 'jdbc:sqlserver://server:1433;database=db'
jdbc_driver: 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
jdbc_user: '${SQL_USER}'
jdbc_password: '${SQL_PASSWORD}'
jdbc_query: 'SELECT * FROM Table'
```

### JDBC (Oracle)
```sql
source_type: 'jdbc'
jdbc_url: 'jdbc:oracle:thin:@server:1521:db'
jdbc_driver: 'oracle.jdbc.driver.OracleDriver'
jdbc_user: '${ORACLE_USER}'
jdbc_password: '${ORACLE_PASSWORD}'
jdbc_query: 'SELECT * FROM TABLE'
```

## Configuration

Key settings in `config.yaml`:

```yaml
metadata_driven_enabled: true
metadata_catalog: main
metadata_schema: ingestion_metadata
metadata_table: ingestion_jobs

bronze_catalog: main
bronze_schema: bronze
bronze_prefix: "bronze_"

audit_enabled: true
audit_catalog: main
audit_schema: ingestion_audit
audit_table: ingestion_audit_log
```

## Best Practices

1. Use consistent naming conventions
2. Always specify primary keys for deduplication
3. Partition large tables for performance
4. Monitor success rates and set alerts
5. Keep audit logs for compliance

---

For more details, see documentation files in repository.
