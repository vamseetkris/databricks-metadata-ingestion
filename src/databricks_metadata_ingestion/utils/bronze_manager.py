"""Utilities for managing Bronze tables and data quality."""

from typing import Any, List, Dict, Optional
from datetime import datetime

from loguru import logger


class BronzeTableManager:
    """Manager for Bronze table operations and metadata tracking."""

    def __init__(self, spark_session, bronze_prefix: str = "bronze_"):
        """Initialize Bronze table manager."""
        self.spark = spark_session
        self.bronze_prefix = bronze_prefix

    def get_bronze_table_name(self, source_table: str) -> str:
        """Get the Bronze table name for a source table."""
        if source_table.startswith(self.bronze_prefix):
            return source_table
        return f"{self.bronze_prefix}{source_table}"

    def add_ingestion_metadata(self, df: Any, source_name: str, job_id: str) -> Any:
        """Add ingestion metadata columns to DataFrame."""
        try:
            from pyspark.sql.functions import lit, current_timestamp

            logger.info(f"Adding ingestion metadata to DataFrame from job {job_id}")
            enriched_df = df.withColumn(
                "_bronze_ingestion_timestamp", current_timestamp()
            ).withColumn(
                "_bronze_source_name", lit(source_name)
            ).withColumn(
                "_bronze_job_id", lit(job_id)
            ).withColumn(
                "_bronze_ingestion_date", current_timestamp().cast("date")
            )
            return enriched_df

        except Exception as e:
            logger.error(f"Error adding ingestion metadata: {e}")
            return df

    def validate_data_quality(self, df: Any, null_checks: Optional[Dict] = None,
                             row_count_threshold: Optional[int] = None) -> Dict[str, Any]:
        """Validate data quality of DataFrame."""
        try:
            logger.info("Starting data quality validation")
            validation_results = {
                "is_valid": True,
                "checks_passed": 0,
                "checks_failed": 0,
                "warnings": [],
                "errors": [],
                "metrics": {"row_count": df.count(), "column_count": len(df.columns)},
            }

            if row_count_threshold is not None:
                if validation_results["metrics"]["row_count"] < row_count_threshold:
                    validation_results["checks_failed"] += 1
                    validation_results["is_valid"] = False
                    validation_results["errors"].append(
                        f"Row count below threshold {row_count_threshold}"
                    )
                else:
                    validation_results["checks_passed"] += 1

            logger.info(f"Data quality validation completed")
            return validation_results

        except Exception as e:
            logger.error(f"Error validating data quality: {e}")
            return {"is_valid": False, "checks_passed": 0, "checks_failed": 1,
                   "errors": [str(e)]}

    def get_bronze_table_stats(self, catalog: str, schema: str, table: str) -> Dict[str, Any]:
        """Get statistics for a Bronze table."""
        try:
            full_table_name = f"{catalog}.{schema}.{table}"
            logger.info(f"Retrieving statistics for table: {full_table_name}")

            df = self.spark.read.table(full_table_name)
            stats = {
                "table_name": full_table_name,
                "row_count": df.count(),
                "column_count": len(df.columns),
                "columns": [col for col in df.columns],
                "retrieval_timestamp": datetime.utcnow().isoformat(),
            }
            return stats

        except Exception as e:
            logger.error(f"Error getting table statistics: {e}")
            return {"error": str(e)}
