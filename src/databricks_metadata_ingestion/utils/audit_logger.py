"""Audit and monitoring utilities for ingestion jobs."""

from typing import Any, Dict, Optional, List
from datetime import datetime
import uuid

from loguru import logger

from databricks_metadata_ingestion.models.metadata_models import AuditRecord, MonitoringMetrics


class AuditLogger:
    """Logger for audit and monitoring records."""

    def __init__(self, spark_session):
        """Initialize audit logger."""
        self.spark = spark_session
        self.audit_records: List[AuditRecord] = []

    def create_audit_record(self, job_id: str, job_name: str, target_table: str,
                           source_location: Optional[str] = None) -> AuditRecord:
        """Create a new audit record."""
        audit_id = f"audit_{uuid.uuid4().hex[:12]}"
        logger.info(f"Creating audit record {audit_id} for job {job_id}")

        record = AuditRecord(
            audit_id=audit_id,
            job_id=job_id,
            job_name=job_name,
            target_table=target_table,
            execution_start=datetime.utcnow(),
            execution_status="running",
            source_location=source_location,
        )
        self.audit_records.append(record)
        return record

    def update_audit_record(self, audit_record: AuditRecord, execution_status: str,
                           records_read: int = 0, records_written: int = 0,
                           records_failed: int = 0, error_message: Optional[str] = None,
                           error_stack_trace: Optional[str] = None,
                           target_location: Optional[str] = None,
                           data_size_bytes: Optional[int] = None,
                           run_by: Optional[str] = None, run_context: str = "manual",
                           run_id: Optional[str] = None) -> AuditRecord:
        """Update audit record with execution results."""
        execution_end = datetime.utcnow()
        duration = (execution_end - audit_record.execution_start).total_seconds()

        audit_record.execution_end = execution_end
        audit_record.execution_status = execution_status
        audit_record.execution_duration_seconds = duration
        audit_record.records_read = records_read
        audit_record.records_written = records_written
        audit_record.records_failed = records_failed
        audit_record.error_message = error_message
        audit_record.error_stack_trace = error_stack_trace
        audit_record.target_location = target_location
        audit_record.data_size_bytes = data_size_bytes
        audit_record.run_by = run_by
        audit_record.run_context = run_context
        audit_record.run_id = run_id

        logger.info(f"Updated audit record {audit_record.audit_id}")
        return audit_record

    def write_audit_records(self, catalog: str, schema: str, table: str = "ingestion_audit_log",
                           mode: str = "append") -> bool:
        """Write audit records to table."""
        try:
            if not self.audit_records:
                logger.warning("No audit records to write")
                return False

            full_table_name = f"{catalog}.{schema}.{table}"
            logger.info(f"Writing {len(self.audit_records)} audit records to {full_table_name}")

            audit_dicts = [record.dict() for record in self.audit_records]
            df = self.spark.createDataFrame(audit_dicts)
            df.write.format("delta").mode(mode).option("mergeSchema", "true"
                                                      ).saveAsTable(full_table_name)

            logger.info(f"Successfully wrote audit records to {full_table_name}")
            return True

        except Exception as e:
            logger.error(f"Error writing audit records: {e}")
            return False

    def get_job_audit_history(self, catalog: str, schema: str, table: str, job_id: str,
                             limit: int = 100) -> List[Dict[str, Any]]:
        """Retrieve audit history for a specific job."""
        try:
            full_table_name = f"{catalog}.{schema}.{table}"
            logger.info(f"Retrieving audit history for job {job_id}")

            query = f"""
                SELECT * FROM {full_table_name}
                WHERE job_id = '{job_id}'
                ORDER BY execution_start DESC
                LIMIT {limit}
            """
            df = self.spark.sql(query)
            records = df.collect()
            result = [dict(row) for row in records]
            logger.info(f"Retrieved {len(result)} audit records for job {job_id}")
            return result

        except Exception as e:
            logger.error(f"Error retrieving audit history: {e}")
            return []


class MonitoringManager:
    """Manager for collecting and reporting monitoring metrics."""

    def __init__(self, spark_session):
        """Initialize monitoring manager."""
        self.spark = spark_session

    def calculate_metrics(self, catalog: str, schema: str, audit_table: str, job_id: str,
                         days: int = 7) -> Optional[MonitoringMetrics]:
        """Calculate aggregated metrics for a job."""
        try:
            full_table_name = f"{catalog}.{schema}.{audit_table}"
            logger.info(f"Calculating metrics for job {job_id} over last {days} days")

            query = f"""
                SELECT
                    COUNT(*) as total_runs,
                    SUM(CASE WHEN execution_status = 'succeeded' THEN 1 ELSE 0 END) as successful_runs,
                    SUM(CASE WHEN execution_status = 'failed' THEN 1 ELSE 0 END) as failed_runs,
                    SUM(CASE WHEN execution_status = 'skipped' THEN 1 ELSE 0 END) as skipped_runs,
                    SUM(records_read) as total_records_processed
                FROM {full_table_name}
                WHERE job_id = '{job_id}'
                AND execution_start >= date_sub(current_timestamp(), {days})
            """

            result = self.spark.sql(query).collect()[0]
            total_runs = result.get("total_runs", 0) or 0
            success_rate = ((total_runs - (result.get("failed_runs", 0) or 0)) / total_runs * 100
                           ) if total_runs > 0 else 0

            metrics = MonitoringMetrics(
                job_id=job_id,
                total_runs=total_runs,
                successful_runs=result.get("successful_runs", 0) or 0,
                failed_runs=result.get("failed_runs", 0) or 0,
                skipped_runs=result.get("skipped_runs", 0) or 0,
                total_records_processed=result.get("total_records_processed", 0) or 0,
                success_rate=success_rate,
            )
            logger.info(f"Calculated metrics for job {job_id}")
            return metrics

        except Exception as e:
            logger.error(f"Error calculating metrics: {e}")
            return None

    def write_metrics(self, metrics: MonitoringMetrics, catalog: str, schema: str,
                     table: str = "ingestion_metrics", mode: str = "append") -> bool:
        """Write monitoring metrics to table."""
        try:
            full_table_name = f"{catalog}.{schema}.{table}"
            logger.info(f"Writing metrics to {full_table_name}")

            df = self.spark.createDataFrame([metrics.dict()])
            df.write.format("delta").mode(mode).option("mergeSchema", "true"
                                                      ).saveAsTable(full_table_name)
            logger.info(f"Successfully wrote metrics to {full_table_name}")
            return True

        except Exception as e:
            logger.error(f"Error writing metrics: {e}")
            return False
