"""Metadata-driven ingestion pipeline for processing job definitions."""

from typing import List, Optional, Dict, Any
from datetime import datetime
import traceback

from loguru import logger

from databricks_metadata_ingestion.models.metadata_models import (
    IngestionJobDefinition,
    AuditRecord,
)
from databricks_metadata_ingestion.connectors.multi_source_connector import MultiSourceConnector
from databricks_metadata_ingestion.utils.bronze_manager import BronzeTableManager
from databricks_metadata_ingestion.utils.audit_logger import AuditLogger, MonitoringManager


class MetadataDrivenPipeline:
    """Pipeline for executing metadata-defined ingestion jobs."""

    def __init__(self, spark_session, bronze_prefix: str = "bronze_"):
        """Initialize metadata-driven pipeline."""
        self.spark = spark_session
        self.connector = MultiSourceConnector(spark_session=spark_session)
        self.bronze_manager = BronzeTableManager(spark_session, bronze_prefix=bronze_prefix)
        self.audit_logger = AuditLogger(spark_session)
        self.monitoring_manager = MonitoringManager(spark_session)
        self.bronze_prefix = bronze_prefix
        self.execution_results: List[Dict[str, Any]] = []

    def load_job_definitions(self, catalog: str, schema: str, table: str,
                            where_clause: Optional[str] = None) -> List[IngestionJobDefinition]:
        """Load ingestion job definitions from metadata table."""
        try:
            full_table_name = f"{catalog}.{schema}.{table}"
            logger.info(f"Loading job definitions from {full_table_name}")

            query = f"SELECT * FROM {full_table_name} WHERE is_active = true"
            if where_clause:
                query += f" AND {where_clause}"

            df = self.spark.sql(query)
            records = df.collect()
            jobs = [IngestionJobDefinition(**dict(row.asDict())) for row in records]
            logger.info(f"Loaded {len(jobs)} active job definitions")
            return jobs

        except Exception as e:
            logger.error(f"Error loading job definitions: {e}")
            return []

    def execute_job(self, job_definition: IngestionJobDefinition,
                   run_by: Optional[str] = None, run_context: str = "manual",
                   run_id: Optional[str] = None) -> Optional[AuditRecord]:
        """Execute a single ingestion job."""
        logger.info(f"Starting execution of job {job_definition.job_id}")

        source_location = job_definition.source_path or job_definition.jdbc_url
        audit_record = self.audit_logger.create_audit_record(
            job_id=job_definition.job_id,
            job_name=job_definition.job_name,
            target_table=self.bronze_manager.get_bronze_table_name(job_definition.target_table),
            source_location=source_location,
        )

        try:
            if not self.connector.connect():
                raise Exception("Failed to connect to data sources")

            if job_definition.source_type.lower() == "path":
                df = self.connector.read_from_path(job_definition.source_path,
                                                   job_definition.source_format)
            elif job_definition.source_type.lower() == "jdbc":
                df = self.connector.read_from_jdbc(
                    jdbc_url=job_definition.jdbc_url,
                    table=job_definition.jdbc_query,
                    user=job_definition.jdbc_user,
                    password=job_definition.jdbc_password,
                    driver=job_definition.jdbc_driver,
                )
            else:
                raise ValueError(f"Unsupported source type: {job_definition.source_type}")

            if df is None:
                raise Exception(f"Failed to read data for job {job_definition.job_id}")

            records_read = df.count()

            # Apply transformations
            if job_definition.primary_keys:
                df = self.connector.deduplicate_data(df, job_definition.primary_keys)
            if job_definition.partition_columns:
                df = self.connector.partition_data(df, job_definition.partition_columns)

            # Add Bronze metadata
            df = self.bronze_manager.add_ingestion_metadata(df, source_location,
                                                            job_definition.job_id)

            # Write to Bronze table
            bronze_table_name = self.bronze_manager.get_bronze_table_name(
                job_definition.target_table)
            success = self.connector.write_to_table(
                df, catalog=job_definition.target_catalog,
                schema=job_definition.target_schema,
                table=bronze_table_name,
                mode=job_definition.mode,
                mergeSchema="true",
            )

            if not success:
                raise Exception(f"Failed to write data to {bronze_table_name}")

            records_written = df.count()

            # Update audit record
            audit_record = self.audit_logger.update_audit_record(
                audit_record,
                execution_status="succeeded",
                records_read=records_read,
                records_written=records_written,
                run_by=run_by,
                run_context=run_context,
                run_id=run_id,
            )

            logger.info(f"Successfully completed job {job_definition.job_id}")
            return audit_record

        except Exception as e:
            error_message = str(e)
            error_trace = traceback.format_exc()
            logger.error(f"Error executing job {job_definition.job_id}: {error_message}")

            audit_record = self.audit_logger.update_audit_record(
                audit_record,
                execution_status="failed",
                error_message=error_message,
                error_stack_trace=error_trace,
                run_by=run_by,
                run_context=run_context,
                run_id=run_id,
            )
            return audit_record

        finally:
            self.connector.disconnect()

    def execute_all_jobs(self, metadata_catalog: str, metadata_schema: str,
                        metadata_table: str, run_by: Optional[str] = None,
                        run_context: str = "manual",
                        audit_catalog: str = "main",
                        audit_schema: str = "ingestion_audit") -> List[AuditRecord]:
        """Execute all active ingestion jobs."""
        logger.info("Starting batch execution of all active ingestion jobs")

        jobs = self.load_job_definitions(metadata_catalog, metadata_schema, metadata_table)

        if not jobs:
            logger.warning("No active jobs found to execute")
            return []

        results = []
        for job in jobs:
            audit_record = self.execute_job(job, run_by=run_by, run_context=run_context)
            if audit_record:
                results.append(audit_record)

        logger.info(f"Writing {len(results)} audit records")
        self.audit_logger.write_audit_records(audit_catalog, audit_schema,
                                             table="ingestion_audit_log", mode="append")

        logger.info(f"Batch execution completed: {len(results)} jobs processed")
        return results
