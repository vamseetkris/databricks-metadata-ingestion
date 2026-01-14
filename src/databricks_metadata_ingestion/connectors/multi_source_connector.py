"""Multi-source connector for path-based and JDBC data ingestion."""

import os
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from loguru import logger

from databricks_metadata_ingestion.connectors.base import BaseConnector


class MultiSourceConnector(BaseConnector):
    """Connector supporting multiple source types: CSV, Parquet, Delta, JSON, and JDBC."""

    def __init__(self, spark_session=None):
        """Initialize multi-source connector.

        Args:
            spark_session: Spark session for DataFrame operations
        """
        self.spark = spark_session
        self._connected = False
        self.jdbc_connections: Dict[str, Any] = {}

    def connect(self) -> bool:
        """Establish connection (validates Spark session).

        Returns:
            True if connection is successful, False otherwise
        """
        try:
            if self.spark is None:
                logger.warning("Spark session not provided. Attempting to get or create one.")
                try:
                    from pyspark.sql import SparkSession
                    self.spark = SparkSession.builder.appName("MetadataIngestion").getOrCreate()
                except Exception as e:
                    logger.error(f"Failed to create Spark session: {e}")
                    return False

            self._connected = True
            logger.info("Connected to Spark session")
            return True
        except Exception as e:
            logger.error(f"Failed to establish connection: {e}")
            return False

    def disconnect(self) -> None:
        """Close all connections."""
        self.jdbc_connections.clear()
        self._connected = False
        logger.info("Disconnected from all sources")

    def read_from_path(self, path: str, format: str, **options) -> Any:
        """Read data from a file path."""
        if not self._connected:
            logger.error("Not connected. Call connect() first.")
            return None

        try:
            logger.info(f"Reading {format.upper()} data from: {path}")

            if format.lower() == "csv":
                df = self.spark.read.csv(path, header=True, **options)
            elif format.lower() == "parquet":
                df = self.spark.read.parquet(path, **options)
            elif format.lower() == "delta":
                df = self.spark.read.format("delta").load(path, **options)
            elif format.lower() == "json":
                df = self.spark.read.json(path, **options)
            else:
                logger.error(f"Unsupported format: {format}")
                return None

            row_count = df.count()
            logger.info(f"Successfully read {row_count} rows from {path}")
            return df

        except Exception as e:
            logger.error(f"Error reading from path {path}: {e}")
            return None

    def read_from_jdbc(self, jdbc_url: str, table: str, user: str, password: str,
                      driver: str = "com.microsoft.sqlserver.jdbc.SQLServerDriver", **options) -> Any:
        """Read data from JDBC source."""
        if not self._connected:
            logger.error("Not connected. Call connect() first.")
            return None

        try:
            resolved_user = self._resolve_env_var(user)
            resolved_password = self._resolve_env_var(password)

            logger.info(f"Reading from JDBC source: {jdbc_url}")

            jdbc_options = {
                "url": jdbc_url,
                "dbtable": table,
                "user": resolved_user,
                "password": resolved_password,
                "driver": driver,
                **options,
            }

            df = self.spark.read.format("jdbc").options(**jdbc_options).load()
            row_count = df.count()
            logger.info(f"Successfully read {row_count} rows from JDBC table: {table}")
            return df

        except Exception as e:
            logger.error(f"Error reading from JDBC source {jdbc_url}: {e}")
            return None

    def write_to_table(self, df: Any, catalog: str, schema: str, table: str,
                      mode: str = "overwrite", **options) -> bool:
        """Write DataFrame to Unity Catalog table."""
        if not self._connected:
            logger.error("Not connected. Call connect() first.")
            return False

        try:
            table_name = f"{catalog}.{schema}.{table}"
            logger.info(f"Writing {df.count()} rows to table: {table_name} (mode: {mode})")

            df.write.format("delta").mode(mode).options(**options).saveAsTable(table_name)
            logger.info(f"Successfully wrote data to {table_name}")
            return True

        except Exception as e:
            logger.error(f"Error writing to table {table_name}: {e}")
            return False

    def deduplicate_data(self, df: Any, primary_keys: List[str]) -> Any:
        """Remove duplicate records based on primary keys."""
        try:
            if not primary_keys:
                logger.warning("No primary keys specified for deduplication")
                return df

            logger.info(f"Deduplicating data based on keys: {primary_keys}")
            deduplicated_df = df.dropDuplicates(subset=primary_keys)
            rows_removed = df.count() - deduplicated_df.count()
            logger.info(f"Removed {rows_removed} duplicate rows")
            return deduplicated_df

        except Exception as e:
            logger.error(f"Error deduplicating data: {e}")
            return df

    def partition_data(self, df: Any, partition_columns: List[str]) -> Any:
        """Repartition DataFrame by specified columns."""
        try:
            if not partition_columns:
                logger.warning("No partition columns specified")
                return df

            logger.info(f"Partitioning data by: {partition_columns}")
            return df.repartition(partition_columns)

        except Exception as e:
            logger.error(f"Error partitioning data: {e}")
            return df

    def get_tables(self) -> List[Dict[str, Any]]:
        """Get list of accessible tables."""
        logger.debug("get_tables not implemented for MultiSourceConnector")
        return []

    def get_clusters(self) -> List[Dict[str, Any]]:
        """Get cluster metadata."""
        logger.debug("get_clusters not implemented for MultiSourceConnector")
        return []

    def get_jobs(self) -> List[Dict[str, Any]]:
        """Get job metadata."""
        logger.debug("get_jobs not implemented for MultiSourceConnector")
        return []

    @staticmethod
    def _resolve_env_var(value: str) -> str:
        """Resolve environment variable reference."""
        import re

        def replace_env(match):
            env_var = match.group(1)
            return os.getenv(env_var, match.group(0))

        return re.sub(r"\$\{([^}]+)\}", replace_env, value)
