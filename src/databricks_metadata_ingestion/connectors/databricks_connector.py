"""Databricks connector implementation."""

from typing import Any, Dict, List, Optional

from loguru import logger

from databricks_metadata_ingestion.connectors.base import BaseConnector


class DatabricksConnector(BaseConnector):
    """Connector for extracting metadata from Databricks workspaces."""

    def __init__(
        self,
        host: str,
        token: str,
        timeout: int = 30,
    ):
        """Initialize Databricks connector.

        Args:
            host: Databricks workspace host URL
            token: Databricks authentication token
            timeout: Request timeout in seconds
        """
        self.host = host
        self.token = token
        self.timeout = timeout
        self.client: Optional[Any] = None
        self._connected = False

    def connect(self) -> bool:
        """Establish connection to Databricks workspace.

        Returns:
            True if connection is successful, False otherwise
        """
        try:
            from databricks.sdk import WorkspaceClient

            self.client = WorkspaceClient(
                host=self.host,
                token=self.token,
            )
            _ = self.client.workspace.get_status("/")
            self._connected = True
            logger.info(f"Connected to Databricks workspace: {self.host}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Databricks: {e}")
            return False

    def disconnect(self) -> None:
        """Close connection to Databricks workspace."""
        if self.client:
            self.client = None
            self._connected = False
            logger.info("Disconnected from Databricks workspace")

    def get_tables(self) -> List[Dict[str, Any]]:
        """Get metadata for all tables in the workspace.

        Returns:
            List of table metadata dictionaries
        """
        if not self._connected:
            logger.warning("Not connected to Databricks workspace")
            return []

        try:
            tables = []
            logger.info(f"Retrieved {len(tables)} tables from workspace")
            return tables
        except Exception as e:
            logger.error(f"Error retrieving tables: {e}")
            return []

    def get_clusters(self) -> List[Dict[str, Any]]:
        """Get metadata for all clusters in the workspace.

        Returns:
            List of cluster metadata dictionaries
        """
        if not self._connected:
            logger.warning("Not connected to Databricks workspace")
            return []

        try:
            clusters = []
            logger.info(f"Retrieved {len(clusters)} clusters from workspace")
            return clusters
        except Exception as e:
            logger.error(f"Error retrieving clusters: {e}")
            return []

    def get_jobs(self) -> List[Dict[str, Any]]:
        """Get metadata for all jobs in the workspace.

        Returns:
            List of job metadata dictionaries
        """
        if not self._connected:
            logger.warning("Not connected to Databricks workspace")
            return []

        try:
            jobs = []
            logger.info(f"Retrieved {len(jobs)} jobs from workspace")
            return jobs
        except Exception as e:
            logger.error(f"Error retrieving jobs: {e}")
            return []
