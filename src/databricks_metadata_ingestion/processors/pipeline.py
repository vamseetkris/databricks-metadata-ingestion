"""Metadata ingestion pipeline implementation."""

from typing import Optional
from datetime import datetime

from loguru import logger

from databricks_metadata_ingestion.connectors.base import BaseConnector
from databricks_metadata_ingestion.models.metadata_models import WorkspaceMetadata


class MetadataPipeline:
    """Pipeline for ingesting and processing metadata."""

    def __init__(self, connector: BaseConnector):
        """Initialize metadata pipeline.

        Args:
            connector: BaseConnector instance for metadata extraction
        """
        self.connector = connector
        self.workspace_metadata: Optional[WorkspaceMetadata] = None

    def ingest_all(self, workspace_id: str = "default") -> WorkspaceMetadata:
        """Ingest all available metadata from the workspace.

        Args:
            workspace_id: ID of the workspace to ingest from

        Returns:
            WorkspaceMetadata object containing all ingested metadata
        """
        logger.info(f"Starting metadata ingestion for workspace: {workspace_id}")

        try:
            if not self.connector.connect():
                logger.error("Failed to connect to metadata source")
                return WorkspaceMetadata(workspace_id=workspace_id)

            workspace_metadata = WorkspaceMetadata(
                workspace_id=workspace_id,
                ingestion_timestamp=datetime.utcnow(),
            )

            logger.info("Ingesting table metadata...")
            table_list = self.connector.get_tables()

            logger.info("Ingesting cluster metadata...")
            cluster_list = self.connector.get_clusters()

            logger.info("Ingesting job metadata...")
            job_list = self.connector.get_jobs()

            self.workspace_metadata = workspace_metadata
            logger.info("Metadata ingestion completed successfully")
            return workspace_metadata

        except Exception as e:
            logger.error(f"Error during metadata ingestion: {e}")
            return WorkspaceMetadata(workspace_id=workspace_id)
        finally:
            self.connector.disconnect()

    def get_metadata(self) -> Optional[WorkspaceMetadata]:
        """Get the current workspace metadata.

        Returns:
            WorkspaceMetadata object or None if no metadata has been ingested
        """
        return self.workspace_metadata
