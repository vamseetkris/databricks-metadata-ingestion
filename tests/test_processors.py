"""Tests for metadata processors."""

import pytest
from unittest.mock import Mock, MagicMock

from databricks_metadata_ingestion.processors import MetadataPipeline
from databricks_metadata_ingestion.connectors import BaseConnector


class TestMetadataPipeline:
    """Test cases for MetadataPipeline."""

    def test_initialization(self):
        """Test pipeline initialization."""
        mock_connector = Mock(spec=BaseConnector)
        pipeline = MetadataPipeline(mock_connector)
        assert pipeline.connector == mock_connector
        assert pipeline.workspace_metadata is None

    def test_get_metadata_before_ingestion(self):
        """Test getting metadata before ingestion."""
        mock_connector = Mock(spec=BaseConnector)
        pipeline = MetadataPipeline(mock_connector)
        metadata = pipeline.get_metadata()
        assert metadata is None

    def test_ingest_all_connection_failure(self):
        """Test ingest_all when connection fails."""
        mock_connector = Mock(spec=BaseConnector)
        mock_connector.connect.return_value = False
        pipeline = MetadataPipeline(mock_connector)
        
        metadata = pipeline.ingest_all(workspace_id="test-workspace")
        
        assert metadata.workspace_id == "test-workspace"
        assert len(metadata.tables) == 0
        assert len(metadata.clusters) == 0
        assert len(metadata.jobs) == 0
