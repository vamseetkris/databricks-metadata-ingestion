"""Tests for connector implementations."""

import pytest

from databricks_metadata_ingestion.connectors import DatabricksConnector


class TestDatabricksConnector:
    """Test cases for DatabricksConnector."""

    def test_initialization(self):
        """Test connector initialization."""
        connector = DatabricksConnector(
            host="https://example.databricks.com",
            token="test-token",
        )
        assert connector.host == "https://example.databricks.com"
        assert connector.token == "test-token"
        assert connector._connected is False

    def test_get_tables_not_connected(self):
        """Test getting tables when not connected."""
        connector = DatabricksConnector(
            host="https://example.databricks.com",
            token="test-token",
        )
        tables = connector.get_tables()
        assert tables == []

    def test_get_clusters_not_connected(self):
        """Test getting clusters when not connected."""
        connector = DatabricksConnector(
            host="https://example.databricks.com",
            token="test-token",
        )
        clusters = connector.get_clusters()
        assert clusters == []

    def test_get_jobs_not_connected(self):
        """Test getting jobs when not connected."""
        connector = DatabricksConnector(
            host="https://example.databricks.com",
            token="test-token",
        )
        jobs = connector.get_jobs()
        assert jobs == []
