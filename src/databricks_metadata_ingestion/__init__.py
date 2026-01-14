"""Databricks Metadata Ingestion Framework.

A comprehensive framework for ingesting and managing metadata from Databricks workspaces.
"""

__version__ = "0.1.0"
__author__ = "Vamsee Krishna Tadikonda"
__email__ = "vamsee@example.com"
__license__ = "MIT"

from databricks_metadata_ingestion.config import Config
from databricks_metadata_ingestion.connectors import DatabricksConnector
from databricks_metadata_ingestion.processors import MetadataPipeline

__all__ = [
    "Config",
    "DatabricksConnector",
    "MetadataPipeline",
]
