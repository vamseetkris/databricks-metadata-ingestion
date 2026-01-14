"""Configuration management for the metadata ingestion framework."""

import os
from typing import Optional
from pathlib import Path

import yaml
from pydantic import BaseSettings, Field
from loguru import logger


class Config(BaseSettings):
    """Configuration settings for the metadata ingestion framework."""

    # Databricks Configuration
    databricks_host: str = Field(
        default="",
        description="Databricks workspace host URL",
        env="DATABRICKS_HOST"
    )
    databricks_token: str = Field(
        default="",
        description="Databricks authentication token",
        env="DATABRICKS_TOKEN"
    )

    # Ingestion Configuration
    metadata_output_format: str = Field(
        default="json",
        description="Output format for metadata (json, yaml, parquet)"
    )
    output_path: str = Field(
        default="./metadata_output",
        description="Path where metadata output will be saved"
    )
    batch_size: int = Field(
        default=100,
        description="Batch size for metadata ingestion"
    )

    # Logging Configuration
    log_level: str = Field(
        default="INFO",
        description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)"
    )
    log_file: Optional[str] = Field(
        default=None,
        description="Path to log file (optional)"
    )

    # Feature Flags
    ingest_tables: bool = Field(default=True, description="Ingest table metadata")
    ingest_clusters: bool = Field(default=True, description="Ingest cluster metadata")
    ingest_jobs: bool = Field(default=True, description="Ingest job metadata")
    ingest_workflows: bool = Field(default=True, description="Ingest workflow metadata")

    class Config:
        """Pydantic configuration."""
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False

    @classmethod
    def from_file(cls, file_path: str) -> "Config":
        """Load configuration from a YAML or JSON file.

        Args:
            file_path: Path to the configuration file

        Returns:
            Config instance with loaded settings
        """
        file_path = Path(file_path)

        if not file_path.exists():
            logger.warning(f"Configuration file not found: {file_path}")
            return cls()

        try:
            if file_path.suffix in [".yaml", ".yml"]:
                with open(file_path, "r") as f:
                    data = yaml.safe_load(f)
                return cls(**data)
            else:
                logger.error(f"Unsupported configuration file format: {file_path.suffix}")
                return cls()
        except Exception as e:
            logger.error(f"Error loading configuration from {file_path}: {e}")
            return cls()

    def validate_connection(self) -> bool:
        """Validate Databricks connection configuration.

        Returns:
            True if configuration is valid, False otherwise
        """
        if not self.databricks_host:
            logger.error("Databricks host is not configured")
            return False
        if not self.databricks_token:
            logger.error("Databricks token is not configured")
            return False
        return True
