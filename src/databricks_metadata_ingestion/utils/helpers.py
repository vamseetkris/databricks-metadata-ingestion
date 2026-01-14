"""Helper functions for the metadata ingestion framework."""

import json
from pathlib import Path
from typing import Any

from loguru import logger
import yaml

from databricks_metadata_ingestion.models.metadata_models import WorkspaceMetadata


def setup_logging(log_level: str = "INFO", log_file: str = None) -> None:
    """Configure logging for the application.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional path to log file
    """
    logger.remove()
    logger.add(
        lambda msg: print(msg, end=""),
        level=log_level,
        format="<level>{time:YYYY-MM-DD HH:mm:ss}</level> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    )

    if log_file:
        logger.add(
            log_file,
            level=log_level,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            rotation="500 MB",
            retention="7 days",
        )
        logger.info(f"Log file configured: {log_file}")


def export_metadata(
    metadata: WorkspaceMetadata,
    output_path: str,
    format: str = "json",
) -> bool:
    """Export workspace metadata to a file.

    Args:
        metadata: WorkspaceMetadata object to export
        output_path: Path where to save the metadata
        format: Output format (json or yaml)

    Returns:
        True if export was successful, False otherwise
    """
    try:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if format.lower() == "json":
            with open(output_path, "w") as f:
                json.dump(metadata.dict(), f, indent=2, default=str)
        elif format.lower() == "yaml":
            with open(output_path, "w") as f:
                yaml.dump(metadata.dict(), f, default_flow_style=False, allow_unicode=True)
        else:
            logger.error(f"Unsupported export format: {format}")
            return False

        logger.info(f"Metadata exported successfully to {output_path}")
        return True

    except Exception as e:
        logger.error(f"Error exporting metadata: {e}")
        return False
