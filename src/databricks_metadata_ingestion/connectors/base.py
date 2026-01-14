"""Base connector interface for metadata sources."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List


class BaseConnector(ABC):
    """Abstract base class for metadata connectors."""

    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to the metadata source.

        Returns:
            True if connection is successful, False otherwise
        """
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close connection to the metadata source."""
        pass

    @abstractmethod
    def get_tables(self) -> List[Dict[str, Any]]:
        """Get metadata for all tables.

        Returns:
            List of table metadata dictionaries
        """
        pass

    @abstractmethod
    def get_clusters(self) -> List[Dict[str, Any]]:
        """Get metadata for all clusters.

        Returns:
            List of cluster metadata dictionaries
        """
        pass

    @abstractmethod
    def get_jobs(self) -> List[Dict[str, Any]]:
        """Get metadata for all jobs.

        Returns:
            List of job metadata dictionaries
        """
        pass
