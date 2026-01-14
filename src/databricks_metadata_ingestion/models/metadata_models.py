"""Pydantic data models for metadata representation."""

from typing import List, Optional, Dict, Any
from datetime import datetime

from pydantic import BaseModel, Field


class TableMetadata(BaseModel):
    """Model for table metadata."""

    name: str = Field(..., description="Table name")
    schema: str = Field(..., description="Schema/Database name")
    catalog: Optional[str] = Field(None, description="Catalog name")
    owner: Optional[str] = Field(None, description="Table owner")
    description: Optional[str] = Field(None, description="Table description")
    location: Optional[str] = Field(None, description="Storage location")
    table_type: Optional[str] = Field(None, description="Type of table (MANAGED/EXTERNAL)")
    created_at: Optional[datetime] = Field(None, description="Creation timestamp")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")
    properties: Dict[str, Any] = Field(default_factory=dict, description="Custom properties")

    class Config:
        """Pydantic config."""
        json_schema_extra = {
            "example": {
                "name": "customers",
                "schema": "sales",
                "owner": "data_team",
                "table_type": "MANAGED",
            }
        }


class ClusterMetadata(BaseModel):
    """Model for cluster metadata."""

    cluster_id: str = Field(..., description="Cluster ID")
    cluster_name: str = Field(..., description="Cluster name")
    spark_version: str = Field(..., description="Spark version")
    driver_node_type: str = Field(..., description="Driver node type")
    worker_node_type: str = Field(..., description="Worker node type")
    num_workers: int = Field(..., description="Number of workers")
    state: str = Field(..., description="Cluster state")
    created_at: Optional[datetime] = Field(None, description="Creation timestamp")
    properties: Dict[str, Any] = Field(default_factory=dict, description="Custom properties")

    class Config:
        """Pydantic config."""
        json_schema_extra = {
            "example": {
                "cluster_id": "0123-456789-abcde",
                "cluster_name": "production-cluster",
                "spark_version": "11.3.x-scala2.12",
                "state": "RUNNING",
            }
        }


class JobMetadata(BaseModel):
    """Model for job metadata."""

    job_id: int = Field(..., description="Job ID")
    job_name: str = Field(..., description="Job name")
    job_type: str = Field(..., description="Job type (notebook, jar, etc.)")
    creator_user_name: Optional[str] = Field(None, description="Creator user name")
    schedule: Optional[Dict[str, Any]] = Field(None, description="Job schedule")
    created_time: Optional[datetime] = Field(None, description="Creation timestamp")
    max_concurrent_runs: int = Field(default=1, description="Max concurrent runs")
    properties: Dict[str, Any] = Field(default_factory=dict, description="Custom properties")

    class Config:
        """Pydantic config."""
        json_schema_extra = {
            "example": {
                "job_id": 123,
                "job_name": "daily_etl",
                "job_type": "notebook",
                "max_concurrent_runs": 1,
            }
        }


class WorkspaceMetadata(BaseModel):
    """Model for workspace metadata aggregation."""

    workspace_id: str = Field(..., description="Workspace ID")
    workspace_name: Optional[str] = Field(None, description="Workspace name")
    ingestion_timestamp: datetime = Field(
        default_factory=datetime.utcnow,
        description="Metadata ingestion timestamp"
    )
    tables: List[TableMetadata] = Field(default_factory=list, description="Table metadata")
    clusters: List[ClusterMetadata] = Field(default_factory=list, description="Cluster metadata")
    jobs: List[JobMetadata] = Field(default_factory=list, description="Job metadata")
    metadata_version: str = Field(default="1.0", description="Metadata schema version")

    class Config:
        """Pydantic config."""
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
