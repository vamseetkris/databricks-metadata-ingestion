"""Basic example of using the metadata ingestion framework."""

from databricks_metadata_ingestion import Config, DatabricksConnector, MetadataPipeline
from databricks_metadata_ingestion.utils import setup_logging, export_metadata


def main():
    """Run basic metadata ingestion example."""
    setup_logging(log_level="INFO")

    config = Config()

    if not config.validate_connection():
        print("Configuration is invalid. Please check DATABRICKS_HOST and DATABRICKS_TOKEN.")
        return

    connector = DatabricksConnector(
        host=config.databricks_host,
        token=config.databricks_token,
    )

    pipeline = MetadataPipeline(connector)
    metadata = pipeline.ingest_all(workspace_id="my-workspace")

    if metadata:
        export_metadata(
            metadata,
            output_path=f"{config.output_path}/metadata.json",
            format=config.metadata_output_format,
        )
        print(f"Metadata ingestion completed successfully!")
        print(f"Tables: {len(metadata.tables)}")
        print(f"Clusters: {len(metadata.clusters)}")
        print(f"Jobs: {len(metadata.jobs)}")
    else:
        print("Failed to ingest metadata")


if __name__ == "__main__":
    main()
