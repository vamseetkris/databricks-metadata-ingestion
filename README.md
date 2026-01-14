# Databricks Metadata Ingestion Framework

A comprehensive metadata ingestion framework for Databricks workspaces. This framework enables efficient extraction, transformation, and management of metadata from Databricks environments.

## Features

- 🔍 Extract metadata from Databricks workspaces
- 📊 Support for multiple metadata sources (tables, clusters, jobs, workflows, etc.)
- 🔄 Flexible ingestion strategies and pipelines
- 🎯 Metadata transformation and enrichment
- 🔌 Easy integration with data catalogs and metadata stores
- 📝 Type-safe data models using Pydantic
- ✅ Comprehensive testing and quality assurance

## Installation

### Prerequisites

- Python 3.9 or higher
- Databricks workspace with appropriate permissions

### Install from source

```bash
git clone https://github.com/vamseetkris/databricks-metadata-ingestion.git
cd databricks-metadata-ingestion
pip install -e .
```

### Install with development dependencies

```bash
pip install -e ".[dev]"
```

## Quick Start

```python
from databricks_metadata_ingestion.connectors import DatabricksConnector
from databricks_metadata_ingestion.processors import MetadataPipeline

# Initialize the Databricks connector
connector = DatabricksConnector(
    host="https://your-databricks-instance.com",
    token="your-token"
)

# Create and run a metadata pipeline
pipeline = MetadataPipeline(connector)
metadata = pipeline.ingest_all()

# Process the metadata
for table in metadata.tables:
    print(f"Table: {table.name}, Schema: {table.schema}")
```

## Project Structure

```
databricks-metadata-ingestion/
├── src/
│   └── databricks_metadata_ingestion/
│       ├── connectors/          # Metadata source connectors
│       ├── models/              # Pydantic data models
│       ├── processors/          # Metadata processing pipelines
│       ├── utils/               # Utility functions
│       └── config.py            # Configuration management
├── tests/                       # Unit and integration tests
├── examples/                    # Usage examples
├── docs/                        # Documentation
└── README.md
```

## Development

### Run Tests

```bash
make test
# or with coverage
make test-cov
```

### Code Formatting

```bash
make format
```

### Linting

```bash
make lint
```

### Type Checking

```bash
make type-check
```

### Run All Checks

```bash
make all
```

## Configuration

Configuration can be provided through:

1. Environment variables
2. `.env` file
3. Configuration file (YAML/JSON)
4. Python configuration object

Example `.env` file:

```
DATABRICKS_HOST=https://your-databricks-instance.com
DATABRICKS_TOKEN=your-token
METADATA_OUTPUT_FORMAT=json
```

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For issues and questions, please open an issue on [GitHub Issues](https://github.com/vamseetkris/databricks-metadata-ingestion/issues).
