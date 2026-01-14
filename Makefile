.PHONY: help install install-dev test format lint clean

help:
	@echo "Databricks Metadata Ingestion Framework - Available commands:"
	@echo ""
	@echo "install          Install the package"
	@echo "install-dev      Install the package with development dependencies"
	@echo "test             Run tests"
	@echo "test-cov         Run tests with coverage report"
	@echo "format           Format code with black and isort"
	@echo "lint             Run flake8 linting"
	@echo "type-check       Run mypy type checking"
	@echo "clean            Clean up build artifacts and cache files"
	@echo "all              Run format, lint, type-check, and test"

install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"

test:
	pytest

test-cov:
	pytest --cov=src/databricks_metadata_ingestion --cov-report=html --cov-report=term-missing

format:
	black src/ tests/
	isort src/ tests/

lint:
	flake8 src/ tests/

type-check:
	mypy src/

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf .coverage
	rm -rf htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} +

all: format lint type-check test
