# Contributing to Databricks Metadata Ingestion Framework

We appreciate your interest in contributing! This document provides guidelines and instructions for contributing to this project.

## Code of Conduct

Please be respectful and constructive in all interactions.

## How to Contribute

### Reporting Bugs

Before creating bug reports, please check the issue list as you might find out that you don't need to create one. When you are creating a bug report, please include as many details as possible:

- **Use a clear and descriptive title**
- **Describe the exact steps which reproduce the problem**
- **Provide specific examples to demonstrate the steps**
- **Describe the behavior you observed after following the steps**
- **Explain which behavior you expected to see instead and why**
- **Include screenshots if possible**

### Suggesting Enhancements

Enhancement suggestions are tracked as GitHub issues. When creating an enhancement suggestion, please include:

- **Use a clear and descriptive title**
- **Provide a step-by-step description of the suggested enhancement**
- **Provide specific examples to demonstrate the steps**
- **Describe the current behavior and the expected behavior**
- **Explain why this enhancement would be useful**

### Pull Requests

- Fill in the required template
- Follow the Python styleguides
- Include appropriate test cases
- Update documentation as needed
- End all files with a newline

## Development Setup

1. Fork the repository
2. Clone your fork: `git clone https://github.com/your-username/databricks-metadata-ingestion.git`
3. Create a virtual environment: `python -m venv venv`
4. Activate the virtual environment:
   - On Windows: `venv\Scripts\activate`
   - On macOS/Linux: `source venv/bin/activate`
5. Install development dependencies: `pip install -e ".[dev]"`
6. Create a new branch: `git checkout -b feature/your-feature-name`

## Development Workflow

1. Make your changes
2. Run tests: `make test`
3. Format code: `make format`
4. Check linting: `make lint`
5. Run type checking: `make type-check`
6. Commit with clear messages
7. Push to your fork
8. Create a pull request

## Styleguides

### Python Styleguide

- Follow [PEP 8](https://www.python.org/dev/peps/pep-0008/)
- Use type hints for function parameters and return types
- Use descriptive variable and function names
- Write docstrings for all public functions and classes
- Max line length: 100 characters

### Git Commit Messages

- Use the present tense ("Add feature" not "Added feature")
- Use the imperative mood ("Move cursor to..." not "Moves cursor to...")
- Limit the first line to 72 characters or less
- Reference issues and pull requests liberally after the first line

### Documentation Styleguide

- Use Markdown
- Include code examples where appropriate
- Keep documentation up-to-date with code changes

## License

By contributing to this project, you agree that your contributions will be licensed under the same MIT License that covers the project.
