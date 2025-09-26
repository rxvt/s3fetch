# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

This project uses Hatch for dependency management and task running:

- **Install dependencies**: `hatch env create` (creates default environment with dev/test dependencies)
- **Run tests**: `hatch run pytest` (runs all tests) or `hatch run test_unit`, `hatch run test_integration`, `hatch run test_e2e`
- **Type checking**: `hatch run check_types` (runs mypy on src/s3fetch and tests)
- **Linting/formatting**: `ruff check` and `ruff format` (configured in pyproject.toml)
- **Run the CLI**: `hatch run s3fetch <args>` or install with `pipx install -e .`

Test commands include specialized targets:
- `hatch run test_regex` - tests regex functionality against live S3 bucket
- `hatch run test_dryrun` - tests dry-run mode against live S3 bucket

## Project Architecture

s3fetch is a multi-threaded S3 download tool with these core components:

### Key Modules (src/s3fetch/)
- **cli.py**: Click-based command line interface, entry point for the application
- **api.py**: Public API functions for programmatic usage
- **s3.py**: Core S3 operations, threading, and download queue management (S3FetchQueue)
- **aws.py**: AWS credential handling and S3 client creation
- **fs.py**: File system operations for creating directories and handling downloads
- **utils.py**: Utility functions including custom print and logging
- **exceptions.py**: Custom exception classes

### Architecture Patterns
- **Multi-threaded design**: Separate threads for listing objects vs downloading them
- **Queue-based**: Uses S3FetchQueue for coordinating between listing and download threads
- **Producer-Consumer**: Object listing thread produces work, download threads consume
- **Early streaming**: Downloads start as soon as first object is found, while listing continues in background

### Key Features
- Concurrent downloads with configurable thread count
- Regex filtering of objects without full bucket listing
- Dry-run mode for testing
- Custom output formatting and progress indication
- Standard boto3 AWS credentials support

## Important Notes

- Minimum Python version: 3.10 (specified in pyproject.toml)
- Uses hatch-pip-compile for dependency management with locked requirements
- Pre-commit hooks are automatically installed in dev environment
- Tests include unit, integration, and e2e categories
- Ruff configuration includes strict linting rules (Google docstring convention)
- Always prompt for confirmation before making changes (per Copilot instructions)
