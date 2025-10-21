# Agent Instructions for s3fetch

## Development Commands

This project uses Hatch for dependency management and task running:

- **Setup environment**: `hatch env create` (creates default environment with dev/test dependencies)
- **Run CLI**: `hatch run s3fetch <args>`
- **Run all tests**: `just test -a` (runs all tests for all Python versions)
- **Run single test**: `just test <filename>::<test_function_name>`
- **Run unit tests**: `just test-unit`
- **Run integration tests**: `just test-integration`
- **Run e2e tests**: `just test-e2e`
- **Linting**: `just lint`
- **Formatting**: `just format`
- **All quality checks**: `just check`

## Project Architecture

s3fetch is a multi-threaded S3 download tool with these core components:

### Key Modules (src/s3fetch/)
- **cli.py**: Click-based command line interface, entry point
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
- **Early streaming**: Downloads start as soon as first object is found, while listing continues

## Code Style Guidelines

- **Python version**: 3.10+ minimum
- **Formatting**: ruff (88 char line length)
- **Linting**: ruff with Google docstring convention
- **Type hints**: Required, enforced by mypy
- **Imports**: stdlib → third-party → local, one per line
- **Naming**: snake_case functions/variables, PascalCase classes
- **Docstrings**: Google style for all public functions
- **Error handling**: Use custom exceptions from exceptions.py
- **Logging**: logging module with module-level loggers
- **Security**: Never log credentials, use boto3 credential chain

### Key Features

- Concurrent downloads with configurable thread count
- Regex filtering of objects without full bucket listing
- Dry-run mode for testing
- Custom output formatting and progress indication
- Standard boto3 AWS credentials support

## CICD

- CICD runs via GitHub Actions
- There is a test AWS S3 bucket called `s3://s3fetch-cicd-test-bucket` in the `us-east-1` region
- The AWS CloudFormation template is in the `infra` directory
- The AWS CloudFormation stack is called `s3fetch-cicd-test-bucket` and is provisioned in `us-east-1`
- Always specify `--capabilities CAPABILITY_NAMED_IAM)` when updating the CloudFormation template
- It should contain a mixture of small and big objects with various naming schemes for testing purposes

## Important Notes

- Uses hatch-pip-compile for dependency management with locked requirements
- Pre-commit hooks are automatically installed in dev environment
- Tests include unit, integration, and e2e categories
- Always run a test after updating it
- Always prompt for confirmation before making changes, even in agent mode
- If altering a test ALWAYS run the test after making the change
- When searching over files respect the `.gitignore` file
- Create plans in the `./plans` directory (never commit to git)
- Make sure to run the formatter and linter via `just` before committing
- Always run `just lint` after making changes and fix any errors
