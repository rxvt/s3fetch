# Agent Instructions for s3fetch

## Development Commands

- **Setup environment**: `hatch env create` (creates default environment with dev/test dependencies)
- **Run CLI**: `hatch run s3fetch <args>` or install with `pipx install -e .`
- **Run all tests**: `hatch test -a` (runs all tests for all Python versions)
- **Run single test**: `hatch run pytest tests/unit/test_cli.py::test_function_name`
- **Run unit tests**: `hatch run test_unit`
- **Run integration tests**: `hatch run test_integration`
- **Run e2e tests**: `hatch run test_e2e`
- **Specialized tests**: `hatch run test_regex`, `hatch run test_dryrun` (against live S3 bucket)
- **Type checking**: `hatch run check_types` (runs mypy on src/s3fetch and tests)
- **Linting**: `ruff check`
- **Formatting**: `ruff format`
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

## Important Notes

- Uses hatch-pip-compile for dependency management with locked requirements
- Pre-commit hooks are automatically installed in dev environment
- Tests include unit, integration, and e2e categories
- Always run a test after updating it
- Always prompt for confirmation before making changes, even in agent mode
- If altering a test ALWAYS run the test after making the change
- When searching over files respect the `.gitignore` file
- Create plans in the `./plan` directory (never commit to git)
- Make sure to run the formatter and linter via `just` before committing
- Always run `just lint` after making changes and fix any errors
