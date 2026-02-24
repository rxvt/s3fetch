# Contributing

## Setup development environment

1. Clone the repository.
1. Install [hatch](https://hatch.pypa.io/) using [uv](https://docs.astral.sh/uv/):

    ```bash
    uv tool install hatch --with hatch-pip-compile
    ```

1. Create the development environment:

    ```bash
    hatch env create
    # or equivalently:
    just setup
    ```

    This installs all dev and test dependencies and sets up pre-commit hooks automatically.

1. Run s3fetch from source to verify the setup:

    ```bash
    hatch run s3fetch --help
    # or:
    just run --help
    ```

## Running tests

Tests are organised into three layers:

| Layer | Location | Description |
|-------|----------|-------------|
| **Unit** | `tests/unit/` | Isolated function tests using `moto` to mock AWS |
| **Integration** | `tests/integration/` | Multi-component tests using `moto` to mock AWS |
| **E2E** | `tests/e2e/` | Live tests against a real S3 bucket — require AWS credentials |

Run unit tests:

```bash
just test-unit
```

Run integration tests:

```bash
just test-integration
```

Run e2e tests (requires AWS credentials with access to the test bucket):

```bash
just test-e2e
```

Run all tests across all supported Python versions:

```bash
just test -a
```

Run a single test by name:

```bash
just test tests/unit/test_cli.py::TestValidateThreadCount
```

Run unit and integration tests with a coverage report (excludes e2e):

```bash
just test-coverage
```

## Linting, formatting, and type checking

Run the linter:

```bash
just lint
```

Auto-fix lint issues:

```bash
just lint-fix
```

Run the formatter:

```bash
just format
```

Check formatting without applying changes:

```bash
just format-check
```

Run all quality checks (lint + format check) at once:

```bash
just check
```

Type checking is enforced via pre-commit (mypy). To run it manually:

```bash
hatch run mypy src/s3fetch tests
```

## Pre-commit hooks

Pre-commit hooks are installed automatically when you run `hatch env create`. They run on every commit and cover:

- YAML validity, end-of-file newlines, trailing whitespace
- Ruff linting and formatting
- mypy type checking
- bandit security scanning

To install the hooks manually:

```bash
pre-commit install
```

To run all hooks against the entire codebase without committing:

```bash
pre-commit run --all-files
```

## Cleaning up build artefacts

```bash
just clean
```
