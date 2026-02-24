# Contributing

## Setup development environment using Hatch

1. Clone the repository.
1. Install [hatch](https://hatch.pypa.io/) using [uv](https://docs.astral.sh/uv/):

    ```bash
    uv tool install hatch --with hatch-pip-compile
    ```

1. Create the development environment:

    ```bash
    hatch env create
    ```

    This installs all dev and test dependencies and sets up pre-commit hooks automatically.

1. Run s3fetch from source to verify the setup:

    ```bash
    hatch run s3fetch --help
    ```

## Running tests

Run unit tests:

```bash
just test-unit
```

Run integration tests:

```bash
just test-integration
```

Run all tests across all supported Python versions:

```bash
just test -a
```

Run a single test:

```bash
just test tests/unit/test_cli.py::TestValidateThreadCount
```

## Linting and formatting

```bash
just lint
just format
```

Or run all quality checks at once:

```bash
just check
```

## Pre-commit hooks

Pre-commit hooks are installed automatically when you run `hatch env create`. To install them manually:

```bash
pre-commit install
```
