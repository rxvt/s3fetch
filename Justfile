# s3fetch development commands using Hatch

# Default recipe - show available commands
[private]
default:
    @just --list

# Setup and Dependencies
# Create development environment and install dependencies
setup:
    hatch env create

# Development
# Run s3fetch CLI with arguments
run *args:
    hatch run s3fetch {{args}}

# Release
# Bump version, run full test suite, commit, tag, push, and create GitHub release
release version:
    #!/usr/bin/env bash
    set -euo pipefail

    if [ -n "$(git status --porcelain)" ]; then
        echo "Working tree is not clean. Commit or stash changes before releasing."
        exit 1
    fi

    just check
    just test -a
    hatch version {{version}}
    git add src/s3fetch/__init__.py
    git commit -m "Bump version to {{version}}"
    git tag -a "v{{version}}" -m "Release v{{version}}"
    git push origin HEAD
    git push origin "v{{version}}"
    gh release create "v{{version}}" --title "v{{version}}" --generate-notes

# Testing
# Run all tests
test *args:
    hatch test {{args}}

# Run unit tests only
test-unit *args:
    hatch test {{args}} tests/unit

# Run integration tests only
test-integration *args:
    hatch test {{args}} tests/integration

# Run e2e tests only
test-e2e *args:
    hatch test {{args}} tests/e2e

# Run tests with coverage (excludes e2e tests which require AWS credentials)
test-coverage *args:
    hatch test {{args}} --cover -- tests/unit tests/integration

# Code Quality
# Run linting with ruff
lint *args:
    hatch run ruff check {{args}}

# Fix linting issues automatically
lint-fix *args:
    hatch run ruff check --fix {{args}}

# Run formatting with ruff
format *args:
    hatch run ruff format {{args}}

# Check formatting without applying changes
format-check *args:
    hatch run ruff format --check {{args}}

# Run all quality checks (type check + lint + format check)
check: lint format-check

# Run pre-commit inside the env
pre-commit *args:
  hatch run pre-commit run {{args}}

# Cleanup
clean:
    find . -type d -name "__pycache__" -delete
    find . -type f -name "*.pyc" -delete
    find . -type d -name "*.egg-info" -exec rm -rf {} +
    rm -rf tmp/
    rm -rf htmlcov/
    rm -rf .coverage
    rm -rf .pytest_cache/
    rm -rf .mypy_cache/
    rm -rf .ruff_cache/
