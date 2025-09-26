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

# Testing
# Run all tests
test:
    hatch run pytest

# Run unit tests only
test-unit:
    hatch run test_unit

# Run integration tests only
test-integration:
    hatch run test_integration

# Run e2e tests only
test-e2e:
    hatch run test_e2e

# Run tests with coverage
test-coverage:
    hatch run pytest --cov=s3fetch --cov-report=html --cov-report=term

# Code Quality
# Run type checking with mypy
typecheck:
    hatch run check_types

# Run linting with ruff
lint:
    hatch run ruff check

# Run formatting with ruff
format:
    hatch run ruff format

# Check formatting without applying changes
format-check:
    hatch run ruff format --check

# Fix linting issues automatically
lint-fix:
    hatch run ruff check --fix

# Run all quality checks (type check + lint + format check)
check: typecheck lint format-check

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
