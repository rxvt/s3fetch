# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - 2025-09-27

### ⚠️ BREAKING CHANGES

- **Python Requirements**: Minimum Python version increased from 3.7 to 3.10
- **Build System**: Migrated from Poetry to Hatch for dependency management
- **Code Quality**: Major refactoring with strict type annotations and linting rules
- **CLI Interface**: Some internal API changes may affect programmatic usage

### Added

- **Development Environment**: Full Hatch integration with locked dependencies
- **Type Safety**: Comprehensive type annotations across all modules
- **Code Quality**: Pre-commit hooks with Ruff, mypy, and formatting checks
- **Testing**: Structured test organization (unit, integration, e2e)
- **CLI Validation**: Input validation for regex patterns, thread counts, and S3 URIs
- **Error Handling**: Improved exception handling and user-facing error messages
- **Logging**: Better logging configuration for both CLI and library usage
- **Documentation**: Added AGENTS.md for development guidance

### Changed

- **Minimum Python Version**: Now requires Python 3.10 or higher
- **Build System**: Migrated from Poetry to Hatch for dependency management
- **Code Formatting**: Migrated from Black to Ruff for linting and formatting
- **Type Annotations**: All functions now have proper type hints
- **Error Messages**: More actionable and user-friendly error messages
- **Testing Framework**: Organized tests into unit, integration, and e2e categories

### Fixed

- **Logging Configuration**: Global logging setup no longer interferes with library usage
- **Input Validation**: Added proper validation for CLI parameters and S3 URIs
- **Documentation**: Fixed inconsistencies between README.md and pyproject.toml
- **Thread Management**: Improved thread count validation and bounds checking

### Deprecated

- **Python 3.7-3.9**: No longer supported (use version 1.x for older Python versions)

## [1.1.6] - 2023-07-10

### Added

- Pre-commit hooks support
- Ruff integration for code quality
- Python 3.10 and 3.11 support

### Changed

- Migrated to Ruff for linting and formatting
- Updated dependency versions

## [1.1.5] - 2022-12-01

### Added

- Python 3.10 support in testing matrix
- Updated GitHub Actions versions

### Fixed

- Build and publish workflow improvements
- TestPyPI upload configuration

## [1.1.4] - 2022-11-01

### Added

- Python 3.10 support
- Updated dependencies for security

### Changed

- Updated testing workflows
- Improved GitHub Actions configuration

## [1.1.3] - 2022-05-01

### Fixed

- Thread count determination method
- Connection pool size optimization
- MacOS Monterey compatibility

### Changed

- Default thread count now uses available CPU cores
- Updated dependencies

## [1.1.2] - 2021-08-01

### Fixed

- Thread-safe stdout printing
- Graceful handling when destination directory exists

### Added

- Thread-safe printing implementation

## [1.1.1] - 2021-07-01

### Fixed

- Local object download paths
- Debug message improvements

### Changed

- Improved object listing messages

## [1.1.0] - 2021-06-01

### Added

- **Multi-threading**: Moved object listing to separate thread using FIFO queue
- **Performance**: Tuned urllib connection pool for better concurrency
- **CLI Options**: Added `--list-only` as alias for `--dry-run`

### Changed

- Object filtering now happens during initial population
- Improved download performance with optimized connection pooling

### Fixed

- Regex exception handling improvements
- Better error messages for invalid patterns

## [1.0.2] - 2021-05-01

### Added

- Python 3.7+ compatibility
- AWS and S3 packaging keywords

### Changed

- Updated license and project metadata
- Improved Python version compatibility

### Fixed

- Test workflow organization
- Documentation updates

## [1.0.1] - 2021-04-15

### Fixed

- Initial packaging issues
- Documentation improvements

## [1.0.0] - 2021-04-01

### Added

- Initial release of s3fetch
- Multi-threaded S3 download functionality
- Regex filtering support
- Dry-run mode
- CLI interface with Click
- AWS credentials integration
- Concurrent download support

### Features

- Download files from S3 buckets with configurable concurrency
- Filter objects using regex patterns
- Dry-run mode for testing without downloading
- Progress indication and status reporting
- Support for AWS credential profiles and IAM roles

---

## Migration Guide: v1.x to v2.0

### Python Version Requirements

**Before (v1.x):**
```bash
# Supported Python 3.7+
python3.7 -m pip install s3fetch
```

**After (v2.0):**
```bash
# Requires Python 3.10+
python3.10 -m pip install s3fetch
```

### Development Setup

**Before (v1.x):**
```bash
# Poetry-based development
poetry install
poetry run s3fetch --help
```

**After (v2.0):**
```bash
# Hatch-based development
hatch env create
hatch run s3fetch --help
```

### Testing

**Before (v1.x):**
```bash
# Poetry and Nox
poetry run nox -s tests
```

**After (v2.0):**
```bash
# Hatch testing
hatch test -a
hatch test tests/unit
hatch test tests/integration
hatch test tests/e2e
```

### CLI Usage

The CLI interface remains the same for end users:
```bash
s3fetch s3://bucket/prefix/ /local/path/
```

### Library Usage

Public API functions remain compatible, but internal imports may have changed:
```python
# Still works in v2.0
from s3fetch.api import download_objects
```
