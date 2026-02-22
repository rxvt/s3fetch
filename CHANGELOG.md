# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.0.0] - Unreleased

### ⚠️ BREAKING CHANGES

- **Python Requirements**: Minimum Python version increased from 3.7 to 3.10
- **Public API**: Replaced multiple entry points with a single `download()` function
- **CLI `--progress`**: Removed `none` mode; `simple` is now the default; added `live-update` and `fancy` modes
- **Build System**: Migrated from Poetry to Hatch for dependency management

### Added

- **Public API**: New single `download()` entry point in `s3fetch.api` replacing the old multi-function API
- **`on_complete` callback**: `download()` accepts an `on_complete` callable invoked with each object key as it completes
- **`DownloadResult`**: Structured result object with `key`, `file_size`, `success`, and `error` fields emitted per download
- **`ProgressProtocol`**: Protocol class for implementing custom progress trackers compatible with both CLI and library usage
- **`--progress live-update`**: Real-time single-line status display (overwrites in place) with final summary; suppresses per-object output
- **`--progress fancy`**: Rich-powered progress bar with transfer speed and elapsed time; requires `pip install s3fetch[fancy]`
- **`s3fetch[fancy]` optional extra**: `rich>=13.0.0` installable as an optional dependency
- **Python 3.14 support**: Added to test matrix and CI
- **Atomic writes**: Downloads write to a temporary file first and rename on completion, preventing partial files on failure or interruption
- **Path traversal protection**: Destination paths are validated against the download directory using `Path.is_relative_to()`
- **Development Environment**: Full Hatch integration with locked dependencies via hatch-pip-compile
- **Type Safety**: Comprehensive type annotations across all modules
- **Code Quality**: Pre-commit hooks with Ruff, mypy, bandit, and formatting checks
- **Testing**: Structured test organisation (unit, integration, e2e) with 160+ tests
- **CLI Validation**: Input validation for regex patterns, thread counts, S3 URIs, and download directories
- **Error Handling**: Actionable error messages with troubleshooting suggestions for common AWS errors

### Changed

- **`--progress simple`**: Now the default mode; prints each object key as it downloads with no summary (previously `none` was the default)
- **`--progress detailed`**: Prints per-object keys plus a final summary (previously tried and failed to show a live status line)
- **`--quiet`**: Now explicitly mutually exclusive with `--progress`; suppresses all stdout but errors still go to stderr
- **Thread count warning**: Now warns instead of erroring when thread count exceeds 1000
- **Minimum Python Version**: Now requires Python 3.10 or higher
- **Build System**: Migrated from Poetry to Hatch
- **Code Formatting**: Migrated from Black to Ruff
- **Type Annotations**: All public functions now have complete type hints
- **Error Messages**: More actionable and user-friendly error messages

### Fixed

- **`--progress detailed` live line**: The `\r` overwrite line was broken when per-object output was active; fixed by moving the live display to `live-update` mode which correctly suppresses per-object output
- **Logging Configuration**: Global logging setup no longer interferes with library usage
- **Path traversal**: Fixed vulnerability where crafted S3 keys could write files outside the download directory
- **Disk-full errors**: `OSError` during download now gives a clear error message instead of an unhandled exception

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

The core CLI usage is unchanged:
```bash
s3fetch s3://bucket/prefix/ --download-dir /local/path/
```

The `--progress` option has new modes. The default is now `simple` (prints each
object key). Use `--progress detailed` for a final summary, `--progress live-update`
for a real-time status line, or `--progress fancy` for a Rich progress bar
(requires `pip install s3fetch[fancy]`).

### Library Usage

The public API has been simplified to a single entry point:

```python
# v1.x — multiple entry points (no longer available)
# from s3fetch.api import download_objects, list_objects

# v2.0 — single download() function
from s3fetch import download

success_count, failures = download("s3://my-bucket/prefix/")
```

Per-object callbacks replace the old completed-objects queue approach:

```python
from s3fetch import download

def on_done(key: str) -> None:
    print(f"Downloaded: {key}")

success_count, failures = download(
    "s3://my-bucket/prefix/",
    on_complete=on_done,
)
```
