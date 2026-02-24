# S3Fetch — Developer Guide

## Overview

S3Fetch is a simple, fast, multi-threaded S3 download tool that also works as a Python library. Its core design advantage is that downloads begin immediately while listing is still in progress — making it dramatically faster than tools like `aws s3 cp` or `s4cmd` when downloading a subset of objects from a bucket containing millions of objects.

---

## Project Structure

```
main/
├── src/s3fetch/
│   ├── __init__.py       # Public exports: download, DownloadResult, ProgressProtocol
│   ├── __main__.py       # Enables `python -m s3fetch`
│   ├── api.py            # Public library API (download() entry point)
│   ├── cli.py            # Click-based CLI entry point
│   ├── s3.py             # Core S3 operations, threading, queue management
│   ├── aws.py            # AWS credential handling and boto3 client creation
│   ├── fs.py             # Filesystem operations (directory creation, path safety)
│   ├── utils.py          # ProgressTracker, ProgressProtocol, helper functions
│   └── exceptions.py     # Custom exception hierarchy
├── tests/
│   ├── unit/             # Fast, isolated tests with moto-backed S3
│   ├── integration/      # Multi-component tests, also moto-backed
│   └── e2e/              # Live tests against a real AWS S3 bucket
├── benchmarks/           # Benchmark scripts for EC2
├── infra/                # AWS CloudFormation stack for CI test bucket
├── scripts/              # Test data population scripts
├── pyproject.toml        # Build system, dependencies, tool config
├── Justfile              # Task runner (wraps hatch commands)
└── .pre-commit-config.yaml
```

---

## Architecture

### Layer Overview

```
CLI (cli.py)  ──►  API (api.py)  ──►  S3 (s3.py)  ──►  FS (fs.py)
                        │                  │
                        │                  └──► AWS (aws.py)
                        │
                   utils.py  (ProgressTracker, ProgressProtocol)
                   exceptions.py
```

### Component Responsibilities

**`cli.py`** — Click-based entry point. Validates all user input (URI format, regex, thread count, region, download directory) before any S3 work begins. Manages the four progress display modes. `run_cli()` sets up queues, starts threads, and delegates to `api.py`.

**`api.py`** — The single public library entry point via `download()`. Wires together URI parsing, directory setup, thread count, boto3 client, queue creation, listing thread, download threads, and an optional per-object completion callback. Returns `(success_count, failures_list)`.

**`s3.py`** — The core engine. Contains `S3FetchQueue`, `DownloadResult`, and all threading logic. Key functions: `list_objects()` (paginating S3), `create_list_objects_thread()`, `create_download_threads()`, `download()`, and `download_object()`.

**`aws.py`** — Creates the boto3 S3 client with a correctly sized urllib3 connection pool via `get_client()`. Pool size is computed by `calc_connection_pool_size()` to avoid exhaustion under high thread counts.

**`fs.py`** — `create_destination_directory()` converts S3 key paths into local directory structures and enforces path traversal protection. `check_download_dir_exists()` validates the download directory at startup.

**`utils.py`** — `ProgressProtocol` (a `@runtime_checkable` Protocol) and `ProgressTracker` (thread-safe concrete implementation). Also contains `get_thread_count()` for CPU-aware thread detection and `print_completed_objects()` for the default simple/detailed progress modes.

**`exceptions.py`** — Custom exception hierarchy rooted at `S3FetchError`. Specific types include `RegexError`, `DirectoryDoesNotExistError`, `DownloadError`, `NoObjectsFoundError`, `NoCredentialsError`, `PathTraversalError`, etc.

---

## Core Data Flow

### Primary Download Flow

```
User invokes cli() / download()
  │
  ├─ Split URI → (bucket, prefix)
  ├─ Validate download directory
  ├─ Create boto3 client (with sized connection pool)
  │
  ├─ Create download_queue: S3FetchQueue[str]             (object keys)
  ├─ Create completed_queue: S3FetchQueue[DownloadResult]
  ├─ Create exit_event: threading.Event
  │
  ├─► THREAD: list_objects()
  │     └─ Paginate S3 list_objects_v2
  │          ├─ Skip directory keys (key.endswith(delimiter))
  │          ├─ Apply compiled regex filter (re.search)
  │          ├─ Each match → download_queue.put(key)
  │          │              + progress_tracker.increment_found()
  │          └─ Done → download_queue.close()  [puts sentinel None]
  │
  └─► ThreadPoolExecutor(max_workers=N)
        └─ Main loop: download_queue.get(block=True) → submit download()
             │  (breaks when S3FetchQueueClosed raised)
             │
             └─ each download():
                  ├─ process_key() → (dst_dir, dst_file)
                  │    ├─ rollup_object_key_by_prefix()  [strip prefix]
                  │    └─ split_object_key_into_dir_and_file()
                  ├─ create_destination_directory()      [mkdir -p]
                  └─ download_object():
                       ├─ Write to dest.s3fetch_tmp      [atomic write]
                       ├─ Rename tmp → dest
                       ├─ progress_tracker.increment_downloaded(file_size)
                       └─ completed_queue.put(DownloadResult(...))
```

### Key Design: Streaming Start

The listing thread and the download `ThreadPoolExecutor` run concurrently. Downloads begin as soon as the first key lands in the queue — there is no waiting for listing to complete. This is the fundamental performance advantage of S3Fetch.

### Dual-Queue Architecture

Two queues flow in sequence:

1. **`download_queue` (`S3FetchQueue[str]`)** — Listing thread → download workers. Contains raw S3 object keys.
2. **`completed_queue` (`S3FetchQueue[DownloadResult]`)** — Download workers → completion consumer. Used for progress display, `on_complete` callbacks, and error reporting.

These queues are entirely separate, so the completion consumer never blocks the download workers.

### Sentinel-Based Queue Closure

`S3FetchQueue.close()` puts `None` into the queue. `get()` raises `S3FetchQueueClosed` when it reads `None`. This provides a clean shutdown signal: the generic type `S3FetchQueue[T]` means `None` is always out-of-band and can never be confused with a valid item. See `s3.py:72–124`.

---

## Notable Design Decisions

### Atomic Writes (`s3.py:664–717`)
`download_object()` writes to `dest.s3fetch_tmp` first, then calls `Path.replace()` (atomic on POSIX). On any failure the temp file is removed via `unlink(missing_ok=True)` before re-raising. This prevents partial or corrupted files surviving a crash, network cut, or `Ctrl+C`.

### Path Traversal Protection (`fs.py:48–54`)
`create_destination_directory()` resolves the full absolute destination path and asserts it is `relative_to(download_dir.resolve())` before creating it. This blocks crafted S3 keys like `../../etc/shadow` from escaping the intended download directory.

### Connection Pool Sizing (`aws.py:16–35`)
Each download thread uses up to 10 concurrent S3 connections for multipart transfers. `calc_connection_pool_size()` computes `max(MAX_POOL_CONNECTIONS, threads * DEFAULT_S3TRANSFER_CONCURRENCY)` to prevent urllib3 connection pool exhaustion under high thread counts. `DEFAULT_S3TRANSFER_CONCURRENCY = 10` is defined in `s3.py:41`.

### `ProgressProtocol` / Structural Subtyping (`utils.py:17–53`)
Rather than an ABC, `ProgressProtocol` is a `@runtime_checkable` Protocol. Any class with `increment_found()` and `increment_downloaded(bytes_count)` qualifies without inheritance. This enables duck-typed custom trackers (Rich bars, Prometheus metrics, etc.).

### Lock Strategy in `ProgressTracker` (`utils.py:63–84`)
`objects_found` is only ever incremented from the single listing thread, so it needs no lock. Only `objects_downloaded` and `bytes_downloaded` (incremented from N download threads) are protected by `_download_lock`, avoiding unnecessary contention on the hot path.

### Key Rollup Algorithm (`s3.py:496–524`)
Given prefix `my/test/objects/` and key `my/test/objects/one/two/file`, `rollup_object_key_by_prefix()` strips the prefix by counting delimiters and splitting, producing `one/two/file`, which then maps to the local directory structure `one/two/` + filename `file`.

### Thread Count Detection (`utils.py:126–151`)
Prefers `os.sched_getaffinity(0)` (Linux, respects cgroup CPU limits) over `os.cpu_count()`. Always falls back to 1 if detection fails.

### Version as Single Source of Truth
Version lives only in `src/s3fetch/__init__.py` (the `__version__` variable). `pyproject.toml` reads it via `[tool.hatch.version] path = "src/s3fetch/__init__.py"` — no manual synchronization required.

---

## Public API

The library exposes two tiers:

**Simple:** `download()` from `s3fetch` — covers the majority of use cases.

**Advanced:** `create_completed_objects_thread()` + `DownloadResult` + `ProgressProtocol` — for consumers that need real-time per-object hooks or custom progress rendering.

```python
from s3fetch import download, DownloadResult, ProgressProtocol
```

---

## CLI Options Reference

| Option | CLI Flag | API Parameter | Default | Notes |
|---|---|---|---|---|
| S3 URI | positional | `s3_uri` | required | Must start with `s3://` |
| Download dir | `--download-dir` | `download_dir` | CWD | Must exist; path traversal checked |
| Regex filter | `-r/--regex` | `regex` | None | Applied via `re.search()` on each key |
| Threads | `-t/--threads` | `threads` | CPU count | Min 1; warns if > 1000 |
| Region | `--region` | `region` | `us-east-1` | Ignored if custom `client` passed |
| Delimiter | `--delimiter` | `delimiter` | `/` | S3 key hierarchy separator |
| Dry run | `--dry-run/--list-only` | `dry_run` | False | Lists without downloading |
| Quiet | `-q/--quiet` | n/a | False | Mutually exclusive with `--progress` |
| Progress mode | `--progress` | n/a | `simple` | `simple`, `detailed`, `live-update`, `fancy` |
| Debug | `-d/--debug` | n/a | False | Sets log level to DEBUG |
| Custom client | n/a | `client` | None | Override for custom sessions/roles |
| Completion callback | n/a | `on_complete` | None | `Callable[[str], None]` per key |
| Progress tracker | n/a | `progress_tracker` | None | Must satisfy `ProgressProtocol` |

### Progress Modes (managed in `cli.py:run_cli()`)

- **`simple`** — Prints each object key to stdout as it completes.
- **`detailed`** — Same as simple, plus a final summary (requires `ProgressTracker`).
- **`live-update`** — Suppresses per-object output; a daemon thread overwrites a single `\r` status line every 2 seconds, plus final summary.
- **`fancy`** — Rich progress bar in a daemon thread. Requires `pip install s3fetch[fancy]`. Availability is checked at startup via `importlib.util.find_spec("rich")`.

---

## Testing

Three-tier test organisation:

### Unit Tests (`tests/unit/`)
Fast, isolated tests using `moto[s3]` to mock AWS. The `conftest.py` fixture activates `@mock_aws` and provides a real-looking boto3 client. Coverage includes: queue sentinel behaviour, regex filtering, directory exclusion, key rollup, atomic temp-file behaviour, `DownloadResult` emission, special characters in keys, zero-byte objects, overwrite behaviour, and `OSError` handling.

### Integration Tests (`tests/integration/`)
Also moto-backed, but test higher-level interactions between multiple components. Covers `on_complete` callbacks, `ProgressTracker` wired through listing and download threads together, dry run, AWS error codes mapping to correct user-facing CLI messages, and all progress modes.

### E2E Tests (`tests/e2e/`)
Hit the real AWS bucket `s3://s3fetch-cicd-test-bucket` (provisioned via CloudFormation in `infra/s3-bucket.yaml`). Run in CI with OIDC role-based AWS credentials. Covers: single-file download, 120-file bulk download, regex filtering, dry run, all progress modes, custom download directory, and multi-threaded downloads.

### Running Tests

```bash
just test              # All tests (unit + integration)
just test-unit         # Unit tests only
just test-integration  # Integration tests only
just test-e2e          # E2E tests (requires AWS credentials)
just test-coverage     # Tests with coverage report
```

Or via hatch directly:

```bash
hatch test
hatch test --cover
```

---

## Build System and Tooling

- **Build backend:** [Hatch](https://hatch.pypa.io/) + hatchling
- **Dependency locking:** [hatch-pip-compile](https://github.com/juftin/hatch-pip-compile)
- **Minimum Python:** 3.10 (uses `X | Y` union syntax, `Path.is_relative_to()`, structural pattern matching)
- **Python matrix:** 3.10, 3.11, 3.12, 3.13, 3.14

### Runtime Dependencies

- `boto3` — AWS SDK
- `boto3-stubs[s3]` — Type stubs for mypy
- `click` — CLI framework
- `rich` *(optional, `[fancy]` extra)* — Fancy progress bar

### Dev / CI Tooling

- **Linting/formatting:** `ruff`
- **Type checking:** `mypy`
- **Security scanning:** `bandit` (excludes tests)
- **Pre-commit hooks:** `ruff`, `mypy`, `bandit`, `cfn-lint`, plus standard file hygiene checks
- **Task runner:** `Justfile` (wraps hatch; run `just` to see all recipes)

### Common Tasks

```bash
just setup          # Install dev dependencies
just lint           # Run ruff + mypy
just lint-fix       # Auto-fix lint issues
just format         # Run ruff formatter
just check          # lint + tests
just clean          # Remove build artifacts
```

### CI/CD (`.github/workflows/`)

- **`test.yml`** — Runs on push/PR across all 5 Python versions; assumes OIDC role for AWS access to the test bucket.
- **`build-and-publish.yml`** — Triggered on GitHub release; builds wheel + sdist, verifies with `twine check`, publishes to PyPI via OIDC.
- **`pre-commit.yml`** — Runs pre-commit hooks in CI.
