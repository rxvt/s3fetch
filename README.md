# S3Fetch

<!--toc:start-->
- [S3Fetch](#s3fetch)
  - [Features](#features)
  - [Why use S3Fetch?](#why-use-s3fetch)
  - [Benchmarks](#benchmarks)
    - [With 100 threads](#with-100-threads)
      - [With 8 threads](#with-8-threads)
  - [Installation](#installation)
    - [Requirements](#requirements)
    - [uv (recommended)](#uv-recommended)
    - [pip](#pip)
  - [Usage](#usage)
  - [Examples](#examples)
    - [Full example](#full-example)
    - [Download all objects from a bucket](#download-all-objects-from-a-bucket)
    - [Download objects with a specific prefix](#download-objects-with-a-specific-prefix)
    - [Download objects to a specific directory](#download-objects-to-a-specific-directory)
    - [Download multiple objects concurrently](#download-multiple-objects-concurrently)
    - [Filter objects using regular expressions](#filter-objects-using-regular-expressions)
  - [Troubleshooting](#troubleshooting)
    - [Existing files are silently overwritten](#existing-files-are-silently-overwritten)
    - [MacOS hangs when downloading using high number of threads](#macos-hangs-when-downloading-using-high-number-of-threads)
<!--toc:end-->

Simple & fast multi-threaded S3 download tool.

Source: [https://github.com/rxvt/s3fetch](https://github.com/rxvt/s3fetch)

![Build and Publish](https://github.com/rxvt/s3fetch/workflows/Build%20and%20Publish/badge.svg?branch=main)
![Test](https://github.com/rxvt/s3fetch/workflows/Test/badge.svg?branch=development)
[![PyPI version](https://badge.fury.io/py/s3fetch.svg)](https://badge.fury.io/py/s3fetch)

## Features

- Fast.
- Simple to use.
- Multi-threaded, allowing you to download multiple objects concurrently.
- Quickly download a subset of objects under a prefix without listing all objects first.
- Object listing occurs in a separate thread and downloads start as soon as the first object key is returned while the object listing completes in the background.
- Filter list of objects using regular expressions.
- Uses standard Boto3 AWS SDK and standard AWS credential locations.
- List only mode if you just want to see what would be downloaded.
- Implemented as a simple API you can use in your own projects.

## Why use S3Fetch?

Tools such as the [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-welcome.html) and [s4cmd](https://pypi.org/project/s4cmd/) are great and offer a lot of features, but S3Fetch out performs them when downloading a subset of objects from a large S3 bucket.

Benchmarking shows (see below) that S3Fetch can finish downloading 428 objects from a bucket containing 12,204,097 objects in 8 seconds while other tools have not started downloading a single object after 60 minutes.

## Benchmarks

Downloading 428 objects under the `fake-prod-data/2020-10-17` prefix from a bucket containing a total of 12,204,097 objects.

### With 100 threads

```text
s3fetch s3://fake-test-bucket/fake-prod-data/2020-10-17  --threads 100

8.259 seconds
```

```text
s4cmd get s3://fake-test-bucket/fake-prod-data/2020-10-17* --num-threads 100

Timed out while listing objects after 60min.
```

#### With 8 threads

```text
s3fetch s3://fake-test-bucket/fake-prod-data/2020-10-17  --threads 8

29.140 seconds
```

```text
time s4cmd get s3://fake-test-bucket/fake-prod-data/2020-10-17* --num-threads 8

Timed out while listing objects after 60min.
```

## Installation

### Requirements

- Python >= 3.10
- AWS credentials in one of the [standard locations](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html#cli-configure-files-where)

S3Fetch is available on PyPi and can be installed via one of the following methods.

### uv (recommended)

Ensure you have [uv](https://docs.astral.sh/uv/) installed, then:

`uv tool install s3fetch`

### pip

`pip3 install s3fetch`

### Development Installation

For development work on s3fetch:

1. Clone the repository:
   ```bash
   git clone https://github.com/rxvt/s3fetch.git
   cd s3fetch
   ```

2. Install [hatch](https://hatch.pypa.io/) using [uv](https://docs.astral.sh/uv/):
   ```bash
   uv tool install hatch --with hatch-pip-compile
   ```

3. Set up the development environment using hatch:
   ```bash
   hatch env create
   ```

4. Run s3fetch from source:
   ```bash
   hatch run s3fetch --help
   ```

5. (Optional) Populate a test S3 bucket with test data for development:
   ```bash
   # First create your own test bucket (use a unique name!)
   aws s3 mb s3://your-unique-s3fetch-test-bucket-name --region us-east-1

   # Then populate it with test data
   hatch run python scripts/populate_test_bucket.py --bucket your-unique-s3fetch-test-bucket-name --dry-run  # See what would be created
   hatch run python scripts/populate_test_bucket.py --bucket your-unique-s3fetch-test-bucket-name           # Actually populate
   ```

## Usage

```text
Usage: s3fetch [OPTIONS] S3_URI

  Concurrently download objects from S3 buckets.

  Examples:

    s3fetch s3://my-bucket/

    s3fetch s3://my-bucket/photos/ --regex ".*\.jpg$"

    s3fetch s3://my-bucket/data/ --dry-run --threads 10

Options:
  --version                       Show the version and exit.
  --region TEXT                   AWS region for the S3 bucket (e.g., us-
                                  east-1, eu-west-1). Defaults to 'us-east-1'.
  -d, --debug                     Enable verbose debug output.
  --download-dir PATH             Local directory to save downloaded files.
                                  Must already exist. Defaults to current
                                  directory.
  -r, --regex TEXT                Filter objects using regular expressions
                                  (e.g., '.*\.jpg$' for JPEG files).
  -t, --threads INTEGER           Number of concurrent download threads
                                  (minimum 1, warns above 1000). Defaults to
                                  CPU core count.
  --dry-run, --list-only          Show what would be downloaded without
                                  actually downloading files.
  --delimiter TEXT                Object key delimiter for path structure.
                                  Defaults to '/'.
  -q, --quiet                     Suppress all stdout; errors still go to
                                  stderr. Mutually exclusive with --progress.
  --progress [simple|detailed|live-update|fancy]
                                  Progress display mode. 'simple' (default)
                                  prints each object key as it downloads.
                                  'detailed' adds a summary at the end.
                                  'live-update' shows a real-time status line
                                  and summary (no per-object output).
                                  'fancy' shows a Rich progress bar and summary
                                  (requires: pip install s3fetch[fancy]).
  --help                          Show this message and exit.
```

## Examples

### Full example

Download using 100 threads into `~/Downloads/tmp`, only downloading objects that end in `.dmg`.

```text
$ s3fetch s3://my-test-bucket --download-dir ~/Downloads/tmp/ --threads 100  --regex '\.dmg$'
test-1.dmg...done
test-2.dmg...done
test-3.dmg...done
test-4.dmg...done
test-5.dmg...done
```

### Download all objects from a bucket

```text
s3fetch s3://my-test-bucket/
```

### Download objects with a specific prefix

Download all objects that start with `birthday-photos/2020-01-01`.

```text
s3fetch s3://my-test-bucket/birthday-photos/2020-01-01
```

### Download objects to a specific directory

Download objects to the `~/Downloads` directory.

```text
s3fetch s3://my-test-bucket/ --download-dir ~/Downloads
```

### Download multiple objects concurrently

Download 100 objects concurrently.

```text
s3fetch s3://my-test-bucket/ --threads 100
```

### Filter objects using regular expressions

Download objects ending in `.dmg`.

```text
s3fetch s3://my-test-bucket/ --regex '\.dmg$'
```

## Library Usage

S3Fetch can be used as a library in your Python projects.

### Basic Library Usage

```python
from s3fetch import download

success_count, failures = download("s3://my-bucket/data/2023/")

print(f"Downloaded {success_count} objects successfully")
if failures:
    print(f"{len(failures)} objects failed to download")
```

### Common Options

```python
from s3fetch import download

success_count, failures = download(
    "s3://my-bucket/data/",
    download_dir="./downloads",   # local destination (default: cwd)
    regex=r"\.csv$",              # only download .csv files
    threads=20,                   # concurrent downloads (default: CPU count)
    dry_run=False,                # set True to list without downloading
)
```

### Configuring Logging

When using S3Fetch as a library, you can configure its logging behavior:

```python
import logging

# Option 1: Reduce s3fetch output
logging.getLogger("s3fetch").setLevel(logging.WARNING)

# Option 2: Disable s3fetch logging completely
logging.getLogger("s3fetch").disabled = True
```

### Progress Tracking

```python
from s3fetch import download
from s3fetch.utils import ProgressTracker

tracker = ProgressTracker()
success_count, failures = download(
    "s3://my-bucket/data/",
    progress_tracker=tracker,
)

stats = tracker.get_stats()
print(f"Found:      {stats['objects_found']} objects")
print(f"Downloaded: {stats['objects_downloaded']} objects")
print(f"Total size: {stats['bytes_downloaded'] / (1024 * 1024):.1f} MB")
print(f"Speed:      {stats['download_speed_mbps']:.2f} MB/s")
```

The `ProgressTracker` is thread-safe and can be polled from a separate thread
for real-time updates while `download()` is running.

### Advanced Usage — Custom boto3 Client

Pass a pre-built boto3 client to use a custom session, role, or region:

```python
import boto3
from s3fetch import download

### Download Callbacks

Every download attempt — success or failure — places a `DownloadResult` on the
completed queue.  By supplying your own handler to
`create_completed_objects_thread` you get real-time, per-object notifications
with rich context, enabling use cases such as:

- Driving a [Rich](https://rich.readthedocs.io/en/stable/progress.html) progress bar
- Pipelining downloads directly into a compression stream (e.g. adding each
  file to a tarball as soon as it lands, without waiting for the full download
  to finish)
- Ops reporting / audit logging

`DownloadResult` fields:

| Field | Type | Description |
|-------|------|-------------|
| `key` | `str` | S3 object key |
| `dest_filename` | `Path` | Absolute local destination path |
| `success` | `bool` | `True` on success, `False` on failure |
| `file_size` | `int` | Bytes written (0 on failure or dry-run) |
| `error` | `Exception \| None` | Exception that caused the failure, or `None` |

#### Basic example — print every result

```python
import boto3
import threading
from s3fetch.api import create_completed_objects_thread, download_objects, list_objects
from s3fetch.exceptions import S3FetchQueueClosed
from s3fetch.s3 import DownloadResult, S3FetchQueue, create_download_config

def my_handler(queue: S3FetchQueue) -> None:
    while True:
        try:
            result: DownloadResult = queue.get(block=True)
            status = "ok" if result.success else f"FAILED: {result.error}"
            print(f"{result.key} ({result.file_size} bytes) — {status}")
        except S3FetchQueueClosed:
            break

s3_client = boto3.client("s3")
download_queue: S3FetchQueue[str] = S3FetchQueue()
completed_queue: S3FetchQueue[DownloadResult] = S3FetchQueue()
exit_event = threading.Event()

list_objects(
    bucket="my-bucket",
    prefix="exports/2024/",
    client=s3_client,
    download_queue=download_queue,
    delimiter="/",
    regex=None,
    exit_event=exit_event,
)

# Start your custom handler *before* downloading
create_completed_objects_thread(queue=completed_queue, func=my_handler)

download_config = create_download_config()
success_count, failed_downloads = download_objects(
    client=s3_client,
    threads=10,
    download_queue=download_queue,
    completed_queue=completed_queue,
    exit_event=exit_event,
    bucket="my-bucket",
    prefix="exports/2024/",
    download_dir="./downloads",
    delimiter="/",
    download_config=download_config,
)
```

#### Pipeline example — compress as files arrive

This pattern lets you add each downloaded file to a tar archive concurrently
with ongoing downloads, rather than waiting for all downloads to finish first:

```python
import queue
import tarfile
import threading
from pathlib import Path

import boto3
from s3fetch.api import create_completed_objects_thread, download_objects, list_objects
from s3fetch.exceptions import S3FetchQueueClosed
from s3fetch.s3 import DownloadResult, S3FetchQueue, create_download_config

# A small work queue between the download handler and the tar thread
tar_queue: queue.Queue = queue.Queue()

def on_download(queue: S3FetchQueue) -> None:
    """Forward successful downloads to the tar work queue."""
    while True:
        try:
            result: DownloadResult = queue.get(block=True)
            if result.success:
                tar_queue.put(result.dest_filename)
        except S3FetchQueueClosed:
            tar_queue.put(None)  # sentinel
            break

def tar_worker(archive_path: Path) -> None:
    """Compress files as they arrive from the download handler."""
    with tarfile.open(archive_path, "w:gz") as tar:
        while True:
            path = tar_queue.get()
            if path is None:
                break
            tar.add(path, arcname=path.name)
            print(f"Compressed {path.name}")

# Start the compression thread
archive = Path("output.tar.gz")
threading.Thread(target=tar_worker, args=(archive,), daemon=True).start()

# Run the download
s3_client = boto3.client("s3")
download_queue: S3FetchQueue[str] = S3FetchQueue()
completed_queue: S3FetchQueue[DownloadResult] = S3FetchQueue()
exit_event = threading.Event()

list_objects(
    bucket="my-bucket",
    prefix="data/",
    client=s3_client,
    download_queue=download_queue,
    delimiter="/",
    regex=None,
    exit_event=exit_event,
)

create_completed_objects_thread(queue=completed_queue, func=on_download)

download_config = create_download_config()
download_objects(
    client=s3_client,
    threads=20,
    download_queue=download_queue,
    completed_queue=completed_queue,
    exit_event=exit_event,
    bucket="my-bucket",
    prefix="data/",
    download_dir="./downloads",
    delimiter="/",
    download_config=download_config,
)
```

#### Custom progress tracker

You can also implement the `ProgressProtocol` to receive aggregate counts
during listing and downloading without needing the completed queue at all:

```python
from s3fetch import ProgressProtocol

class MyTracker:
    """Minimal tracker that satisfies ProgressProtocol."""

    def increment_found(self) -> None:
        print(".", end="", flush=True)  # one dot per object listed

    def increment_downloaded(self, bytes_count: int) -> None:
        print(f" +{bytes_count // 1024}KB", end="", flush=True)

# Pass to list_objects and download_objects via progress_tracker=MyTracker()
```

session = boto3.Session(profile_name="production")
client = session.client("s3", region_name="us-west-2")

success_count, failures = download(
    "s3://my-bucket/data/",
    client=client,
)
```

### Per-Object Completion Callback

```python
from s3fetch import download

def on_object_complete(key: str) -> None:
    print(f"Finished: {key}")

success_count, failures = download(
    "s3://my-bucket/data/",
    on_complete=on_object_complete,
)
```

## Troubleshooting

### Existing files are silently overwritten

s3fetch does not check whether a file already exists before downloading. If you run s3fetch
twice against the same download directory, existing files will be silently overwritten with
the latest version from S3.

If you want to avoid overwriting files, use a fresh download directory or move previously
downloaded files before re-running.

### MacOS hangs when downloading using high number of threads

From my testing this is caused by Spotlight on MacOS trying to index a large number of files at once.

You can exclude the directory you're using to store your downloads via the Spotlight system preference control panel.
