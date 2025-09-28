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
    - [pipx (recommended)](#pipx-recommended)
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

### pipx (recommended)

Ensure you have [pipx](https://pypi.org/project/pipx/) installed, then:

`pipx install s3fetch`

### pip

`pip3 install s3fetch`

### Development Installation

For development work on s3fetch:

1. Clone the repository:
   ```bash
   git clone https://github.com/rxvt/s3fetch.git
   cd s3fetch
   ```

2. Set up the development environment using hatch:
   ```bash
   hatch env create
   ```

3. Run s3fetch from source:
   ```bash
   hatch run s3fetch --help
   ```

4. (Optional) Populate a test S3 bucket with test data for development:
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

  Easily download objects from an S3 bucket.

  Example: s3fetch s3://my-test-bucket/birthday-photos/2020-01-01

  The above will download all S3 objects located under the `birthday-
  photos/2020-01-01` prefix.

  You can download all objects in a bucket by using `s3fetch s3://my-test-
  bucket/`

Options:
  --region TEXT           Bucket region. Defaults to 'us-east-1'.
  -d, --debug             Enable debug output.
  --download-dir TEXT     Download directory. Defaults to current directory.
  -r, --regex TEXT        Filter list of available objects by regex.
  -t, --threads INTEGER   Number of threads to use. Defaults to core count.
  --dry-run, --list-only  List objects only, do not download.
  --delimiter TEXT        Specify the "directory" delimiter. Defaults to '/'.
  -q, --quiet             Don't print to stdout.
  --version               Print out version information.
  --help                  Show this message and exit.
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

S3Fetch can be used as a library in your Python projects. Here's how to use it programmatically:

### Basic Library Usage

```python
import boto3
import threading
from s3fetch.api import list_objects, download_objects
from s3fetch.s3 import S3FetchQueue, create_download_config

# Setup S3 client and queues
s3_client = boto3.client("s3")
download_queue = S3FetchQueue()
completed_queue = S3FetchQueue()
exit_event = threading.Event()

# Download all objects from a bucket prefix
list_objects(
    bucket="my-test-bucket",
    prefix="data/2023/",
    client=s3_client,
    download_queue=download_queue,
    delimiter="/",
    regex=None,
    exit_event=exit_event
)

download_config = create_download_config()
success_count, failed_downloads = download_objects(
    client=s3_client,
    threads=10,
    download_queue=download_queue,
    completed_queue=completed_queue,
    exit_event=exit_event,
    bucket="my-test-bucket",
    prefix="data/2023/",
    download_dir="./downloads",
    delimiter="/",
    download_config=download_config
)

print(f"Downloaded {success_count} objects successfully")
```

### Configuring Logging

When using S3Fetch as a library, you can configure its logging behavior:

```python
import logging
import s3fetch

# Option 1: Configure s3fetch's logger level
s3fetch_logger = logging.getLogger("s3fetch")
s3fetch_logger.setLevel(logging.WARNING)  # Reduce s3fetch output
s3fetch_logger.addHandler(logging.StreamHandler())

# Option 2: Disable s3fetch logging completely
s3fetch_logger = logging.getLogger("s3fetch")
s3fetch_logger.disabled = True

# Then use s3fetch normally with the API functions
# (See Basic Library Usage section for complete example)
```

### Progress Tracking

S3Fetch includes built-in progress tracking capabilities that you can use to monitor download progress in your applications:

```python
import s3fetch
from s3fetch.utils import ProgressTracker

# Create a progress tracker
tracker = ProgressTracker()

# Download with progress tracking
s3fetch.download_bucket(
    bucket="my-bucket",
    prefix="data/",
    download_dir="./downloads",
    threads=10,
    progress_tracker=tracker
)

# Monitor progress during download
stats = tracker.get_stats()
print(f"Found: {stats['objects_found']} objects")
print(f"Downloaded: {stats['objects_downloaded']} objects")
print(f"Total size: {stats['bytes_downloaded']} bytes")
print(f"Speed: {stats['download_speed_mbps']:.2f} MB/s")
```

For more granular control, you can also monitor progress in real-time:

```python
import boto3
import threading
import time
from s3fetch.api import list_objects, download_objects
from s3fetch.s3 import S3FetchQueue, create_download_config
from s3fetch.utils import ProgressTracker

def monitor_progress(tracker):
    """Monitor and display progress updates."""
    while True:
        stats = tracker.get_stats()
        print(f"\rFound: {stats['objects_found']} | "
              f"Downloaded: {stats['objects_downloaded']} | "
              f"Speed: {stats['download_speed_mbps']:.1f} MB/s", end="")
        time.sleep(1)

# Create progress tracker
tracker = ProgressTracker()

# Start monitoring thread
monitor_thread = threading.Thread(target=monitor_progress, args=(tracker,))
monitor_thread.daemon = True
monitor_thread.start()

# Setup and start download
s3_client = boto3.client("s3")
download_queue = S3FetchQueue()
completed_queue = S3FetchQueue()
exit_event = threading.Event()

list_objects(
    bucket="my-bucket",
    prefix="large-dataset/",
    client=s3_client,
    download_queue=download_queue,
    delimiter="/",
    regex=None,
    exit_event=exit_event,
    progress_tracker=tracker
)

download_config = create_download_config()
success_count, failed_downloads = download_objects(
    client=s3_client,
    threads=20,
    download_queue=download_queue,
    completed_queue=completed_queue,
    exit_event=exit_event,
    bucket="my-bucket",
    prefix="large-dataset/",
    download_dir="./downloads",
    delimiter="/",
    download_config=download_config,
    progress_tracker=tracker
)

print("\nDownload complete!")
```

The ProgressTracker provides these statistics:
- `objects_found`: Number of objects discovered during listing
- `objects_downloaded`: Number of objects successfully downloaded
- `bytes_downloaded`: Total bytes downloaded
- `elapsed_time`: Time elapsed since tracking started
- `download_speed_mbps`: Current download speed in MB/s

Note: The progress tracker is thread-safe and can be safely accessed from multiple threads.

### Advanced Usage

```python
import boto3
import threading
from s3fetch.api import list_objects, download_objects
from s3fetch.s3 import S3FetchQueue, create_download_config

# Use custom boto3 session for credentials
session = boto3.Session(profile_name="production")
s3_client = session.client("s3", region_name="us-west-2")

# Use custom client with s3fetch API functions
download_queue = S3FetchQueue()
completed_queue = S3FetchQueue()
exit_event = threading.Event()

list_objects(
    bucket="my-bucket",
    prefix="data/",
    client=s3_client,
    download_queue=download_queue,
    delimiter="/",
    regex=None,
    exit_event=exit_event
)

download_config = create_download_config()
success_count, failed_downloads = download_objects(
    client=s3_client,
    threads=10,
    download_queue=download_queue,
    completed_queue=completed_queue,
    exit_event=exit_event,
    bucket="my-bucket",
    prefix="data/",
    download_dir="./data",
    delimiter="/",
    download_config=download_config
)
```

## Troubleshooting

### MacOS hangs when downloading using high number of threads

From my testing this is caused by Spotlight on MacOS trying to index a large number of files at once.

You can exclude the directory you're using to store your downloads via the Spotlight system preference control panel.
