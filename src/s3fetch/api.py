"""Public API for S3Fetch."""

import threading
from pathlib import Path
from typing import Callable, Optional, Tuple

from mypy_boto3_s3.client import S3Client

from . import s3
from .s3 import S3FetchQueue


def list_objects(
    bucket: str,
    prefix: str,
    client: S3Client,
    download_queue: S3FetchQueue,
    delimiter: str,
    regex: Optional[str],
    exit_event: threading.Event,
) -> None:
    """Starts a seperate thread that lists of objects from the specified S3 bucket.

    Starts a seperate thread that lists of objects from the specified S3 bucket
    and prefix, filters the object list and adds the valid objects to the download
    queue.

    Args:
        bucket (str): S3 bucket name.
        prefix (str): S3 object key prefix.
        client (S3Client): Boto3 S3 client object.
        download_queue (S3FetchQueue): FIFO download queue.
        delimiter (str): Delimiter for the logical folder hierarchy.
        regex (Optional[str]): Regular expression to use for filtering objects.
        exit_event (threading.Event): Notify that script to exit.
    """
    s3.create_list_objects_thread(
        bucket=bucket,
        prefix=prefix,
        client=client,
        download_queue=download_queue,
        delimiter=delimiter,
        regex=regex,
        exit_event=exit_event,
    )


def download_objects(
    client: S3Client,
    threads: int,
    download_queue: S3FetchQueue,
    completed_queue: S3FetchQueue,
    exit_event: threading.Event,
    bucket: str,
    prefix: str,
    download_dir: Path,
    delimiter: str,
    download_config: dict,
    callback: Optional[Callable] = None,
    dry_run: bool = False,
) -> Tuple[int, list]:
    """Download objects from S3 bucket.

    Args:
        client (S3Client): S3 client, e.g. boto3.client("s3").
        threads (int): Number of threads to use.
        download_queue (S3FetchQueue): Download queue.
        completed_queue (S3FetchQueue): Completed download queue.
        exit_event (threading.Event): Notify that script to exit.
        bucket (str): S3 bucket name, e.g. my-bucket.
        prefix (str): S3 object key prefix, e.g. my-folder/.
        download_dir (Path): Destination directory, e.g. /tmp.
        delimiter (str): S3 object key delimiter.
        download_config (dict): Download configuration.
        callback (Callable): Callback function.
        dry_run (bool): Run in dry run mode.

    Returns:
        Tuple[int, list]: Number of successful downloads and list of failed downloads.
    """
    stats = s3.create_download_threads(
        client=client,
        threads=threads,
        download_queue=download_queue,
        completed_queue=completed_queue,
        exit_event=exit_event,
        bucket=bucket,
        prefix=prefix,
        download_dir=download_dir,
        delimiter=delimiter,
        download_config=download_config,
        callback=callback,
        dry_run=dry_run,
    )

    success, failures = stats
    return success, failures
