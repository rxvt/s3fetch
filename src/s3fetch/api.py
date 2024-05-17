"""API for S3Fetch."""

import threading
from typing import Optional

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
