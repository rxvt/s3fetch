"""S3 functions and classes."""
import logging
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from typing import Generator, Optional

from botocore.exceptions import ClientError
from mypy_boto3_s3.client import S3Client

from s3fetch.exceptions import InvalidCredentialsError, PermissionError, RegexError

from .exceptions import S3FetchQueueEmpty

logger = logging.getLogger(__name__)


class S3FetchQueue:
    """Wrapper around a standard Python FIFO queue."""

    def __init__(self):  # noqa: D107
        self.queue = Queue()

    def put(self, key: Optional[str]) -> None:
        """Add object key to the download queue."""
        self.queue.put_nowait(key)

    def get(self, block: bool = False) -> str:
        """Get object key from the download queue.

        Args:
            block (bool, optional): Block until an item if available. Defaults to False.

        Raises:
            S3FetchQueueEmpty: Raised when the queue is empty.

        Returns:
            str: _description_
        """
        key = self.queue.get(block=block)
        if key is None:
            raise S3FetchQueueEmpty  # TODO: Change this to S3FetchQueueClosed
        return key

    def close(self) -> None:
        """Close queue by adding a sentinel message of None onto the download queue."""
        self.queue.put(None)


def get_download_queue() -> S3FetchQueue:
    """Returns the download queue.

    Returns:
        S3FetchQueue: FIFO download queue.
    """
    queue = S3FetchQueue()
    return queue


def start_listing_objects(
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
    threading.Thread(
        name="list_objects",
        target=list_objects,
        kwargs={
            "bucket": bucket,
            "prefix": prefix,
            "client": client,
            "queue": download_queue,
            "delimiter": delimiter,
            "regex": regex,
            "exit_event": exit_event,
        },
    ).start()


def list_objects(
    client: S3Client,
    queue: S3FetchQueue,
    bucket: str,
    prefix: str,
    delimiter: str,
    regex: Optional[str],
    exit_event: threading.Event,
) -> None:
    """List objects in an S3 bucket prefixed by `prefix`.

    Args:
        client (S3Client): boto3.S3Client object.
        queue (S3FetchQueue): FIFO download queue.
        bucket (str): S3 bucket name.
        prefix (str): Download objects starting with this prefix.
        delimiter (str): Delimiter for the logical folder hierarchy.
        regex (Optional[str]): Regular expression to use for filtering objects.
        exit_event (threading.Event): Notify that script to exit.

    Raises:
        InvalidCredentialsError: Raised if AWS credentials are invalid.
    """
    try:
        for obj_key in paginate_objects(client=client, bucket=bucket, prefix=prefix):
            if exit_requested(exit_event):
                logger.debug(
                    "Not adding %s to download queue as exit_event is set", obj_key
                )
                raise SystemExit
            if exclude_object(obj_key, delimiter, regex):
                continue
            add_object_to_download_queue(obj_key, queue)
        close_download_queue(queue)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "InvalidAccessKeyId":
            raise InvalidCredentialsError(e) from e
        elif e.response.get("Error", {}).get("Code") == "AccessDenied":
            raise PermissionError(e) from e
        else:
            raise e
    logger.debug("Finished adding objects to download queue")


def paginate_objects(client: S3Client, bucket: str, prefix: str = "") -> Generator:
    """Paginate over each page of listing results, yielding each object key.

    Args:
        client (S3Client): boto3.S3Client object.
        bucket (str): S3 bucket name.
        prefix (str): List objects under this prefix. Defaults to empty string.

    Yields:
        Generator: Yields each individual S3 object key as it is listed, followed by
            a sentinel None value when all objects have been listed.
    """
    paginator = client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj.get("Key")


def exit_requested(exit_event: threading.Event) -> bool:
    """Check if the exit_event has been set.

    Args:
        exit_event (threading.Event): Threading.Event object.

    Returns:
        bool: True if the exit_event has been set, False otherwise.
    """
    if exit_event.is_set():
        return True
    return False


def exclude_object(key: str, delimiter: str, regex: Optional[str]) -> bool:
    """Determines if an S3 object should be added to the download queue.

    This is a wrapper for checking if the object key should be added to the download
    queue. Reasons for excluding an object could be it's a 'directory' or if the object
    matches a provided regular expression.

    Args:
        key (str): S3 object key.
        delimiter (str): Object key 'folder' delimiter.
        regex (Optional[str]): Python compatible regular expression.

    Returns:
        bool: Returns True if the object should be downloaded, False otherwise.
    """
    # If true then object is included in results
    if check_if_key_is_directory(key=key, delimiter=delimiter):
        logger.debug("Excluded directory %s from results", key)
        return True
    if regex and not filter_by_regex(key=key, regex=regex):
        return True
    return False


def check_if_key_is_directory(key: str, delimiter: str) -> bool:
    """Check if an S3 object key is a 'directory' in the logical hierarchy.

    Args:
        key (str): S3 object key.
        delimiter (str): Object key 'folder' delimiter.

    Returns:
        bool: Returns True if object key is a 'directory', False otherwise.
    """
    if key.endswith(delimiter):
        logger.debug("Detected key %s is a directory.", key)
        return True
    return False


def filter_by_regex(key: str, regex: str) -> bool:
    """Filter objects by regular expression.

    If an object matches the regex then it is included in the list of objects to
    download.

    Args:
        key (str): S3 object key.
        regex (str): Python compatible regular expression.

    Raises:
        RegexError: Raised when the regex cannot be compiled due to an error.

    Returns:
        bool: True if the regex matches the object key, False otherwise.
    """
    try:
        rexp = re.compile(rf"{regex}")
    except re.error as e:
        raise RegexError from e

    if rexp.search(key):
        return True
    return False


def add_object_to_download_queue(key: str, queue: S3FetchQueue) -> None:
    """Add S3 object to download queue.

    Args:
        key (str): S3 object key.
        queue (S3FetchQueue): FIFO download queue.
    """
    queue.put(key)
    logger.debug("Added %s to download queue", key)


def close_download_queue(queue: S3FetchQueue) -> None:
    """Add sentinel None message onto the download queue.

    Indicates all objects have been listed and added to the download queue.

    Args:
        queue (S3FetchQueue): FIFO download queue.
    """
    queue.close()
    logger.debug("Added sentinel message to download queue")


def shutdown_download_threads(executor: ThreadPoolExecutor) -> None:
    """Shutdown the download threads.

    Args:
        executor (ThreadPoolExecutor): Executor object.
    """
    logger.debug("Shutting down download threads")
    executor.shutdown(wait=False)
