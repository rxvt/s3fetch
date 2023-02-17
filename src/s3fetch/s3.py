import logging
import re
import threading
from queue import Queue
from typing import Generator, Optional

from botocore.exceptions import NoCredentialsError
from mypy_boto3_s3.client import S3Client

from s3fetch.exceptions import InvalidCredentialsError, RegexError

from .exceptions import S3FetchQueueEmpty

logger = logging.getLogger(__name__)


class S3FetchQueue:
    """Wrapper around a standard Python FIFO queue."""

    def __init__(self):
        self.queue = Queue()

    def put(self, key: Optional[str]) -> None:
        self.queue.put_nowait(key)

    # TODO: Remove this after refactor, should only use self.put()
    def put_nowait(self, key: Optional[str]) -> None:
        self.queue.put_nowait(key)

    def get(self, block: bool = False) -> str:
        key = self.queue.get(block=block)
        if key is None:
            raise S3FetchQueueEmpty  # TODO: Change this to S3FetchQueueClosed
        return key

    def close(self) -> None:
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
) -> None:
    """Starts a seperate thread that lists of objects from the specified S3 bucket
    and prefix, filters the object list and adds the valid objects to the download
    queue.

    Args:
        bucket (str): S3 bucket name.
        prefix (str): S3 object key prefix.
        client (S3Client): Boto3 S3 client object.
        download_queue (S3FetchQueue): FIFO download queue.
        delimiter (str): Delimiter for the logical folder hierarchy.
        regex (Optional[str]): Regular expression to use for filtering objects.
    """
    # Start a single thread to list the objects
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
        },
    ).start()


def list_objects(
    client: S3Client,
    queue: S3FetchQueue,
    bucket: str,
    prefix: str,
    delimiter: str,
    regex: Optional[str],
) -> None:
    """List objects in an S3 bucket prefixed by `prefix`.

    Args:
        client (S3Client): boto3.S3Client object.
        queue (S3FetchQueue): FIFO download queue.
        bucket (str): S3 bucket name.
        prefix (str): Download objects starting with this prefix.
        delimiter (str): Delimiter for the logical folder hierarchy.
        regex (Optional[str]): Regular expression to use for filtering objects.

    Raises:
        InvalidCredentialsError: Raised if AWS credentials are invalid.
    """
    try:
        for obj_key in paginate_objects(client=client, bucket=bucket, prefix=prefix):
            if not filter_object(obj_key, delimiter, regex):
                continue
            add_object_to_download_queue(obj_key, queue)
        close_download_queue(queue)
    except NoCredentialsError as e:
        raise InvalidCredentialsError(e) from e
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


def filter_object(key: str, delimiter: str, regex: Optional[str]) -> bool:
    """Determines if an S3 object should be downloaded.

    This is a wrapper for checking if the object key is a 'directory' or
    if the object matches a provided regular expression.

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
        return False
    if regex and not filter_by_regex(key=key, regex=regex):
        return False
    return True


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
    """Filter objects by regular expression. If an object matches the regex then it is
    included in the list of objects to download.

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
