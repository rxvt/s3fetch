"""This module contains functions to interact with AWS S3."""

import logging
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from queue import Queue
from typing import Callable, Generator, Optional, Tuple

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from mypy_boto3_s3.client import S3Client

from s3fetch.exceptions import (
    InvalidCredentialsError,
    PermissionError,
    PrefixDoesNotExistError,
    RegexError,
)

from . import fs
from .exceptions import S3FetchQueueClosed, S3FetchQueueEmpty

logger = logging.getLogger(__name__)

# Would be nice to be able to pull this from `s3transfer` package but
# the default of 10 is specified as a parameter default on the TransferConfig class.
DEFAULT_S3TRANSFER_CONCURRENCY = 10


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
            str: S3 object key.
        """
        key = self.queue.get(block=block)
        if key is None:
            raise S3FetchQueueClosed
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


def get_completion_queue() -> S3FetchQueue:
    """Returns a completion queue.

    Returns:
        S3FetchQueue: FIFO completion queue.
    """
    queue = S3FetchQueue()
    return queue


def create_list_objects_thread(
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


def create_download_threads(
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
    dry_run: bool = False,
) -> Tuple[int, list]:
    """Create download threads.

    Args:
        client (S3Client): S3 client object.
        threads (int): Number of threads to use for downloading objects.
        download_queue (S3FetchQueue): Download queue.
        completed_queue (S3FetchQueue): Completed download queue.
        exit_event (threading.Event): Notify the script to exit.
        bucket (str): S3 bucket name, e.g. `my-bucket`.
        prefix (str): S3 object key prefix, e.g. `my/test/objects/`.
        download_dir (Path): Download directory, e.g. `~/Downloads`.
        delimiter (str): S3 object key delimiter, e.g. `/`.
        download_config (dict): Download configuration.
        dry_run (bool): Run in dry run mode.

    Returns:
        Tuple[int, list]: _description_
    """
    successful_downloads = 0
    failed_downloads: list[str] = []
    with ThreadPoolExecutor(max_workers=threads) as executor:
        try:
            futures = {}
            while True:
                try:
                    key = download_queue.get(block=True)
                    futures[key] = executor.submit(
                        download,
                        client=client,
                        bucket=bucket,
                        key=key,
                        exit_event=exit_event,
                        delimiter=delimiter,
                        prefix=prefix,
                        download_dir=download_dir,
                        download_config=download_config,
                        completed_queue=completed_queue,
                        dry_run=dry_run,
                    )
                except S3FetchQueueEmpty:
                    break

            successful_downloads, failed_downloads = generate_stats(futures)
        except KeyboardInterrupt:
            exit_event.set()
            shutdown_download_threads(executor)
            raise
    return successful_downloads, failed_downloads


def generate_stats(futures: dict) -> Tuple[int, list]:
    """Generate download statistics.

    Args:
        futures (dict): Dictionary containing the futures objects.

    Returns:
        Tuple[int, list]: Tuple containing the number of successful downloads and a list
            of failed downloads.
    """
    successful_downloads = 0
    failed_downloads = []
    for key, future in futures.items():
        try:
            future.result()
            successful_downloads += 1
        except Exception as e:
            failed_downloads.append((key, e))
    return successful_downloads, failed_downloads


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


def process_key(key: str, delimiter: str, prefix: str) -> Tuple[Optional[str], str]:
    """Process the object key.

    Rollup the object key to the nearest delimiter by the prefix and then split the
    object key into directory and file.

    Args:
        key (str): S3 object key, e.g. `my/test/objects/one/mytestobject/one`.
        delimiter (str): Object key delimiter, e.g. `/`.
        prefix (str): Object key prefix, .e.g. `my/test/objects/`.

    Returns:
        Tuple[Optional[str], str]: Tuple containing the directory and file.
    """
    tmp_key = rollup_object_key_by_prefix(key=key, delimiter=delimiter, prefix=prefix)

    dst_dir, dst_file = split_object_key_into_dir_and_file(tmp_key, delimiter)
    return dst_dir, dst_file


def rollup_object_key_by_prefix(prefix: str, delimiter: str, key: str) -> str:
    """Rollup the object key to the nearest delimiter by the prefix.

    For example, if the prefix is `my/test/objects/` and the delimiter is `/` and the
    object key is `my/test/objects/one/mytestobject/one` then the object key will be
    rolled up to `one/mytestobject/one`.

    Args:
        prefix (str): Object key prefix.
        delimiter (str): Object key delimiter.
        key (str): Object key.

    Raises:
        PrefixDoesNotExistError: Raised when the prefix does not exist in the object
            key.

    Returns:
        str: Object key rolled up to the nearest delimiter by the prefix.
    """
    if not key.startswith(prefix):
        raise PrefixDoesNotExistError("Prefix not found in key")

    if prefix == "":
        return key

    # TODO: Can this be made simpler?
    delimiter_count = prefix.count(delimiter)
    tmp_key = key.split(delimiter, maxsplit=delimiter_count)[-1]
    return tmp_key


def split_object_key_into_dir_and_file(
    key: str, delimiter: str
) -> Tuple[Optional[str], str]:
    """Split the object key into directory and file.

    Args:
        key (str): Object key.
        delimiter (str): Object key delimiter.

    Returns:
        tuple: Tuple containing the directory and file. If the delimiter is not found
            in the object key then the directory will be None and
            the file will be the entire object key.
    """
    if delimiter not in key:
        return "", key
    directory, file = key.rsplit(delimiter, maxsplit=1)
    return directory, file


def create_s3_transfer_config(
    use_threads: bool = True,
    max_concurrency: int = 10,
) -> TransferConfig:  # type: ignore
    """Create a boto3.s3.transfer.TransferConfig object.

    Args:
        use_threads (bool, optional): Use threads. Defaults to True.
        max_concurrency (int, optional): How many threads should be used _per object_
            download. Defaults to 10.

    Returns:
        boto3.s3.transfer.TransferConfig: TransferConfig object.
    """
    s3transfer_config = boto3.s3.transfer.TransferConfig(  # type: ignore
        use_threads=use_threads,
        max_concurrency=max_concurrency,
    )
    return s3transfer_config


def create_download_config(callback: Optional[Callable] = None) -> dict:
    """Create a download configuration.

    Args:
        callback (Optional[Callable]): Callback function.

    Returns:
        dict: Download configuration.
    """
    extra_kwargs = {}

    transfer_config = create_s3_transfer_config(
        use_threads=True,
        max_concurrency=DEFAULT_S3TRANSFER_CONCURRENCY,
    )

    if callback:
        extra_kwargs["Callback"] = callback
    if transfer_config:
        extra_kwargs["Config"] = transfer_config
    return extra_kwargs


def download_object(
    key: str,
    dest_filename: Path,
    client: S3Client,
    bucket: str,
    download_config: dict,
    completed_queue: S3FetchQueue,
    dry_run: bool = False,
) -> None:
    """Download an object from S3.

    Args:
        key (str): S3 object key, e.g. `my/test/objects/one/mytestobject/one`.
        dest_filename (Path): Absolute local destination filename, e.g. `/tmp/myfile`.
        client (S3Client): S3 client object, e.g. `boto3.client("s3")`.
        bucket (str): S3 bucket name, e.g. `my-bucket`.
        download_config (dict): Download configuration.
        completed_queue (S3FetchQueue): Completed download queue.
        dry_run (bool): Run in dry run mode.

    Raises:
        PermissionError: Raised when there is a permission error.
    """
    try:
        if not dry_run:
            client.download_file(
                Bucket=bucket,
                Key=key,
                Filename=str(dest_filename),
                **download_config,
            )
    except PermissionError as e:
        raise PermissionError(
            f"Permission error when attempting to write object to {dest_filename}"
        ) from e
    else:
        logger.debug(f"Downloaded {key} to {dest_filename}")
        completed_queue.put(key)


def download(
    client: S3Client,
    bucket: str,
    key: str,
    exit_event: threading.Event,
    delimiter: str,
    prefix: str,
    download_dir: Path,
    download_config: dict,
    completed_queue: S3FetchQueue,
    dry_run: bool = False,
) -> None:
    """Download an object from S3.

    Args:
        client (S3Client): boto3.S3Client object.
        bucket (str): S3 bucket name.
        key (str): S3 object key.
        dest_filename (str): Absolute local destination filename.
        exit_event (threading.Event): Notify that script to exit.
        config (Optional[TransferConfig], optional): S3 TransferConfig object.
        download_dir (Path): Download directory, e.g. `~/Downloads`.
        delimiter (str): S3 object key delimiter, e.g. `/`.
        prefix (str): S3 object key prefix, e.g. `my/test/objects/`.
        download_config (dict): Download configuration.
        completed_queue (S3FetchQueue): Completed download queue.
        dry_run (bool): Run in dry run mode.

    Raises:
        PermissionError: Raised when there is a permission error.
    """
    if exit_event.is_set():
        logger.debug("Not downloading %s as exit_event is set", key)
        return

    dst_dir, dst_file = process_key(key=key, prefix=prefix, delimiter=delimiter)

    absolute_dest_dir = fs.create_destination_directory(
        download_dir=download_dir,
        object_dir=dst_dir,
        delimiter=delimiter,
    )

    dest_filename = absolute_dest_dir / dst_file

    logger.debug(f"Downloading s3://{bucket}{delimiter}{key}")

    download_object(
        key=key,
        dest_filename=dest_filename,
        client=client,
        bucket=bucket,
        download_config=download_config,
        completed_queue=completed_queue,
        dry_run=dry_run,
    )


def trim_schema_from_uri(uri: str) -> str:
    """Trim the schema from the URI.

    s3://my-bucket/my/object -> my-bucket/my/object

    Args:
        uri (str): S3 URI.

    Returns:
        str: URI without the schema.
    """
    return uri.replace("s3://", "", 1)


def split_uri_into_bucket_and_prefix(s3_uri: str, delimiter: str) -> Tuple[str, str]:
    """Parse and split the S3 URI into bucket and path prefix.

    :param s3_uri: S3 URI
    :type s3_uri: str
    :param delimiter: S3 path delimiter.
    :type delimiter: str
    :return: Tuple containing the S3 bucket and path prefix.
    :rtype: Tuple[str, str]
    """
    tmp_path = trim_schema_from_uri(s3_uri)
    try:
        bucket, prefix = tmp_path.split(delimiter, maxsplit=1)
    except ValueError:
        bucket = tmp_path
        prefix = ""
    logger.debug(f"Split S3 URI into bucket={bucket}, prefix={prefix}")
    return bucket, prefix
