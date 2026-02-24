"""This module contains functions to interact with AWS S3."""

import logging
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from queue import Queue
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    Generic,
    Optional,
    Pattern,
    Tuple,
    TypeVar,
)

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
from .exceptions import S3FetchQueueClosed

logger = logging.getLogger(__name__)

# Would be nice to be able to pull this from `s3transfer` package but
# the default of 10 is specified as a parameter default on the TransferConfig class.
DEFAULT_S3TRANSFER_CONCURRENCY = 10

T = TypeVar("T")


@dataclass
class DownloadResult:
    """The result of a single S3 object download attempt.

    Instances of this class are placed on the completed queue after every
    download attempt — whether it succeeded or failed — giving consumers
    real-time, per-object notification with rich context.

    Attributes:
        key (str): The S3 object key that was downloaded.
        dest_filename (Path): The absolute local path where the object was
            (or would have been) written.
        success (bool): True if the download completed successfully.
        file_size (int): Size of the downloaded file in bytes.  0 on failure
            or in dry-run mode.
        error (Optional[Exception]): The exception that caused the download to
            fail, or None on success.
    """

    key: str
    dest_filename: Path
    success: bool
    file_size: int = 0
    error: Optional[Exception] = field(default=None, repr=False)


class S3FetchQueue(Generic[T]):
    """Generic wrapper around a standard Python FIFO queue.

    Type parameter ``T`` is the item type stored in the queue.  The sentinel
    value used to signal queue closure is always ``None`` (never a valid item).

    Examples:
        Download queue (keys only)::

            q: S3FetchQueue[str] = S3FetchQueue()

        Completed-downloads queue (rich result objects)::

            q: S3FetchQueue[DownloadResult] = S3FetchQueue()
    """

    def __init__(self) -> None:  # noqa: D107
        self.queue: Queue[Optional[T]] = Queue()

    def put(self, item: T) -> None:
        """Add an item to the queue.

        Args:
            item (T): The item to enqueue.
        """
        self.queue.put_nowait(item)

    def get(self, block: bool = False) -> T:
        """Remove and return the next item from the queue.

        Args:
            block (bool, optional): Block until an item is available.
                Defaults to False.

        Raises:
            S3FetchQueueClosed: Raised when the sentinel ``None`` is received,
                indicating the queue has been closed by the producer.

        Returns:
            T: The next item from the queue.
        """
        item = self.queue.get(block=block)
        if item is None:
            raise S3FetchQueueClosed
        return item

    def close(self) -> None:
        """Close the queue by placing a sentinel ``None`` onto it.

        After calling this method the consumer will raise
        :class:`S3FetchQueueClosed` the next time it calls :meth:`get`.
        """
        self.queue.put(None)


def get_queue(queue_type: str) -> "S3FetchQueue[Any]":
    """Factory function to create a download or completion queue.

    Args:
        queue_type (str): Type of queue to create. Must be 'download' or
            'completion'.

    Returns:
        S3FetchQueue: FIFO queue instance.

    Raises:
        ValueError: If queue_type is not 'download' or 'completion'.
    """
    if queue_type not in ("download", "completion"):
        raise ValueError("queue_type must be 'download' or 'completion'")
    return S3FetchQueue()


def create_list_objects_thread(
    bucket: str,
    prefix: str,
    client: S3Client,
    download_queue: "S3FetchQueue[str]",
    delimiter: str,
    regex: Optional[str],
    exit_event: threading.Event,
    progress_tracker: Optional[Any] = None,  # noqa: ANN401
) -> threading.Thread:
    """Starts a seperate thread that lists of objects from the specified S3 bucket.

    Starts a seperate thread that lists of objects from the specified S3 bucket
    and prefix, filters the object list and adds the valid objects to the download
    queue.

    Args:
        bucket (str): S3 bucket name.
        prefix (str): S3 object key prefix.
        client (S3Client): Boto3 S3 client object.
        download_queue (S3FetchQueue[str]): FIFO download queue.
        delimiter (str): Delimiter for the logical folder hierarchy.
        regex (Optional[str]): Regular expression to use for filtering objects.
        exit_event (threading.Event): Notify that script to exit.
        progress_tracker (Optional[Any]): Progress tracker instance.

    Returns:
       threading.Thread: Thread that lists the objects in the bucket.
    """
    list_objects_thread = threading.Thread(
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
            "progress_tracker": progress_tracker,
        },
    )
    return list_objects_thread


def create_download_threads(
    client: S3Client,
    threads: int,
    download_queue: "S3FetchQueue[str]",
    completed_queue: "S3FetchQueue[DownloadResult]",
    exit_event: threading.Event,
    bucket: str,
    prefix: str,
    download_dir: Path,
    delimiter: str,
    download_config: dict,
    dry_run: bool = False,
    progress_tracker: Optional[Any] = None,  # noqa: ANN401
) -> Tuple[int, list]:
    """Create download threads.

    Args:
        client (S3Client): S3 client object.
        threads (int): Number of threads to use for downloading objects.
        download_queue (S3FetchQueue[str]): Download queue.
        completed_queue (S3FetchQueue[DownloadResult]): Completed download queue.
        exit_event (threading.Event): Notify the script to exit.
        bucket (str): S3 bucket name, e.g. `my-bucket`.
        prefix (str): S3 object key prefix, e.g. `my/test/objects/`.
        download_dir (Path): Download directory, e.g. `~/Downloads`.
        delimiter (str): S3 object key delimiter, e.g. `/`.
        download_config (dict): Download configuration.
        dry_run (bool): Run in dry run mode.
        progress_tracker (Optional[Any]): Progress tracker instance.

    Returns:
        Tuple[int, list]: Number of successful downloads and list of failed
            downloads as ``(key, exception)`` tuples.
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
                        progress_tracker=progress_tracker,
                    )
                except S3FetchQueueClosed:
                    break
            successful_downloads, failed_downloads = generate_stats(futures)
            completed_queue.close()
        except KeyboardInterrupt:
            exit_event.set()
            completed_queue.close()
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
    queue: "S3FetchQueue[str]",
    bucket: str,
    prefix: str,
    delimiter: str,
    regex: Optional[str],
    exit_event: threading.Event,
    progress_tracker: Optional[Any] = None,  # noqa: ANN401
) -> None:
    """List objects in an S3 bucket prefixed by `prefix`.

    Args:
        client (S3Client): boto3.S3Client object.
        queue (S3FetchQueue[str]): FIFO download queue.
        bucket (str): S3 bucket name.
        prefix (str): Download objects starting with this prefix.
        delimiter (str): Delimiter for the logical folder hierarchy.
        regex (Optional[str]): Regular expression to use for filtering objects.
        exit_event (threading.Event): Notify that script to exit.
        progress_tracker (Optional[Any]): Progress tracker instance.

    Raises:
        InvalidCredentialsError: Raised if AWS credentials are invalid.
        RegexError: Raised if the regex cannot be compiled.
    """
    compiled_regex: Optional[Pattern] = None
    if regex:
        try:
            compiled_regex = re.compile(rf"{regex}")
        except re.error as e:
            raise RegexError from e

    try:
        for obj_key in paginate_objects(client=client, bucket=bucket, prefix=prefix):
            if exit_requested(exit_event):
                logger.debug(
                    "Not adding %s to download queue as exit_event is set", obj_key
                )
                raise SystemExit
            if exclude_object(obj_key, delimiter, compiled_regex):
                continue
            add_object_to_download_queue(obj_key, queue, progress_tracker)
        close_download_queue(queue)
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code in (
            "InvalidAccessKeyId",
            "SignatureDoesNotMatch",
            "InvalidUserID.NotFound",
        ):
            raise InvalidCredentialsError(
                f"Invalid AWS credentials: {error_code}"
            ) from e
        elif error_code == "TokenRefreshRequired":
            raise InvalidCredentialsError(
                "SSO token has expired and needs to be refreshed"
            ) from e
        elif error_code in ("AccessDenied", "UnauthorizedOperation"):
            raise PermissionError(f"Access denied: {error_code}") from e
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


def exclude_object(key: str, delimiter: str, regex: Optional[Pattern]) -> bool:
    """Determines if an S3 object should be added to the download queue.

    This is a wrapper for checking if the object key should be added to the download
    queue. Reasons for excluding an object could be it's a 'directory' or if the object
    does not match a provided regular expression.

    Args:
        key (str): S3 object key.
        delimiter (str): Object key 'folder' delimiter.
        regex (Optional[Pattern]): Compiled Python regular expression, or None.

    Returns:
        bool: Returns True if the object should be excluded (not downloaded), False
        otherwise.
    """
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


def filter_by_regex(key: str, regex: Pattern) -> bool:
    """Filter objects by compiled regular expression.

    If an object matches the regex then it is included in the list of objects to
    download.

    Args:
        key (str): S3 object key.
        regex (Pattern): Compiled Python regular expression.

    Returns:
        bool: True if the regex matches the object key, False otherwise.
    """
    if regex.search(key):
        return True
    return False


def add_object_to_download_queue(
    key: str,
    queue: "S3FetchQueue[str]",
    progress_tracker: Optional[Any] = None,  # noqa: ANN401
) -> None:
    """Add S3 object to download queue.

    Args:
        key (str): S3 object key.
        queue (S3FetchQueue[str]): FIFO download queue.
        progress_tracker (Optional[Any]): Progress tracker instance.
    """
    queue.put(key)
    if progress_tracker is not None:
        progress_tracker.increment_found()
    logger.debug("Added %s to download queue", key)


def close_download_queue(queue: "S3FetchQueue[str]") -> None:
    """Add sentinel None message onto the download queue.

    Indicates all objects have been listed and added to the download queue.

    Args:
        queue (S3FetchQueue[str]): FIFO download queue.
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


def process_key(key: str, delimiter: str, prefix: str) -> Tuple[str, str]:
    """Process the object key.

    Rollup the object key to the nearest delimiter by the prefix and then split the
    object key into directory and file.

    Args:
        key (str): S3 object key, e.g. `my/test/objects/one/mytestobject/one`.
        delimiter (str): Object key delimiter, e.g. `/`.
        prefix (str): Object key prefix, .e.g. `my/test/objects/`.

    Returns:
        Tuple[str, str]: Tuple containing the directory and file.
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

    # Count how many delimiter-separated segments are in the prefix, then split
    # the key by that many delimiters and take the tail.  This strips complete
    # path segments rather than an arbitrary byte-for-byte prefix, so a prefix
    # like "my/test/pre" (2 delimiters) correctly yields "prefix/file" from the
    # key "my/test/prefix/file" rather than stripping mid-segment to "fix/file".
    delimiter_count = prefix.count(delimiter)
    return key.split(delimiter, maxsplit=delimiter_count)[-1]


def split_object_key_into_dir_and_file(key: str, delimiter: str) -> Tuple[str, str]:
    """Split the object key into directory and file.

    Args:
        key (str): Object key.
        delimiter (str): Object key delimiter.

    Returns:
        tuple: Tuple containing the directory and file. If the delimiter is not found
            in the object key then the directory will be an empty string and
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


def create_download_config(callback: Optional[Callable] = None) -> Dict[str, Any]:
    """Create a download configuration.

    Args:
        callback (Optional[Callable]): Callback function.

    Returns:
        Dict[str, Any]: Download configuration.
    """
    extra_kwargs: Dict[str, Any] = {}

    transfer_config = create_s3_transfer_config(
        use_threads=True,
        max_concurrency=DEFAULT_S3TRANSFER_CONCURRENCY,
    )

    if callback:
        extra_kwargs["Callback"] = callback

    extra_kwargs["Config"] = transfer_config
    return extra_kwargs


def _get_file_size(path: Path) -> int:
    """Return the size of a file in bytes, or 0 if the stat call fails.

    Args:
        path (Path): Path to the file.

    Returns:
        int: File size in bytes, or 0 on error.
    """
    try:
        return path.stat().st_size
    except OSError:
        logger.warning(f"Could not get file size for {path}")
        return 0


def _raise_download_client_error(e: ClientError, dest_filename: Path) -> None:
    """Translate a ClientError from a download into a more specific exception.

    Args:
        e (ClientError): The ClientError to translate.
        dest_filename (Path): The destination filename (used in error messages).

    Raises:
        InvalidCredentialsError: For credential-related errors.
        PermissionError: For access-denied errors.
        ClientError: Re-raised for any other unrecognised error codes.
    """
    error_code = e.response.get("Error", {}).get("Code")
    if error_code in (
        "InvalidAccessKeyId",
        "SignatureDoesNotMatch",
        "InvalidUserID.NotFound",
    ):
        raise InvalidCredentialsError(
            f"Invalid AWS credentials during download: {error_code}"
        ) from e
    elif error_code == "TokenRefreshRequired":
        raise InvalidCredentialsError("SSO token has expired during download") from e
    elif error_code in ("AccessDenied", "UnauthorizedOperation"):
        raise PermissionError(f"Access denied during download: {error_code}") from e
    else:
        raise e


def download_object(
    key: str,
    dest_filename: Path,
    client: S3Client,
    bucket: str,
    download_config: dict,
    completed_queue: "S3FetchQueue[DownloadResult]",
    dry_run: bool = False,
    progress_tracker: Optional[Any] = None,  # noqa: ANN401
) -> None:
    """Download an object from S3.

    On completion — whether success or failure — a :class:`DownloadResult` is
    placed on ``completed_queue`` so that consumers receive real-time,
    per-object notifications.

    Args:
        key (str): S3 object key, e.g. `my/test/objects/one/mytestobject/one`.
        dest_filename (Path): Absolute local destination filename, e.g. `/tmp/myfile`.
        client (S3Client): S3 client object, e.g. `boto3.client("s3")`.
        bucket (str): S3 bucket name, e.g. `my-bucket`.
        download_config (dict): Download configuration.
        completed_queue (S3FetchQueue[DownloadResult]): Completed download queue.
        dry_run (bool): Run in dry run mode.
        progress_tracker (Optional[Any]): Progress tracker instance.

    Raises:
        InvalidCredentialsError: Raised when AWS credentials are invalid.
        PermissionError: Raised when there is a permission error.
    """
    # Download to a temp file in the same directory, then rename atomically.
    # This ensures no partial/corrupted file is left on disk if the download
    # is interrupted by a network error, Ctrl+C, or disk full.
    tmp_filename = dest_filename.with_suffix(dest_filename.suffix + ".s3fetch_tmp")
    try:
        if not dry_run:
            client.download_file(
                Bucket=bucket,
                Key=key,
                Filename=str(tmp_filename),
                **download_config,
            )
            tmp_filename.replace(dest_filename)
    except ClientError as e:
        tmp_filename.unlink(missing_ok=True)
        completed_queue.put(
            DownloadResult(key=key, dest_filename=dest_filename, success=False, error=e)
        )
        _raise_download_client_error(e, dest_filename)
    except PermissionError as e:
        tmp_filename.unlink(missing_ok=True)
        completed_queue.put(
            DownloadResult(key=key, dest_filename=dest_filename, success=False, error=e)
        )
        raise PermissionError(
            f"Permission error when attempting to write object to {dest_filename}"
        ) from e
    except OSError as e:
        tmp_filename.unlink(missing_ok=True)
        completed_queue.put(
            DownloadResult(key=key, dest_filename=dest_filename, success=False, error=e)
        )
        raise OSError(
            f"I/O error writing '{dest_filename}': {e.strerror} (errno {e.errno})"
        ) from e
    except Exception as e:
        tmp_filename.unlink(missing_ok=True)
        completed_queue.put(
            DownloadResult(key=key, dest_filename=dest_filename, success=False, error=e)
        )
        raise
    else:
        logger.debug(f"Downloaded {key} to {dest_filename}")
        file_size = _get_file_size(dest_filename) if not dry_run else 0
        if progress_tracker is not None and not dry_run:
            progress_tracker.increment_downloaded(file_size)
        completed_queue.put(
            DownloadResult(
                key=key,
                dest_filename=dest_filename,
                success=True,
                file_size=file_size,
            )
        )


def download(
    client: S3Client,
    bucket: str,
    key: str,
    exit_event: threading.Event,
    delimiter: str,
    prefix: str,
    download_dir: Path,
    download_config: dict,
    completed_queue: "S3FetchQueue[DownloadResult]",
    dry_run: bool = False,
    progress_tracker: Optional[Any] = None,  # noqa: ANN401
) -> None:
    """Download an object from S3.

    Args:
        client (S3Client): boto3.S3Client object.
        bucket (str): S3 bucket name.
        key (str): S3 object key.
        exit_event (threading.Event): Notify that script to exit.
        download_dir (Path): Download directory, e.g. `~/Downloads`.
        delimiter (str): S3 object key delimiter, e.g. `/`.
        prefix (str): S3 object key prefix, e.g. `my/test/objects/`.
        download_config (dict): Download configuration.
        completed_queue (S3FetchQueue[DownloadResult]): Completed download queue.
        dry_run (bool): Run in dry run mode.
        progress_tracker (Optional[Any]): Progress tracker instance.

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
        dry_run=dry_run,
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
        progress_tracker=progress_tracker,
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
