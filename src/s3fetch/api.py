"""Public API for S3Fetch."""

import logging
import threading
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple, Union

from mypy_boto3_s3.client import S3Client

from . import aws, s3, utils
from .s3 import DownloadResult, S3FetchQueue
from .utils import ProgressProtocol

logger = logging.getLogger(__name__)


def download(
    s3_uri: str,
    download_dir: Union[str, Path] = ".",
    *,
    regex: Optional[str] = None,
    threads: Optional[int] = None,
    region: str = "us-east-1",
    delimiter: str = "/",
    dry_run: bool = False,
    client: Optional[S3Client] = None,
    on_complete: Optional[Callable[[str], None]] = None,
    progress_tracker: Optional[ProgressProtocol] = None,
) -> Tuple[int, list]:
    r"""Download objects from an S3 URI to a local directory.

    Args:
        s3_uri: S3 URI to download from, e.g. ``s3://my-bucket/prefix/``.
        download_dir: Local directory to save downloaded files. Defaults to
            the current working directory.
        regex: Optional regex pattern to filter object keys.
        threads: Number of concurrent download threads. Defaults to CPU count.
        region: AWS region name. Defaults to ``"us-east-1"``. Ignored when a
            custom ``client`` is provided.
        delimiter: S3 key delimiter. Defaults to ``"/"``.
        dry_run: If ``True``, list objects without downloading.
        client: Optional pre-built boto3 S3 client. Created internally using
            ``region`` and the default credential chain if not provided.
        on_complete: Optional callback invoked with the object key each time a
            download completes.
        progress_tracker: Optional progress tracker instance for monitoring
            progress.

    Returns:
        A tuple of ``(success_count, failures)`` where *failures* is a list of
        ``(key, exception)`` pairs for objects that could not be downloaded.

    Example::

        from s3fetch import download

        success, failures = download("s3://my-bucket/prefix/")

    Example with options::

        from s3fetch import download

        success, failures = download(
            "s3://my-bucket/data/",
            download_dir="./out",
            regex=r"\\.csv$",
            threads=20,
        )
    """
    bucket, prefix = s3.split_uri_into_bucket_and_prefix(s3_uri, delimiter)
    download_dir = utils.set_download_dir(
        Path(download_dir) if isinstance(download_dir, str) else download_dir
    )

    if threads is None:
        threads = utils.get_available_threads()

    conn_pool_size = aws.calc_connection_pool_size(
        threads, s3.DEFAULT_S3TRANSFER_CONCURRENCY
    )

    if client is None:
        client = aws.get_client(region, conn_pool_size)

    download_queue: S3FetchQueue[str] = s3.get_queue("download")
    completed_queue: S3FetchQueue[DownloadResult] = s3.get_queue("completion")
    exit_event = utils.create_exit_event()
    download_config = s3.create_download_config(callback=None)

    if on_complete is not None:

        def _on_complete_consumer(queue: S3FetchQueue[DownloadResult]) -> None:
            from .exceptions import S3FetchQueueClosed

            while True:
                try:
                    result = queue.get(block=True)
                    if result.success:
                        on_complete(result.key)
                except S3FetchQueueClosed:
                    break

        _create_completed_objects_thread(
            queue=completed_queue,
            func=_on_complete_consumer,
        )

    _list_objects(
        bucket=bucket,
        prefix=prefix,
        client=client,
        download_queue=download_queue,
        delimiter=delimiter,
        regex=regex,
        exit_event=exit_event,
        progress_tracker=progress_tracker,
    )

    return _download_objects(
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
        dry_run=dry_run,
        progress_tracker=progress_tracker,
    )


def _list_objects(
    bucket: str,
    prefix: str,
    client: S3Client,
    download_queue: "S3FetchQueue[str]",
    delimiter: str,
    regex: Optional[str],
    exit_event: threading.Event,
    progress_tracker: Optional[ProgressProtocol] = None,
) -> None:
    """Start a background thread that lists objects from the specified S3 bucket.

    Starts a separate thread that lists objects from the specified S3 bucket
    and prefix, filters the object list and adds the valid objects to the
    download queue.

    Args:
        bucket (str): S3 bucket name.
        prefix (str): S3 object key prefix.
        client (S3Client): Boto3 S3 client object.
        download_queue (S3FetchQueue[str]): FIFO download queue.
        delimiter (str): Delimiter for the logical folder hierarchy.
        regex (Optional[str]): Regular expression to use for filtering objects.
        exit_event (threading.Event): Notify that script to exit.
        progress_tracker (Optional[ProgressProtocol]): Progress tracker instance.
    """
    list_objects_thread = s3.create_list_objects_thread(
        bucket=bucket,
        prefix=prefix,
        client=client,
        download_queue=download_queue,
        delimiter=delimiter,
        regex=regex,
        exit_event=exit_event,
        progress_tracker=progress_tracker,
    )
    list_objects_thread.start()


def _download_objects(
    client: S3Client,
    threads: int,
    download_queue: "S3FetchQueue[str]",
    completed_queue: "S3FetchQueue[DownloadResult]",
    exit_event: threading.Event,
    bucket: str,
    prefix: str,
    download_dir: Union[str, Path],
    delimiter: str,
    download_config: dict,
    dry_run: bool = False,
    progress_tracker: Optional[ProgressProtocol] = None,
) -> Tuple[int, list]:
    """Download objects from S3 bucket.

    Args:
        client (S3Client): S3 client, e.g. boto3.client("s3").
        threads (int): Number of threads to use.
        download_queue (S3FetchQueue[str]): Download queue.
        completed_queue (S3FetchQueue[DownloadResult]): Completed download queue.
            A :class:`~s3fetch.s3.DownloadResult` is placed on this queue for
            every download attempt, regardless of success or failure.  Pass your
            own consumer thread via :func:`create_completed_objects_thread` to
            react to each result in real time.
        exit_event (threading.Event): Notify that script to exit.
        bucket (str): S3 bucket name, e.g. my-bucket.
        prefix (str): S3 object key prefix, e.g. my-folder/.
        download_dir (Union[str, Path]): Destination directory, e.g. /tmp.
        delimiter (str): S3 object key delimiter.
        download_config (dict): Download configuration.
        dry_run (bool): Run in dry run mode.
        progress_tracker (Optional[ProgressProtocol]): Progress tracker instance.

    Returns:
        Tuple[int, list]: Number of successful downloads and list of failed
            downloads as ``(key, exception)`` tuples.
    """
    if isinstance(download_dir, str):
        download_dir = Path(download_dir)

    success, failures = s3.create_download_threads(
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
        dry_run=dry_run,
        progress_tracker=progress_tracker,
    )
    return success, failures


def create_completed_objects_thread(
    queue: "S3FetchQueue[DownloadResult]",
    func: Callable[..., None],
    **kwargs: Dict[str, Any],
) -> None:
    """Create a thread to consume completed download results.

    The ``func`` is called in a new daemon thread with the ``queue`` as its
    first keyword argument, plus any additional ``kwargs``.  It should loop
    calling ``queue.get(block=True)`` until
    :class:`~s3fetch.exceptions.S3FetchQueueClosed` is raised.

    Each item yielded by the queue is a :class:`~s3fetch.s3.DownloadResult`
    containing the key, destination path, success flag, file size, and any
    error that occurred.  This gives consumers real-time, per-object
    notification for both successes and failures.

    The built-in :func:`~s3fetch.utils.print_completed_objects` function can
    be used here for simple CLI-style output, or you can supply your own
    handler for custom behaviour such as Rich progress bars or pipelining
    downloads into a compression stream.

    Args:
        queue (S3FetchQueue[DownloadResult]): Completed-downloads queue.
        func (Callable[..., None]): Function to run in a new thread. Will receive
            the completed objects queue and any additional kwargs passed.
        **kwargs: Any additional arguments to pass to ``func``.
    """
    logger.debug("Creating completed objects thread")
    threading.Thread(
        name="completed_objects",
        target=func,
        kwargs={
            "queue": queue,
            **kwargs,
        },
        daemon=True,
    ).start()


# Internal alias used by cli.py
_create_completed_objects_thread = create_completed_objects_thread
