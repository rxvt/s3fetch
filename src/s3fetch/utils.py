"""Misc utilities."""

import logging
import os
import threading
from pathlib import Path
from typing import Optional

from . import fs
from .exceptions import S3FetchQueueClosed
from .s3 import S3FetchQueue

logger = logging.getLogger(__name__)


def set_download_dir(download_dir: Optional[Path]) -> Path:
    """Set the download directory.

    Args:
        download_dir (Optional[Path]): Download directory, e,g. /tmp. Defaults to None.

    Returns:
        Path: Download directory.
    """
    if not download_dir:
        download_dir = Path(os.getcwd())

    fs.check_download_dir_exists(download_dir)

    logger.debug(f"download_dir={download_dir}")
    return download_dir


def get_available_threads() -> int:
    """Get the number of available threads.

    Returns:
        int: Number of available threads.
    """
    # os.sched_getaffinity() is not available on MacOS so default back to
    # os.cpu_count()
    try:
        threads = len(os.sched_getaffinity(0))  # type: ignore
    except AttributeError:
        logger.debug("os.sched_getaffinity() not available, using os.cpu_count()")
        threads = os.cpu_count() or 0

    if not threads:
        logger.warning("threads not available, defaulting to 1")
        threads = 1

    logger.debug(f"threads available: {threads}")
    return threads


def create_exit_event() -> threading.Event:
    """Create an exit event.

    Returns:
        threading.Event: Exit event.
    """
    return threading.Event()


def enable_debug() -> None:
    """Enable debug logging."""
    from s3fetch import logger as tmp_logger

    tmp_logger.setLevel(logging.DEBUG)


def custom_print(msg: str, quiet: bool) -> None:
    """Print message."""
    if not quiet:
        print(msg, flush=True)


def print_completed_objects(queue: S3FetchQueue) -> None:
    """Watch the completed object queue and print the object keys to STDOUT.

    Args:
        queue (S3FetchQueue): FIFO download queue.
    """
    while True:
        try:
            key = queue.get(block=True)
            custom_print(key, False)
        except S3FetchQueueClosed:
            logger.debug("S3FetchQueueClosed exception received")
            break
