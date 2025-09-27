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
    """Get the number of available threads for the current platform.

    Returns:
        int: Number of available threads (always >= 1).
    """
    threads = None
    # Prefer sched_getaffinity if available (Linux), fallback to cpu_count otherwise.
    sched_getaffinity = getattr(os, "sched_getaffinity", None)
    if sched_getaffinity is not None:
        try:
            threads = len(sched_getaffinity(0))
            logger.debug("Using os.sched_getaffinity(0): %d threads available", threads)
        except Exception as e:
            logger.warning(
                "Failed to use os.sched_getaffinity(0): %s. "
                "Falling back to os.cpu_count().",
                e,
            )
    if threads is None:
        threads = os.cpu_count()
        logger.debug("Using os.cpu_count(): %s threads available", threads)
    if not isinstance(threads, int) or threads < 1:
        logger.warning("Could not determine thread count, defaulting to 1")
        threads = 1
    return threads


def create_exit_event() -> threading.Event:
    """Create an exit event.

    Returns:
        threading.Event: Exit event.
    """
    return threading.Event()


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
