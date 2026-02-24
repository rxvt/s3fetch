"""Misc utilities."""

import logging
import os
import threading
import time
from pathlib import Path
from typing import Dict, Optional, Protocol, runtime_checkable

from . import fs
from .exceptions import S3FetchQueueClosed
from .s3 import DownloadResult, S3FetchQueue

logger = logging.getLogger(__name__)


@runtime_checkable
class ProgressProtocol(Protocol):
    """Protocol for progress tracking during S3 downloads.

    Any object that implements :meth:`increment_found` and
    :meth:`increment_downloaded` satisfies this protocol — no inheritance
    required.  The built-in :class:`ProgressTracker` already conforms.

    Example — custom tracker that emits to a Rich progress bar::

        class RichProgressTracker:
            def __init__(self, task):
                self.task = task

            def increment_found(self) -> None:
                # update total count label
                ...

            def increment_downloaded(self, bytes_count: int) -> None:
                self.task.advance(bytes_count)

    Pass your tracker to :func:`s3fetch.api.list_objects` and
    :func:`s3fetch.api.download_objects` via the ``progress_tracker``
    parameter.
    """

    def increment_found(self) -> None:
        """Called once for each S3 object discovered during listing."""
        ...

    def increment_downloaded(self, bytes_count: int) -> None:
        """Called once for each successfully downloaded object.

        Args:
            bytes_count (int): Size of the downloaded file in bytes.
        """
        ...


class ProgressTracker:
    """Thread-safe progress tracking for s3fetch operations.

    Tracks objects found (single-threaded) and objects/bytes downloaded
    (multi-threaded).
    """

    def __init__(self) -> None:
        """Initialize progress tracking counters."""
        self.objects_found = 0  # No lock needed - single-threaded listing
        self.objects_downloaded = 0
        self.bytes_downloaded = 0
        self._download_lock = threading.Lock()  # Only for download counters
        self.start_time = time.time()

    def increment_found(self) -> None:
        """Increment objects found counter (called from single listing thread)."""
        self.objects_found += 1

    def increment_downloaded(self, bytes_count: int) -> None:
        """Increment downloaded counters (called from multiple download threads).

        Args:
            bytes_count: Number of bytes downloaded for this object.
        """
        with self._download_lock:
            self.objects_downloaded += 1
            self.bytes_downloaded += bytes_count

    def get_stats(self) -> Dict[str, float]:
        """Get current progress statistics.

        Returns:
            Dict with keys: objects_found, objects_downloaded, bytes_downloaded,
            elapsed_time, download_speed_mbps
        """
        with self._download_lock:
            downloaded = self.objects_downloaded
            bytes_dl = self.bytes_downloaded

        elapsed = time.time() - self.start_time
        speed_mbps = (bytes_dl / (1024 * 1024)) / elapsed if elapsed > 0 else 0.0

        return {
            "objects_found": self.objects_found,
            "objects_downloaded": downloaded,
            "bytes_downloaded": bytes_dl,
            "elapsed_time": elapsed,
            "download_speed_mbps": speed_mbps,
        }


def format_bytes(num_bytes: float, suffix: str = "") -> str:
    """Format a byte count as a human-readable string with adaptive units.

    Automatically selects B, KB, MB, or GB based on magnitude.

    Args:
        num_bytes: Number of bytes to format.
        suffix: Optional suffix appended after the unit, e.g. ``"/s"`` for
            speed values.

    Returns:
        Formatted string such as ``"4.5 KB"``, ``"12.3 MB/s"``, ``"512 B"``.

    Examples:
        >>> format_bytes(512)
        '512 B'
        >>> format_bytes(1536)
        '1.5 KB'
        >>> format_bytes(1048576, suffix="/s")
        '1.0 MB/s'
    """
    if num_bytes < 1024:
        return f"{int(num_bytes)} B{suffix}"
    if num_bytes < 1024**2:
        return f"{num_bytes / 1024:.1f} KB{suffix}"
    if num_bytes < 1024**3:
        return f"{num_bytes / 1024**2:.1f} MB{suffix}"
    return f"{num_bytes / 1024**3:.1f} GB{suffix}"


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


def custom_print(msg: str, quiet: bool, *, end: str = "\n", flush: bool = True) -> None:
    """Print message with configurable formatting.

    Args:
        msg: Message to print
        quiet: If True, suppress output
        end: String appended after the message (default: newline)
        flush: Whether to forcibly flush the output stream
    """
    if not quiet:
        print(msg, end=end, flush=flush)


def print_completed_objects(queue: "S3FetchQueue[DownloadResult]") -> None:
    """Watch the completed object queue and print each object key to STDOUT.

    Consumes :class:`~s3fetch.s3.DownloadResult` objects from the queue as
    they arrive and prints the ``key`` of every successful download.  Failed
    downloads are silently skipped by this function (the failure is already
    captured in the result and re-raised by the download thread).

    To react to every result — including failures — pass your own handler to
    :func:`s3fetch.api.create_completed_objects_thread` instead of this
    function.

    Args:
        queue (S3FetchQueue[DownloadResult]): Completed-downloads queue.
    """
    while True:
        try:
            result = queue.get(block=True)
            if result.success:
                custom_print(result.key, False)
        except S3FetchQueueClosed:
            logger.debug("S3FetchQueueClosed exception received")
            break
