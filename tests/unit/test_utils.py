import os
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import pytest

from s3fetch import utils
from s3fetch.utils import ProgressProtocol, ProgressTracker

# ---------------------------------------------------------------------------
# ProgressProtocol
# ---------------------------------------------------------------------------


def test_progress_tracker_satisfies_protocol():
    """ProgressTracker must satisfy ProgressProtocol (runtime_checkable)."""
    tracker = ProgressTracker()
    assert isinstance(tracker, ProgressProtocol)


def test_custom_class_satisfies_protocol():
    """Any class with the two required methods satisfies ProgressProtocol."""

    class MinimalTracker:
        def increment_found(self) -> None:
            pass

        def increment_downloaded(self, bytes_count: int) -> None:
            pass

    assert isinstance(MinimalTracker(), ProgressProtocol)


def test_missing_method_fails_protocol_check():
    """A class missing either method should not satisfy ProgressProtocol."""

    class IncompleteTracker:
        def increment_found(self) -> None:
            pass

        # increment_downloaded is absent

    assert not isinstance(IncompleteTracker(), ProgressProtocol)


def test_none_does_not_satisfy_protocol():
    """None should not satisfy ProgressProtocol."""
    assert not isinstance(None, ProgressProtocol)


# ---------------------------------------------------------------------------
# create_exit_event
# ---------------------------------------------------------------------------


def test_create_exit_event():
    exit_event = utils.create_exit_event()
    assert exit_event.is_set() is False
    exit_event.set()
    assert exit_event.is_set() is True
    exit_event.clear()
    assert exit_event.is_set() is False


# ---------------------------------------------------------------------------
# ProgressTracker
# ---------------------------------------------------------------------------


def test_progress_tracker_initial_state():
    """Tracker starts with expected initial values."""
    tracker = utils.ProgressTracker()
    stats = tracker.get_stats()

    assert stats["objects_found"] == 0
    assert stats["objects_downloaded"] == 0
    assert stats["bytes_downloaded"] == 0
    assert stats["elapsed_time"] >= 0
    assert stats["download_speed_mbps"] == 0.0


def test_progress_tracker_increment_found():
    """increment_found correctly updates the found counter."""
    tracker = utils.ProgressTracker()

    tracker.increment_found()
    assert tracker.objects_found == 1

    tracker.increment_found()
    assert tracker.objects_found == 2


def test_progress_tracker_increment_downloaded():
    """increment_downloaded updates counters correctly."""
    tracker = utils.ProgressTracker()

    tracker.increment_downloaded(1024)
    stats = tracker.get_stats()
    assert stats["objects_downloaded"] == 1
    assert stats["bytes_downloaded"] == 1024

    tracker.increment_downloaded(2048)
    stats = tracker.get_stats()
    assert stats["objects_downloaded"] == 2
    assert stats["bytes_downloaded"] == 3072


def test_progress_tracker_download_speed_calculation():
    """Download speed is calculated correctly."""
    tracker = utils.ProgressTracker()

    # Set start time to a known value for predictable speed calculation
    tracker.start_time = time.time() - 1.0  # 1 second ago
    tracker.increment_downloaded(1024 * 1024)  # 1 MB

    stats = tracker.get_stats()
    # Should be approximately 1 MB/s
    assert 0.5 <= stats["download_speed_mbps"] <= 1.5


def test_progress_tracker_thread_safety_objects_found():
    """objects_found increments work correctly across multiple threads."""
    tracker = utils.ProgressTracker()

    def increment_found_many() -> None:
        for _ in range(100):
            tracker.increment_found()

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(increment_found_many) for _ in range(5)]
        for future in futures:
            future.result()

    assert tracker.objects_found == 500


def test_progress_tracker_thread_safety_downloads():
    """Download counters are thread-safe with concurrent access."""
    tracker = utils.ProgressTracker()

    def increment_downloads() -> None:
        for i in range(100):
            tracker.increment_downloaded(100 + i)  # Varying sizes

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(increment_downloads) for _ in range(5)]
        for future in futures:
            future.result()

    stats = tracker.get_stats()
    assert stats["objects_downloaded"] == 500

    # Calculate expected bytes: 5 threads * 100 calls * (100 + i for i in 0..99)
    # Sum of (100 + i) for i in 0..99 = 100*100 + sum(0..99) = 10000 + 4950 = 14950
    expected_bytes = 5 * 14950
    assert stats["bytes_downloaded"] == expected_bytes


def test_progress_tracker_concurrent_operations():
    """Mixed concurrent found/downloaded increments produce correct totals."""
    tracker = utils.ProgressTracker()

    def worker_found() -> None:
        for _ in range(50):
            tracker.increment_found()

    def worker_downloaded() -> None:
        for _ in range(30):
            tracker.increment_downloaded(1000)

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = []
        for _ in range(3):
            futures.append(executor.submit(worker_found))
        for _ in range(3):
            futures.append(executor.submit(worker_downloaded))
        for future in futures:
            future.result()

    stats = tracker.get_stats()
    assert stats["objects_found"] == 150  # 3 * 50
    assert stats["objects_downloaded"] == 90  # 3 * 30
    assert stats["bytes_downloaded"] == 90000  # 3 * 30 * 1000


def test_progress_tracker_get_stats_returns_elapsed_time():
    """Elapsed time is a positive float."""
    tracker = utils.ProgressTracker()
    stats = tracker.get_stats()
    assert isinstance(stats["elapsed_time"], float)
    assert stats["elapsed_time"] >= 0


def test_progress_tracker_get_stats_during_operations():
    """get_stats() returns consistent, monotonically increasing values under load."""
    tracker = utils.ProgressTracker()

    def continuous_downloads() -> None:
        for _i in range(100):
            tracker.increment_downloaded(1000)
            time.sleep(0.001)

    def check_stats() -> list[dict[str, float]]:
        stats_list = []
        for _ in range(20):
            stats_list.append(tracker.get_stats())
            time.sleep(0.005)
        return stats_list

    with ThreadPoolExecutor(max_workers=3) as executor:
        download_future = executor.submit(continuous_downloads)
        stats_future = executor.submit(check_stats)

        download_future.result()
        stats_list = stats_future.result()

    for i in range(1, len(stats_list)):
        current = stats_list[i]
        previous = stats_list[i - 1]

        assert current["objects_downloaded"] >= previous["objects_downloaded"]
        assert current["bytes_downloaded"] >= previous["bytes_downloaded"]
        assert current["elapsed_time"] >= previous["elapsed_time"]


# ---------------------------------------------------------------------------
# get_available_threads
# ---------------------------------------------------------------------------


def test_get_available_threads_uses_sched_getaffinity():
    """On platforms with sched_getaffinity, the affinity set size is returned."""
    with patch.object(os, "sched_getaffinity", return_value={0, 1, 2, 3}, create=True):
        result = utils.get_available_threads()
    assert result == 4


def test_get_available_threads_falls_back_when_sched_getaffinity_raises():
    """When sched_getaffinity raises, fall back to cpu_count."""

    def bad_affinity(n: int) -> set:
        raise OSError("not supported")

    with patch.object(os, "sched_getaffinity", bad_affinity, create=True):
        with patch.object(os, "cpu_count", return_value=8):
            result = utils.get_available_threads()
    assert result == 8


def test_get_available_threads_falls_back_when_sched_getaffinity_absent():
    """On platforms without sched_getaffinity, cpu_count is used."""
    original = getattr(os, "sched_getaffinity", None)
    try:
        if hasattr(os, "sched_getaffinity"):
            delattr(os, "sched_getaffinity")
        with patch.object(os, "cpu_count", return_value=6):
            result = utils.get_available_threads()
        assert result == 6
    finally:
        if original is not None:
            os.sched_getaffinity = original  # type: ignore[attr-defined]


def test_get_available_threads_defaults_to_1_when_cpu_count_none():
    """When cpu_count() returns None, the result defaults to 1."""
    original = getattr(os, "sched_getaffinity", None)
    try:
        if hasattr(os, "sched_getaffinity"):
            delattr(os, "sched_getaffinity")
        with patch.object(os, "cpu_count", return_value=None):
            result = utils.get_available_threads()
        assert result == 1
    finally:
        if original is not None:
            os.sched_getaffinity = original  # type: ignore[attr-defined]


def test_get_available_threads_always_at_least_1():
    """get_available_threads() never returns less than 1."""
    result = utils.get_available_threads()
    assert result >= 1


# ---------------------------------------------------------------------------
# custom_print
# ---------------------------------------------------------------------------


def test_custom_print_prints_when_not_quiet(capsys):
    """When quiet=False, the message is printed to stdout."""
    utils.custom_print("hello world", quiet=False)
    captured = capsys.readouterr()
    assert "hello world" in captured.out


def test_custom_print_suppresses_when_quiet(capsys):
    """When quiet=True, nothing is written to stdout."""
    utils.custom_print("should be hidden", quiet=True)
    captured = capsys.readouterr()
    assert captured.out == ""


def test_custom_print_custom_end_character(capsys):
    """The end= parameter controls the line terminator."""
    utils.custom_print("no newline", quiet=False, end="")
    captured = capsys.readouterr()
    assert captured.out == "no newline"


# ---------------------------------------------------------------------------
# set_download_dir
# ---------------------------------------------------------------------------


def test_set_download_dir_none_returns_cwd(tmp_path, monkeypatch):
    """When download_dir is None, the cwd is used."""
    monkeypatch.chdir(tmp_path)
    result = utils.set_download_dir(None)
    assert result == Path(os.getcwd())


def test_set_download_dir_explicit_path_returned_unchanged(tmp_path):
    """When an existing directory is given, it is returned as-is."""
    result = utils.set_download_dir(tmp_path)
    assert result == tmp_path


def test_set_download_dir_nonexistent_raises(tmp_path):
    """A non-existent directory raises DirectoryDoesNotExistError."""
    from s3fetch.exceptions import DirectoryDoesNotExistError

    with pytest.raises(DirectoryDoesNotExistError):
        utils.set_download_dir(tmp_path / "does_not_exist")
