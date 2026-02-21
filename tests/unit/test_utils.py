import time
from concurrent.futures import ThreadPoolExecutor

from s3fetch import utils
from s3fetch.utils import ProgressProtocol, ProgressTracker


class TestProgressProtocol:
    """Tests for the ProgressProtocol structural interface."""

    def test_progress_tracker_satisfies_protocol(self):
        """ProgressTracker must satisfy ProgressProtocol (runtime_checkable)."""
        tracker = ProgressTracker()
        assert isinstance(tracker, ProgressProtocol)

    def test_custom_class_satisfies_protocol(self):
        """Any class with the two required methods satisfies ProgressProtocol."""

        class MinimalTracker:
            def increment_found(self) -> None:
                pass

            def increment_downloaded(self, bytes_count: int) -> None:
                pass

        assert isinstance(MinimalTracker(), ProgressProtocol)

    def test_missing_method_fails_protocol_check(self):
        """A class missing either method should not satisfy ProgressProtocol."""

        class IncompleteTracker:
            def increment_found(self) -> None:
                pass

            # increment_downloaded is absent

        assert not isinstance(IncompleteTracker(), ProgressProtocol)

    def test_none_does_not_satisfy_protocol(self):
        """None should not satisfy ProgressProtocol."""
        assert not isinstance(None, ProgressProtocol)


def test_create_exit_event():
    exit_event = utils.create_exit_event()
    assert exit_event.is_set() is False
    exit_event.set()
    assert exit_event.is_set() is True
    exit_event.clear()
    assert exit_event.is_set() is False


class TestProgressTracker:
    """Test cases for the ProgressTracker class."""

    def test_initial_state(self):
        """Test that tracker starts with expected initial values."""
        tracker = utils.ProgressTracker()
        stats = tracker.get_stats()

        assert stats["objects_found"] == 0
        assert stats["objects_downloaded"] == 0
        assert stats["bytes_downloaded"] == 0
        assert stats["elapsed_time"] >= 0
        assert stats["download_speed_mbps"] == 0.0

    def test_increment_found(self):
        """Test that increment_found correctly updates the found counter."""
        tracker = utils.ProgressTracker()

        tracker.increment_found()
        assert tracker.objects_found == 1

        tracker.increment_found()
        assert tracker.objects_found == 2

    def test_increment_downloaded(self):
        """Test that increment_downloaded updates counters correctly."""
        tracker = utils.ProgressTracker()

        tracker.increment_downloaded(1024)
        stats = tracker.get_stats()
        assert stats["objects_downloaded"] == 1
        assert stats["bytes_downloaded"] == 1024

        tracker.increment_downloaded(2048)
        stats = tracker.get_stats()
        assert stats["objects_downloaded"] == 2
        assert stats["bytes_downloaded"] == 3072

    def test_download_speed_calculation(self):
        """Test that download speed is calculated correctly."""
        tracker = utils.ProgressTracker()

        # Set start time to a known value for predictable speed calculation
        tracker.start_time = time.time() - 1.0  # 1 second ago
        tracker.increment_downloaded(1024 * 1024)  # 1 MB

        stats = tracker.get_stats()
        # Should be approximately 1 MB/s = 1.0 MB/s
        assert 0.5 <= stats["download_speed_mbps"] <= 1.5

    def test_thread_safety_objects_found(self):
        """Test that objects_found increments are thread-safe.

        This tests single-threaded operation behavior.
        """
        tracker = utils.ProgressTracker()

        def increment_found_many() -> None:
            for _ in range(100):
                tracker.increment_found()

        # Even though objects_found doesn't need locking (single-threaded),
        # test it works correctly with multiple threads
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(increment_found_many) for _ in range(5)]
            for future in futures:
                future.result()

        assert tracker.objects_found == 500

    def test_thread_safety_downloads(self):
        """Test that download counters are thread-safe with concurrent access."""
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

    def test_concurrent_operations(self):
        """Test mixed concurrent operations on all counters."""
        tracker = utils.ProgressTracker()

        def worker_found() -> None:
            for _ in range(50):
                tracker.increment_found()

        def worker_downloaded() -> None:
            for _ in range(30):
                tracker.increment_downloaded(1000)

        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = []
            # 3 threads incrementing found
            for _ in range(3):
                futures.append(executor.submit(worker_found))
            # 3 threads incrementing downloaded
            for _ in range(3):
                futures.append(executor.submit(worker_downloaded))

            for future in futures:
                future.result()

        stats = tracker.get_stats()
        assert stats["objects_found"] == 150  # 3 * 50
        assert stats["objects_downloaded"] == 90  # 3 * 30
        assert stats["bytes_downloaded"] == 90000  # 3 * 30 * 1000

    def test_get_stats_during_operations(self):
        """Test that get_stats() works correctly during concurrent operations."""
        tracker = utils.ProgressTracker()

        def continuous_downloads() -> None:
            for _i in range(100):
                tracker.increment_downloaded(1000)
                time.sleep(0.001)  # Small delay

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

        # Verify stats are consistent and increasing
        for i in range(1, len(stats_list)):
            current = stats_list[i]
            previous = stats_list[i - 1]

            # Values should be monotonically increasing (or equal)
            assert current["objects_downloaded"] >= previous["objects_downloaded"]
            assert current["bytes_downloaded"] >= previous["bytes_downloaded"]
            assert current["elapsed_time"] >= previous["elapsed_time"]
