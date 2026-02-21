"""Integration test for S3 download reporting functionality."""

import threading
from pathlib import Path

import boto3
from moto import mock_aws

from s3fetch import api, s3
from s3fetch.exceptions import S3FetchQueueClosed
from s3fetch.s3 import DownloadResult


@mock_aws
def test_download_success_and_failure_reporting(tmpdir):
    """Test that successful and failed downloads are correctly reported."""
    # Setup mock S3
    s3_client = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    # Create test objects
    valid_keys = [
        "valid/file1.txt",
        "valid/file2.txt",
    ]

    for key in valid_keys:
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=f"Content of {key}")

    # Setup queues and events
    download_queue = s3.get_queue("download")
    completed_queue = s3.get_queue("completion")
    exit_event = threading.Event()

    # Add valid keys to download queue
    for key in valid_keys:
        download_queue.put(key)

    # Add a non-existent key to simulate a failure
    non_existent_key = "invalid/nonexistent.txt"
    download_queue.put(non_existent_key)

    # Close the queue to indicate no more items
    download_queue.close()

    download_dir = Path(tmpdir)
    download_config = s3.create_download_config()

    # Perform downloads
    success_count, failures = api.download_objects(
        client=s3_client,
        threads=2,
        download_queue=download_queue,
        completed_queue=completed_queue,
        exit_event=exit_event,
        bucket=bucket_name,
        prefix="",
        download_dir=download_dir,
        delimiter="/",
        download_config=download_config,
        dry_run=False,
    )

    # Verify results
    assert success_count == 2, "Should have 2 successful downloads"
    assert len(failures) == 1, "Should have 1 failed download"
    # Check that the failed key matches our non-existent key
    assert failures[0][0] == non_existent_key

    # Verify completed queue has the correct items (now DownloadResult objects)
    completed_items = []
    try:
        while True:
            completed_items.append(completed_queue.get())
    except S3FetchQueueClosed:
        pass

    # All items on the queue are DownloadResult objects
    assert all(isinstance(item, DownloadResult) for item in completed_items)
    # Only successful downloads should appear with success=True
    successful_items = [item for item in completed_items if item.success]
    assert len(successful_items) == 2, "Completed queue should have 2 successful items"
    # Check that successful keys match the valid keys
    assert {item.key for item in successful_items} == set(valid_keys)

    # Verify files were actually downloaded
    for key in valid_keys:
        # When prefix is empty, the full key path is preserved
        dir_name, file_name = key.rsplit("/", 1) if "/" in key else ("", key)
        file_path = download_dir / dir_name / file_name
        assert file_path.exists(), f"Downloaded file {file_path} should exist"
        assert file_path.read_text() == f"Content of {key}", "File content should match"


@mock_aws
def test_failed_download_emits_download_result_with_success_false(tmpdir):
    """Failed downloads emit a DownloadResult(success=False) on the completed queue."""
    s3_client = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    download_queue = s3.get_queue("download")
    completed_queue = s3.get_queue("completion")
    exit_event = threading.Event()

    # Only enqueue a key that does not exist in S3
    non_existent_key = "does/not/exist.txt"
    download_queue.put(non_existent_key)
    download_queue.close()

    download_dir = Path(tmpdir)
    download_config = s3.create_download_config()

    success_count, failures = api.download_objects(
        client=s3_client,
        threads=1,
        download_queue=download_queue,
        completed_queue=completed_queue,
        exit_event=exit_event,
        bucket=bucket_name,
        prefix="",
        download_dir=download_dir,
        delimiter="/",
        download_config=download_config,
        dry_run=False,
    )

    assert success_count == 0
    assert len(failures) == 1

    # The failure should still emit a DownloadResult on the completed queue
    completed_items = []
    try:
        while True:
            completed_items.append(completed_queue.get())
    except S3FetchQueueClosed:
        pass

    assert len(completed_items) == 1
    result = completed_items[0]
    assert isinstance(result, DownloadResult)
    assert result.key == non_existent_key
    assert result.success is False
    assert result.error is not None
    assert result.file_size == 0


@mock_aws
def test_create_completed_objects_thread_with_custom_handler(tmpdir):
    """create_completed_objects_thread works with a custom DownloadResult handler."""
    s3_client = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    test_objects = {"file1.txt": "aaa", "file2.txt": "bb"}
    for key, body in test_objects.items():
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=body)

    download_queue = s3.get_queue("download")
    completed_queue: s3.S3FetchQueue[DownloadResult] = s3.S3FetchQueue()
    exit_event = threading.Event()

    for key in test_objects:
        download_queue.put(key)
    download_queue.close()

    # Custom handler that collects results
    collected: list[DownloadResult] = []

    def my_handler(queue: s3.S3FetchQueue[DownloadResult]) -> None:
        from s3fetch.exceptions import S3FetchQueueClosed as Closed

        while True:
            try:
                collected.append(queue.get(block=True))
            except Closed:
                break

    api.create_completed_objects_thread(queue=completed_queue, func=my_handler)

    download_dir = Path(tmpdir)
    download_config = s3.create_download_config()

    success_count, failures = api.download_objects(
        client=s3_client,
        threads=2,
        download_queue=download_queue,
        completed_queue=completed_queue,
        exit_event=exit_event,
        bucket=bucket_name,
        prefix="",
        download_dir=download_dir,
        delimiter="/",
        download_config=download_config,
    )

    # Give the handler thread a moment to finish draining
    import time

    time.sleep(0.1)

    assert success_count == 2
    assert len(failures) == 0
    assert len(collected) == 2
    assert all(isinstance(r, DownloadResult) for r in collected)
    assert all(r.success for r in collected)
    assert {r.key for r in collected} == set(test_objects)
    # file_size should reflect actual content sizes
    for r in collected:
        expected_size = len(test_objects[r.key].encode())
        assert r.file_size == expected_size
