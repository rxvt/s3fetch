"""Integration test for S3 download reporting functionality."""

import threading
from pathlib import Path

import boto3
from moto import mock_aws

from s3fetch import api, s3
from s3fetch.exceptions import S3FetchQueueClosed


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

    # Verify completed queue has the correct items
    completed_items = []
    try:
        while True:
            completed_items.append(completed_queue.get())
    except S3FetchQueueClosed:
        pass

    assert len(completed_items) == 2, "Completed queue should have 2 items"
    # Check that completed items match the valid keys
    assert set(completed_items) == set(valid_keys)

    # Verify files were actually downloaded
    for key in valid_keys:
        # When prefix is empty, the full key path is preserved
        dir_name, file_name = key.rsplit("/", 1) if "/" in key else ("", key)
        file_path = download_dir / dir_name / file_name
        assert file_path.exists(), f"Downloaded file {file_path} should exist"
        assert file_path.read_text() == f"Content of {key}", "File content should match"
