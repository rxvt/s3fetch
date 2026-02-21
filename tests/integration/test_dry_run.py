"""Integration test for S3Fetch dry run functionality."""

import threading
from pathlib import Path

import boto3
from moto import mock_aws

from s3fetch.api import download_objects, list_objects
from s3fetch.exceptions import S3FetchQueueClosed
from s3fetch.s3 import DownloadResult, S3FetchQueue, create_download_config


@mock_aws
def test_dry_run_functionality(tmpdir):
    """Test that dry run lists objects without downloading them."""
    # Setup mock S3
    s3_client = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    # Create test objects
    test_objects = [
        "data/file1.txt",
        "data/file2.txt",
        "config/settings.json",
    ]

    for obj_key in test_objects:
        s3_client.put_object(
            Bucket=bucket_name, Key=obj_key, Body=f"Content of {obj_key}"
        )

    download_dir = Path(tmpdir)

    # Setup queues
    download_queue = S3FetchQueue()
    completed_queue = S3FetchQueue()
    exit_event = threading.Event()

    # List all objects
    list_objects(
        bucket=bucket_name,
        prefix="",
        client=s3_client,
        download_queue=download_queue,
        delimiter="/",
        regex=None,
        exit_event=exit_event,
    )

    # Create download config
    download_config = create_download_config()

    # Execute dry run
    success_count, failures = download_objects(
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
        dry_run=True,  # This is the key parameter we're testing
    )

    # Verify all files were "processed" successfully
    assert success_count == len(test_objects), "All objects should be processed"
    assert len(failures) == 0, "No failures should be reported"

    # Get completed items
    completed_items = []
    try:
        while True:
            completed_items.append(completed_queue.get())
    except S3FetchQueueClosed:
        pass

    # Verify all items were reported as completed
    assert len(completed_items) == len(test_objects), "All items should be counted"
    # Each item is now a DownloadResult; extract the keys for comparison
    assert all(isinstance(item, DownloadResult) for item in completed_items)
    assert {item.key for item in completed_items} == set(test_objects)

    # Most importantly, verify no files were actually downloaded
    downloaded_files = list(download_dir.rglob("*.*"))  # Get all files, not directories
    assert len(downloaded_files) == 0, "No files should be downloaded in dry run mode"

    # Directories should still be created
    assert not (download_dir / "data").exists(), "Directory structure should be created"
    assert not (download_dir / "config").exists(), (
        "Directory structure should be created"
    )
