"""Integration test for S3 regex filtering functionality."""

import threading
import time
from pathlib import Path

import boto3
from moto import mock_aws

from s3fetch.api import download_objects, list_objects
from s3fetch.s3 import S3FetchQueue, create_download_config


@mock_aws
def test_s3_regex_filtering_with_moto(tmpdir):
    """Test regex filtering functionality with mocked S3 bucket."""
    # Setup mock S3
    s3_client = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    # Create test objects with different file types
    test_objects = [
        "data/file1.txt",
        "data/file2.csv",
        "data/file3.log",
        "data/report.json",
        "backup/file4.txt",
        "backup/archive.zip",
        "config.yaml",
    ]

    for obj_key in test_objects:
        s3_client.put_object(
            Bucket=bucket_name, Key=obj_key, Body=f"Content of {obj_key}"
        )

    download_dir = Path(tmpdir)

    # Filter for .txt files only
    download_queue = S3FetchQueue()
    completed_queue = S3FetchQueue()
    exit_event = threading.Event()

    list_objects(
        bucket=bucket_name,
        prefix="",
        client=s3_client,
        download_queue=download_queue,
        delimiter="/",
        regex=r"\.txt$",
        exit_event=exit_event,
    )

    # Wait a moment for listing to populate queue
    time.sleep(0.1)

    # Create proper download config using factory function
    download_config = create_download_config()

    download_objects(
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

    # Verify only .txt files were downloaded
    downloaded_files = list(download_dir.rglob("*"))
    downloaded_names = [f.name for f in downloaded_files if f.is_file()]

    assert "file1.txt" in downloaded_names
    assert "file4.txt" in downloaded_names
    assert "file2.csv" not in downloaded_names
    assert "file3.log" not in downloaded_names
    assert "report.json" not in downloaded_names
