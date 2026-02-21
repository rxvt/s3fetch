"""Integration test for S3 regex filtering functionality."""

from pathlib import Path

import boto3
from moto import mock_aws

from s3fetch import download


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

    download(
        f"s3://{bucket_name}/",
        download_dir=download_dir,
        regex=r"\.txt$",
        threads=1,
        client=s3_client,
    )

    # Verify only .txt files were downloaded
    downloaded_files = list(download_dir.rglob("*"))
    downloaded_names = [f.name for f in downloaded_files if f.is_file()]

    assert "file1.txt" in downloaded_names
    assert "file4.txt" in downloaded_names
    assert "file2.csv" not in downloaded_names
    assert "file3.log" not in downloaded_names
    assert "report.json" not in downloaded_names
