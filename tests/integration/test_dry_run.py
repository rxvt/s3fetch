"""Integration test for S3Fetch dry run functionality."""

from pathlib import Path

import boto3
from moto import mock_aws

from s3fetch import download


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

    success_count, failures = download(
        f"s3://{bucket_name}/",
        download_dir=download_dir,
        dry_run=True,
        client=s3_client,
    )

    # Verify all files were "processed" successfully
    assert success_count == len(test_objects), "All objects should be processed"
    assert len(failures) == 0, "No failures should be reported"

    # Most importantly, verify no files were actually downloaded
    downloaded_files = list(download_dir.rglob("*.*"))
    assert len(downloaded_files) == 0, "No files should be downloaded in dry run mode"

    # Directories should not be created in dry run mode
    assert not (download_dir / "data").exists()
    assert not (download_dir / "config").exists()
