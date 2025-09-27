"""Integration tests for CLI progress functionality."""

import tempfile

import boto3
from click.testing import CliRunner
from moto import mock_aws

from s3fetch.cli import cli


@mock_aws
def test_cli_progress_simple():
    """Test CLI with --progress simple option."""
    # Setup mock S3
    s3_client = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    # Create test objects
    test_objects = ["file1.txt", "file2.txt", "file3.txt"]
    for obj_key in test_objects:
        s3_client.put_object(
            Bucket=bucket_name, Key=obj_key, Body=f"Content of {obj_key}"
        )

    runner = CliRunner()
    with tempfile.TemporaryDirectory() as tmpdir:
        result = runner.invoke(
            cli,
            [f"s3://{bucket_name}/", "--download-dir", tmpdir, "--progress", "simple"],
        )

    assert result.exit_code == 0
    assert "Progress Summary:" in result.output
    assert "Objects found: 3" in result.output
    assert "Objects downloaded: 3" in result.output


@mock_aws
def test_cli_progress_detailed():
    """Test CLI with --progress detailed option."""
    # Setup mock S3
    s3_client = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    # Create test objects
    test_objects = ["file1.txt", "file2.txt"]
    for obj_key in test_objects:
        s3_client.put_object(
            Bucket=bucket_name, Key=obj_key, Body=f"Content of {obj_key}"
        )

    runner = CliRunner()
    with tempfile.TemporaryDirectory() as tmpdir:
        result = runner.invoke(
            cli,
            [
                f"s3://{bucket_name}/",
                "--download-dir",
                tmpdir,
                "--progress",
                "detailed",
            ],
        )

    assert result.exit_code == 0
    assert "Progress Summary:" in result.output
    assert "Objects found: 2" in result.output
    assert "Objects downloaded: 2" in result.output


@mock_aws
def test_cli_progress_none():
    """Test CLI with --progress none (default)."""
    # Setup mock S3
    s3_client = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    # Create test object
    s3_client.put_object(Bucket=bucket_name, Key="file1.txt", Body="Content")

    runner = CliRunner()
    with tempfile.TemporaryDirectory() as tmpdir:
        result = runner.invoke(
            cli,
            [f"s3://{bucket_name}/", "--download-dir", tmpdir, "--progress", "none"],
        )

    assert result.exit_code == 0
    # Should not contain progress summary
    assert "Progress Summary:" not in result.output


@mock_aws
def test_cli_progress_dry_run():
    """Test CLI progress with --dry-run flag."""
    # Setup mock S3
    s3_client = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    # Create test objects
    test_objects = ["file1.txt", "file2.txt"]
    for obj_key in test_objects:
        s3_client.put_object(
            Bucket=bucket_name, Key=obj_key, Body=f"Content of {obj_key}"
        )

    runner = CliRunner()
    result = runner.invoke(
        cli, [f"s3://{bucket_name}/", "--dry-run", "--progress", "simple"]
    )

    assert result.exit_code == 0
    assert "Progress Summary:" in result.output
    assert "Objects found: 2" in result.output
    # In dry run mode, objects should be found but not downloaded
    assert "Objects downloaded: 0" in result.output


@mock_aws
def test_cli_progress_quiet_mode():
    """Test that progress is suppressed in quiet mode."""
    # Setup mock S3
    s3_client = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    # Create test object
    s3_client.put_object(Bucket=bucket_name, Key="file1.txt", Body="Content")

    runner = CliRunner()
    with tempfile.TemporaryDirectory() as tmpdir:
        result = runner.invoke(
            cli,
            [
                f"s3://{bucket_name}/",
                "--download-dir",
                tmpdir,
                "--progress",
                "simple",
                "--quiet",
            ],
        )

    assert result.exit_code == 0
    # Progress should be suppressed in quiet mode
    assert "Progress Summary:" not in result.output
