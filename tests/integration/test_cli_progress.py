"""Integration tests for CLI progress functionality."""

import tempfile
from unittest.mock import patch

import boto3
from click.testing import CliRunner
from moto import mock_aws

from s3fetch.cli import cli


def _setup_bucket(bucket_name: str, keys: list) -> None:
    """Create a moto S3 bucket and populate it with test objects."""
    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket=bucket_name)
    for key in keys:
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=f"Content of {key}")


@mock_aws
def test_cli_default_progress_prints_object_keys():
    """Default (no --progress flag) prints each object key, no summary."""
    _setup_bucket("test-bucket", ["file1.txt", "file2.txt", "file3.txt"])

    runner = CliRunner()
    with tempfile.TemporaryDirectory() as tmpdir:
        result = runner.invoke(cli, ["s3://test-bucket/", "--download-dir", tmpdir])

    assert result.exit_code == 0
    assert "file1.txt" in result.output
    assert "file2.txt" in result.output
    assert "file3.txt" in result.output
    assert "Progress Summary:" not in result.output


@mock_aws
def test_cli_progress_simple_prints_object_keys():
    """--progress simple prints each object key, no summary."""
    _setup_bucket("test-bucket", ["file1.txt", "file2.txt", "file3.txt"])

    runner = CliRunner()
    with tempfile.TemporaryDirectory() as tmpdir:
        result = runner.invoke(
            cli,
            ["s3://test-bucket/", "--download-dir", tmpdir, "--progress", "simple"],
        )

    assert result.exit_code == 0
    assert "file1.txt" in result.output
    assert "file2.txt" in result.output
    assert "file3.txt" in result.output
    assert "Progress Summary:" not in result.output


@mock_aws
def test_cli_progress_detailed_prints_object_keys_and_summary():
    """--progress detailed prints each object key and a final summary."""
    _setup_bucket("test-bucket", ["file1.txt", "file2.txt"])

    runner = CliRunner()
    with tempfile.TemporaryDirectory() as tmpdir:
        result = runner.invoke(
            cli,
            [
                "s3://test-bucket/",
                "--download-dir",
                tmpdir,
                "--progress",
                "detailed",
            ],
        )

    assert result.exit_code == 0
    # Per-object output
    assert "file1.txt" in result.output
    assert "file2.txt" in result.output
    # Summary
    assert "Progress Summary:" in result.output
    assert "Objects found: 2" in result.output
    assert "Objects downloaded: 2" in result.output


@mock_aws
def test_cli_progress_live_update_no_per_object_output():
    """--progress live-update suppresses per-object output, shows summary."""
    _setup_bucket("test-bucket", ["file1.txt", "file2.txt"])

    runner = CliRunner()
    with tempfile.TemporaryDirectory() as tmpdir:
        result = runner.invoke(
            cli,
            [
                "s3://test-bucket/",
                "--download-dir",
                tmpdir,
                "--progress",
                "live-update",
            ],
        )

    assert result.exit_code == 0
    # Per-object keys should NOT appear
    assert "file1.txt" not in result.output
    assert "file2.txt" not in result.output
    # Summary should appear
    assert "Progress Summary:" in result.output
    assert "Objects downloaded: 2" in result.output


@mock_aws
def test_cli_progress_fancy_with_rich_installed():
    """--progress fancy runs successfully and shows a summary when rich is installed."""
    _setup_bucket("test-bucket", ["file1.txt", "file2.txt"])

    runner = CliRunner()
    with tempfile.TemporaryDirectory() as tmpdir:
        result = runner.invoke(
            cli,
            [
                "s3://test-bucket/",
                "--download-dir",
                tmpdir,
                "--progress",
                "fancy",
            ],
        )

    assert result.exit_code == 0, f"Command failed with output: {result.output}"
    # Per-object keys should NOT appear
    assert "file1.txt" not in result.output
    assert "file2.txt" not in result.output
    # Summary should appear
    assert "Progress Summary:" in result.output
    assert "Objects downloaded: 2" in result.output


@mock_aws
def test_cli_progress_fancy_missing_rich_shows_install_hint():
    """--progress fancy without rich installed shows a helpful error."""
    _setup_bucket("test-bucket", ["file1.txt"])

    runner = CliRunner()
    with tempfile.TemporaryDirectory() as tmpdir:
        with patch("importlib.util.find_spec", return_value=None):
            result = runner.invoke(
                cli,
                [
                    "s3://test-bucket/",
                    "--download-dir",
                    tmpdir,
                    "--progress",
                    "fancy",
                ],
            )

    assert result.exit_code != 0
    assert "pip install s3fetch[fancy]" in result.output


@mock_aws
def test_cli_quiet_and_progress_mutually_exclusive():
    """--quiet and --progress together raise a UsageError."""
    _setup_bucket("test-bucket", ["file1.txt"])

    runner = CliRunner()
    with tempfile.TemporaryDirectory() as tmpdir:
        result = runner.invoke(
            cli,
            [
                "s3://test-bucket/",
                "--download-dir",
                tmpdir,
                "--quiet",
                "--progress",
                "detailed",
            ],
        )

    assert result.exit_code != 0
    assert "mutually exclusive" in result.output


@mock_aws
def test_cli_quiet_suppresses_all_stdout():
    """--quiet produces no stdout output."""
    _setup_bucket("test-bucket", ["file1.txt"])

    runner = CliRunner()
    with tempfile.TemporaryDirectory() as tmpdir:
        result = runner.invoke(
            cli,
            ["s3://test-bucket/", "--download-dir", tmpdir, "--quiet"],
        )

    assert result.exit_code == 0
    assert result.output == ""


@mock_aws
def test_cli_progress_dry_run_detailed_shows_summary():
    """--progress detailed with --dry-run shows found but zero downloaded."""
    _setup_bucket("test-bucket", ["file1.txt", "file2.txt"])

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["s3://test-bucket/", "--dry-run", "--progress", "detailed"],
    )

    assert result.exit_code == 0
    assert "Progress Summary:" in result.output
    assert "Objects found: 2" in result.output
    assert "Objects downloaded: 0" in result.output
