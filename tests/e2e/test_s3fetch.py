"""End-to-end tests for S3Fetch against real S3 bucket."""

import pytest
from click.testing import CliRunner

from s3fetch.cli import cli


@pytest.fixture
def test_bucket():
    """Test bucket configuration."""
    return {
        "bucket": "s3fetch-cicd-test-bucket",
        "region": "us-east-1",
    }


def test_download_single_file(test_bucket, tmp_path):
    """Test downloading a single specific file from the test bucket."""
    runner = CliRunner()

    # Download the specific small test file
    result = runner.invoke(
        cli,
        [
            f"s3://{test_bucket['bucket']}/small/file_000.txt",
            "--download-dir",
            str(tmp_path),
            "--region",
            test_bucket["region"],
            "--progress",
            "simple",
        ],
    )

    # Verify the command succeeded
    assert result.exit_code == 0, f"Command failed with output: {result.output}"

    # Verify the specific file was downloaded (at root of download dir)
    expected_file = tmp_path / "file_000.txt"
    assert expected_file.exists(), f"Expected file {expected_file} was not downloaded"
    assert expected_file.stat().st_size > 0, "Downloaded file is empty"

    # Verify progress was reported
    assert "Objects found:" in result.output
    assert "Objects downloaded:" in result.output


def test_download_multiple_files(test_bucket, tmp_path):
    """Test downloading multiple files from a prefix.

    Downloads all 120 files from the small/ prefix to verify bulk
    download functionality works correctly.
    """
    runner = CliRunner()

    # Download all files from the small/ prefix
    result = runner.invoke(
        cli,
        [
            f"s3://{test_bucket['bucket']}/small/",
            "--download-dir",
            str(tmp_path),
            "--region",
            test_bucket["region"],
            "--progress",
            "simple",
        ],
    )

    # Verify the command succeeded
    assert result.exit_code == 0, f"Command failed with output: {result.output}"

    # Verify progress output was shown
    assert "Objects found:" in result.output
    assert "Objects downloaded:" in result.output

    # Get all downloaded .txt files
    downloaded_files = list(tmp_path.glob("file_*.txt"))

    # Verify we got exactly 120 files
    assert len(downloaded_files) == 120, (
        f"Expected 120 files but found {len(downloaded_files)}"
    )

    # Verify file names match expected pattern
    expected_files = {f"file_{i:03d}.txt" for i in range(120)}
    actual_files = {f.name for f in downloaded_files}
    assert expected_files == actual_files, (
        f"File name mismatch. Missing: {expected_files - actual_files}, "
        f"Extra: {actual_files - expected_files}"
    )

    # Spot check: verify some files are not empty
    for i in [0, 50, 119]:
        file_path = tmp_path / f"file_{i:03d}.txt"
        assert file_path.exists(), f"File {file_path.name} doesn't exist"
        assert file_path.stat().st_size > 0, f"File {file_path.name} is empty"

    # Verify files are at root of download dir, not nested
    assert not (tmp_path / "small").exists(), (
        "Files should be at root of download dir, not in 'small/' subdirectory"
    )
