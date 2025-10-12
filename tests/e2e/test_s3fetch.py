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
