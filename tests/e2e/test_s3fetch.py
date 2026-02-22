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

    result = runner.invoke(
        cli,
        [
            f"s3://{test_bucket['bucket']}/small/file_000.txt",
            "--download-dir",
            str(tmp_path),
            "--region",
            test_bucket["region"],
            "--progress",
            "detailed",
        ],
    )

    assert result.exit_code == 0, f"Command failed with output: {result.output}"

    expected_file = tmp_path / "file_000.txt"
    assert expected_file.exists(), f"Expected file {expected_file} was not downloaded"
    assert expected_file.stat().st_size > 0, "Downloaded file is empty"

    assert "Objects found:" in result.output
    assert "Objects downloaded:" in result.output


def test_download_multiple_files(test_bucket, tmp_path):
    """Test downloading multiple files from a prefix.

    Downloads all 120 files from the small/ prefix to verify bulk
    download functionality works correctly.
    """
    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            f"s3://{test_bucket['bucket']}/small/",
            "--download-dir",
            str(tmp_path),
            "--region",
            test_bucket["region"],
            "--progress",
            "detailed",
        ],
    )

    assert result.exit_code == 0, f"Command failed with output: {result.output}"

    assert "Objects found:" in result.output
    assert "Objects downloaded:" in result.output

    downloaded_files = list(tmp_path.glob("file_*.txt"))

    assert len(downloaded_files) == 120, (
        f"Expected 120 files but found {len(downloaded_files)}"
    )

    expected_files = {f"file_{i:03d}.txt" for i in range(120)}
    actual_files = {f.name for f in downloaded_files}
    assert expected_files == actual_files, (
        f"File name mismatch. Missing: {expected_files - actual_files}, "
        f"Extra: {actual_files - expected_files}"
    )

    for i in [0, 50, 119]:
        file_path = tmp_path / f"file_{i:03d}.txt"
        assert file_path.exists(), f"File {file_path.name} doesn't exist"
        assert file_path.stat().st_size > 0, f"File {file_path.name} is empty"

    assert not (tmp_path / "small").exists(), (
        "Files should be at root of download dir, not in 'small/' subdirectory"
    )


def test_regex_filtering(test_bucket, tmp_path):
    """Test regex filtering to download only specific file types.

    Downloads from the extensions/ prefix using regex to filter only .json files.
    This tests s3fetch's key differentiator - regex filtering of S3 objects.
    """
    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            f"s3://{test_bucket['bucket']}/extensions/",
            "--download-dir",
            str(tmp_path),
            "--region",
            test_bucket["region"],
            "--regex",
            r".*\.json$",
            "--progress",
            "detailed",
        ],
    )

    assert result.exit_code == 0, f"Command failed with output: {result.output}"

    assert "Objects found:" in result.output
    assert "Objects downloaded:" in result.output

    downloaded_files = list(tmp_path.glob("*"))

    expected_count = 8
    assert len(downloaded_files) == expected_count, (
        f"Expected {expected_count} .json files but found {len(downloaded_files)}"
    )

    for file_path in downloaded_files:
        assert file_path.suffix == ".json", f"Found non-.json file: {file_path.name}"
        assert file_path.stat().st_size > 0, f"File {file_path.name} is empty"

    assert not (tmp_path / "extensions").exists(), (
        "Files should be at root of download dir, not in 'extensions/' subdirectory"
    )


def test_dry_run_mode(test_bucket, tmp_path):
    """Test dry-run mode lists objects without downloading them.

    Uses --dry-run flag on the small/ prefix to verify that objects are listed
    but not actually downloaded. This is an important safety feature.
    """
    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            f"s3://{test_bucket['bucket']}/small/",
            "--download-dir",
            str(tmp_path),
            "--region",
            test_bucket["region"],
            "--dry-run",
        ],
    )

    assert result.exit_code == 0, f"Command failed with output: {result.output}"

    assert "small/file_000.txt" in result.output
    assert "small/file_119.txt" in result.output

    downloaded_files = list(tmp_path.glob("*"))
    assert len(downloaded_files) == 0, (
        f"Expected no files in dry-run mode, but found {len(downloaded_files)}"
    )


def test_progress_tracking_simple(test_bucket, tmp_path):
    """Test simple progress mode prints object keys with no summary.

    Downloads from the sequences/ prefix with --progress simple (the default)
    to verify that object keys are printed but no summary is shown.
    """
    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            f"s3://{test_bucket['bucket']}/sequences/",
            "--download-dir",
            str(tmp_path),
            "--region",
            test_bucket["region"],
            "--progress",
            "simple",
        ],
    )

    assert result.exit_code == 0, f"Command failed with output: {result.output}"

    # simple mode prints each object key as it downloads
    assert "sequences/image_000.jpg" in result.output

    # simple mode does NOT print a summary
    assert "Objects found:" not in result.output
    assert "Total data:" not in result.output

    downloaded_files = list(tmp_path.glob("image_*.jpg"))
    assert len(downloaded_files) == 100, (
        f"Expected 100 files but found {len(downloaded_files)}"
    )

    for file_path in downloaded_files[:5]:
        assert file_path.stat().st_size > 0, f"File {file_path.name} is empty"

    assert not (tmp_path / "sequences").exists(), (
        "Files should be at root of download dir, not in 'sequences/' subdirectory"
    )


def test_progress_tracking_detailed(test_bucket, tmp_path):
    """Test detailed progress mode shows object keys and a final summary.

    Downloads from the sequences/ prefix with --progress detailed to verify
    both per-object output and the summary are shown.
    """
    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            f"s3://{test_bucket['bucket']}/sequences/",
            "--download-dir",
            str(tmp_path),
            "--region",
            test_bucket["region"],
            "--progress",
            "detailed",
        ],
    )

    assert result.exit_code == 0, f"Command failed with output: {result.output}"

    assert "Objects found:" in result.output
    assert "Objects downloaded:" in result.output
    assert "Total data:" in result.output
    assert "Average speed:" in result.output
    assert "Total time:" in result.output

    downloaded_files = list(tmp_path.glob("image_*.jpg"))
    assert len(downloaded_files) == 100, (
        f"Expected 100 files but found {len(downloaded_files)}"
    )


def test_download_with_custom_directory(test_bucket, tmp_path):
    """Test downloading files to a custom directory."""
    runner = CliRunner()

    custom_dir = tmp_path / "my_custom_downloads"
    custom_dir.mkdir()

    result = runner.invoke(
        cli,
        [
            f"s3://{test_bucket['bucket']}/small/",
            "--download-dir",
            str(custom_dir),
            "--region",
            test_bucket["region"],
            "--progress",
            "detailed",
        ],
    )

    assert result.exit_code == 0, f"Command failed with output: {result.output}"

    assert "Objects found:" in result.output
    assert "Objects downloaded:" in result.output

    assert custom_dir.exists(), f"Custom directory {custom_dir} was not created"
    assert custom_dir.is_dir(), f"{custom_dir} is not a directory"

    downloaded_files = list(custom_dir.glob("file_*.txt"))
    assert len(downloaded_files) == 120, (
        f"Expected 120 files in custom directory but found {len(downloaded_files)}"
    )

    parent_files = list(tmp_path.glob("file_*.txt"))
    assert len(parent_files) == 0, (
        f"Found {len(parent_files)} files in parent directory; "
        "files should only be in custom directory"
    )

    for i in [0, 50, 119]:
        file_path = custom_dir / f"file_{i:03d}.txt"
        assert file_path.exists(), f"File {file_path.name} doesn't exist"
        assert file_path.stat().st_size > 0, f"File {file_path.name} is empty"


def test_multi_threaded_download_e2e(test_bucket, tmp_path):
    """Test multi-threaded download with concurrent workers.

    Downloads from the medium/ prefix (80 files, 1-10MB each) using multiple
    threads to verify the core multi-threading architecture works correctly.
    """
    runner = CliRunner()

    result = runner.invoke(
        cli,
        [
            f"s3://{test_bucket['bucket']}/medium/",
            "--download-dir",
            str(tmp_path),
            "--region",
            test_bucket["region"],
            "--threads",
            "10",
            "--progress",
            "detailed",
        ],
    )

    assert result.exit_code == 0, f"Command failed with output: {result.output}"

    assert "Objects found:" in result.output
    assert "Objects downloaded:" in result.output
    assert "Total data:" in result.output
    assert "Average speed:" in result.output
    assert "Total time:" in result.output

    downloaded_files = list(tmp_path.glob("data_*.jpg"))
    assert len(downloaded_files) == 80, (
        f"Expected 80 files but found {len(downloaded_files)}"
    )

    expected_files = {f"data_{i:03d}.jpg" for i in range(80)}
    actual_files = {f.name for f in downloaded_files}
    assert expected_files == actual_files, (
        f"File name mismatch. Missing: {expected_files - actual_files}, "
        f"Extra: {actual_files - expected_files}"
    )

    for i in [0, 39, 79]:
        file_path = tmp_path / f"data_{i:03d}.jpg"
        assert file_path.exists(), f"File {file_path.name} doesn't exist"
        assert file_path.stat().st_size > 0, f"File {file_path.name} is empty"
        assert file_path.stat().st_size >= 1024 * 1024, (
            f"File {file_path.name} is smaller than expected (< 1MB)"
        )

    assert not (tmp_path / "medium").exists(), (
        "Files should be at root of download dir, not in 'medium/' subdirectory"
    )
