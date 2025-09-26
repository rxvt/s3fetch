import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from click import BadParameter
from click.testing import CliRunner

from s3fetch import __version__ as version
from s3fetch.cli import (
    cli,
    validate_aws_region,
    validate_download_directory,
    validate_regex_pattern,
    validate_s3_uri,
    validate_thread_count,
)


def test_printing_version():
    runner = CliRunner()
    prog_name = runner.get_default_prog_name(cli)
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert result.output == f"{prog_name}, version {version}\n"


class TestValidateS3Uri:
    """Test cases for validate_s3_uri function."""

    def test_valid_s3_uri_bucket_only(self):
        """Test that valid S3 URI with bucket only passes validation."""
        validate_s3_uri("s3://my-bucket")  # Should not raise

    def test_valid_s3_uri_with_prefix(self):
        """Test that valid S3 URI with prefix passes validation."""
        validate_s3_uri("s3://my-bucket/path/to/objects")  # Should not raise

    def test_invalid_uri_missing_s3_prefix(self):
        """Test that URI without s3:// prefix raises BadParameter."""
        with pytest.raises(BadParameter) as exc_info:
            validate_s3_uri("http://my-bucket/path")
        assert "S3 URI must start with 's3://'" in str(exc_info.value)

    def test_invalid_uri_just_s3_prefix(self):
        """Test that URI with only s3:// raises BadParameter."""
        with pytest.raises(BadParameter) as exc_info:
            validate_s3_uri("s3://")
        assert "S3 URI must include a bucket name" in str(exc_info.value)

    def test_invalid_uri_empty_after_prefix(self):
        """Test that URI with s3:// but empty bucket name raises BadParameter."""
        with pytest.raises(BadParameter) as exc_info:
            validate_s3_uri("s3://   ")  # Just whitespace
        assert "S3 URI must include a valid bucket name" in str(exc_info.value)


class TestValidateRegexPattern:
    """Test cases for validate_regex_pattern function."""

    def test_valid_regex_pattern(self):
        """Test that valid regex patterns pass validation."""
        validate_regex_pattern(r"\.jpg$")  # Should not raise
        validate_regex_pattern(r"^prefix.*")  # Should not raise
        validate_regex_pattern(r"[a-zA-Z0-9]+")  # Should not raise

    def test_none_regex_pattern(self):
        """Test that None regex pattern is allowed."""
        validate_regex_pattern(None)  # Should not raise

    def test_empty_regex_pattern(self):
        """Test that empty regex pattern is valid."""
        validate_regex_pattern("")  # Should not raise

    def test_invalid_regex_pattern(self):
        """Test that invalid regex pattern raises BadParameter."""
        with pytest.raises(BadParameter) as exc_info:
            validate_regex_pattern(r"[invalid")  # Missing closing bracket
        assert "Invalid regular expression" in str(exc_info.value)
        assert "Pattern: [invalid" in str(exc_info.value)

    def test_invalid_regex_pattern_with_details(self):
        """Test that invalid regex shows specific error details."""
        with pytest.raises(BadParameter) as exc_info:
            validate_regex_pattern(r"(?P<invalid)")  # Invalid named group
        assert "Invalid regular expression" in str(exc_info.value)


class TestValidateThreadCount:
    """Test cases for validate_thread_count function."""

    def test_valid_thread_counts(self):
        """Test that valid thread counts pass validation."""
        validate_thread_count(1)  # Minimum valid
        validate_thread_count(4)  # Typical value
        validate_thread_count(100)  # High but reasonable
        validate_thread_count(1000)  # Maximum valid

    def test_none_thread_count(self):
        """Test that None thread count is allowed."""
        validate_thread_count(None)  # Should not raise

    def test_invalid_thread_count_zero(self):
        """Test that zero thread count raises BadParameter."""
        with pytest.raises(BadParameter) as exc_info:
            validate_thread_count(0)
        assert "Thread count must be at least 1" in str(exc_info.value)
        assert "Got: 0" in str(exc_info.value)

    def test_invalid_thread_count_negative(self):
        """Test that negative thread count raises BadParameter."""
        with pytest.raises(BadParameter) as exc_info:
            validate_thread_count(-5)
        assert "Thread count must be at least 1" in str(exc_info.value)
        assert "Got: -5" in str(exc_info.value)

    def test_invalid_thread_count_too_high(self):
        """Test that thread count over 1000 raises BadParameter."""
        with pytest.raises(BadParameter) as exc_info:
            validate_thread_count(1001)
        assert "Thread count must be 1000 or less" in str(exc_info.value)
        assert "Got: 1001" in str(exc_info.value)
        assert "AWS rate limits" in str(exc_info.value)


class TestValidateAwsRegion:
    """Test cases for validate_aws_region function."""

    def test_valid_aws_regions(self):
        """Test that valid AWS region formats pass validation."""
        with patch("s3fetch.cli.logger.warning") as mock_warning:
            validate_aws_region("us-east-1")
            validate_aws_region("eu-west-2")
            validate_aws_region("ap-southeast-1")
            validate_aws_region("ca-central-1")
            # Should not trigger any warnings
            mock_warning.assert_not_called()

    def test_invalid_region_format_warning(self):
        """Test that invalid region format triggers warning."""
        with patch("s3fetch.cli.logger.warning") as mock_warning:
            validate_aws_region("invalid-region")
            mock_warning.assert_called_once()
            call_args = mock_warning.call_args[0][0]
            assert "doesn't match typical AWS region format" in call_args

    def test_empty_region_format_warning(self):
        """Test that empty region triggers warning."""
        with patch("s3fetch.cli.logger.warning") as mock_warning:
            validate_aws_region("")
            mock_warning.assert_called_once()


class TestValidateDownloadDirectory:
    """Test cases for validate_download_directory function."""

    def test_none_download_directory(self):
        """Test that None download directory is allowed."""
        validate_download_directory(None)  # Should not raise

    def test_valid_download_directory(self):
        """Test that valid existing directory passes validation."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            validate_download_directory(Path(tmp_dir))  # Should not raise

    def test_nonexistent_download_directory(self):
        """Test that non-existent directory raises BadParameter."""
        non_existent_path = Path("/this/path/does/not/exist")
        with pytest.raises(BadParameter) as exc_info:
            validate_download_directory(non_existent_path)
        assert "Download directory does not exist" in str(exc_info.value)
        assert str(non_existent_path) in str(exc_info.value)

    def test_download_directory_is_file(self):
        """Test that passing a file instead of directory raises BadParameter."""
        with tempfile.NamedTemporaryFile() as tmp_file:
            file_path = Path(tmp_file.name)
            with pytest.raises(BadParameter) as exc_info:
                validate_download_directory(file_path)
            assert "Path exists but is not a directory" in str(exc_info.value)

    def test_download_directory_permission_error(self):
        """Test that inaccessible directory raises BadParameter."""
        # Create a mock Path that raises PermissionError on resolve()
        mock_path = Mock(spec=Path)
        mock_path.exists.return_value = True
        mock_path.is_dir.return_value = True
        mock_path.resolve.side_effect = PermissionError("Access denied")

        with pytest.raises(BadParameter) as exc_info:
            validate_download_directory(mock_path)
        assert "Cannot access download directory" in str(exc_info.value)


class TestCLIValidationIntegration:
    """Test CLI properly calls validation functions and handles validation errors."""

    def test_cli_invalid_s3_uri(self):
        """Test that CLI with invalid S3 URI shows proper error."""
        runner = CliRunner()
        result = runner.invoke(cli, ["invalid-uri"])
        assert result.exit_code == 2  # Click parameter validation error
        assert "S3 URI must start with 's3://'" in result.output

    def test_cli_invalid_regex(self):
        """Test that CLI with invalid regex shows proper error."""
        runner = CliRunner()
        result = runner.invoke(cli, ["s3://test-bucket", "--regex", "[invalid"])
        assert result.exit_code == 2
        assert "Invalid regular expression" in result.output

    def test_cli_invalid_thread_count(self):
        """Test that CLI with invalid thread count shows proper error."""
        runner = CliRunner()
        result = runner.invoke(cli, ["s3://test-bucket", "--threads", "-1"])
        assert result.exit_code == 2
        assert "Thread count must be at least 1" in result.output

    def test_cli_invalid_download_dir(self):
        """Test that CLI with invalid download directory shows proper error."""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["s3://test-bucket", "--download-dir", "/nonexistent/path"]
        )
        assert result.exit_code == 2
        assert "Download directory does not exist" in result.output
