import tempfile
import threading
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
from botocore.exceptions import ClientError
from click import BadParameter
from click.testing import CliRunner

from s3fetch import __version__ as version
from s3fetch.cli import (
    _handle_client_error,
    _setup_progress_display,
    _stop_progress_display,
    _validate_progress_mode,
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
        validate_thread_count(1000)  # At the warning threshold
        validate_thread_count(9999)  # Above threshold — warns but does not raise

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

    def test_thread_count_too_high_prints_warning(self, capsys):
        """Test that thread count over 1000 prints a warning to stderr but does not raise."""  # noqa: E501
        validate_thread_count(1001)
        captured = capsys.readouterr()
        assert "Warning" in captured.err
        assert "1001" in captured.err
        assert "AWS rate limits" in captured.err


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


class TestValidateProgressMode:
    """Test cases for _validate_progress_mode function."""

    def test_quiet_and_progress_raises_usage_error(self):
        """--quiet and --progress together raise a UsageError."""
        import click

        with pytest.raises(click.UsageError, match="mutually exclusive"):
            _validate_progress_mode(quiet=True, progress="simple")

    def test_none_progress_defaults_to_simple(self):
        """None progress resolves to 'simple'."""
        result = _validate_progress_mode(quiet=False, progress=None)
        assert result == "simple"

    def test_explicit_progress_is_returned_unchanged(self):
        """An explicit progress mode is returned as-is."""
        for mode in ("simple", "detailed", "live-update"):
            assert _validate_progress_mode(quiet=False, progress=mode) == mode

    def test_fancy_without_rich_raises_usage_error(self):
        """--progress fancy without rich installed raises UsageError."""
        import click

        with patch("importlib.util.find_spec", return_value=None):
            with pytest.raises(
                click.UsageError, match="pip install s3fetch\\[fancy\\]"
            ):
                _validate_progress_mode(quiet=False, progress="fancy")

    def test_fancy_with_rich_installed_returns_fancy(self):
        """--progress fancy with rich available returns 'fancy'."""
        with patch("importlib.util.find_spec", return_value=object()):
            result = _validate_progress_mode(quiet=False, progress="fancy")
        assert result == "fancy"

    def test_quiet_without_progress_is_allowed(self):
        """--quiet alone (no --progress) is valid and returns 'simple'."""
        result = _validate_progress_mode(quiet=True, progress=None)
        assert result == "simple"


class TestSetupProgressDisplay:
    """Test cases for _setup_progress_display function."""

    def _make_queues_and_event(self) -> tuple:
        from s3fetch.s3 import DownloadResult, S3FetchQueue

        completed_queue: S3FetchQueue[DownloadResult] = S3FetchQueue()
        exit_event = threading.Event()
        return completed_queue, exit_event

    def test_simple_mode_starts_completed_objects_thread(self):
        """Simple mode wires up per-object printing and returns None."""
        completed_queue, exit_event = self._make_queues_and_event()
        with patch("s3fetch.cli.api._create_completed_objects_thread") as mock_thread:
            result = _setup_progress_display(
                progress="simple",
                quiet=False,
                progress_tracker=None,
                completed_queue=completed_queue,
                exit_event=exit_event,
            )
        mock_thread.assert_called_once()
        assert result is None

    def test_quiet_simple_mode_skips_per_object_printing(self):
        """Quiet + simple mode does not start the per-object printing thread."""
        completed_queue, exit_event = self._make_queues_and_event()
        with patch("s3fetch.cli.api._create_completed_objects_thread") as mock_thread:
            result = _setup_progress_display(
                progress="simple",
                quiet=True,
                progress_tracker=None,
                completed_queue=completed_queue,
                exit_event=exit_event,
            )
        mock_thread.assert_not_called()
        assert result is None

    def test_live_update_mode_returns_thread(self):
        """live-update mode starts a background monitoring thread."""
        from s3fetch.utils import ProgressTracker

        completed_queue, exit_event = self._make_queues_and_event()
        tracker = ProgressTracker()
        with patch("s3fetch.cli.start_progress_monitoring") as mock_monitor:
            mock_monitor.return_value = MagicMock(spec=threading.Thread)
            result = _setup_progress_display(
                progress="live-update",
                quiet=False,
                progress_tracker=tracker,
                completed_queue=completed_queue,
                exit_event=exit_event,
            )
        mock_monitor.assert_called_once_with(tracker, exit_event)
        assert result is mock_monitor.return_value

    def test_fancy_mode_returns_thread(self):
        """Fancy mode starts a background Rich progress thread."""
        from s3fetch.utils import ProgressTracker

        completed_queue, exit_event = self._make_queues_and_event()
        tracker = ProgressTracker()
        with patch("s3fetch.cli.start_fancy_progress") as mock_fancy:
            mock_fancy.return_value = MagicMock(spec=threading.Thread)
            result = _setup_progress_display(
                progress="fancy",
                quiet=False,
                progress_tracker=tracker,
                completed_queue=completed_queue,
                exit_event=exit_event,
            )
        mock_fancy.assert_called_once_with(tracker, exit_event)
        assert result is mock_fancy.return_value

    def test_no_progress_thread_when_no_tracker(self):
        """live-update with no tracker returns None (no background thread)."""
        completed_queue, exit_event = self._make_queues_and_event()
        with patch("s3fetch.cli.start_progress_monitoring") as mock_monitor:
            result = _setup_progress_display(
                progress="live-update",
                quiet=False,
                progress_tracker=None,
                completed_queue=completed_queue,
                exit_event=exit_event,
            )
        mock_monitor.assert_not_called()
        assert result is None


class TestStopProgressDisplay:
    """Test cases for _stop_progress_display function."""

    def test_none_thread_is_noop(self):
        """Calling with no thread (None) does nothing."""
        exit_event = threading.Event()
        _stop_progress_display(
            progress="simple", progress_thread=None, exit_event=exit_event
        )
        assert not exit_event.is_set()  # Exit event not touched

    def test_sets_exit_event_and_joins_thread(self):
        """A live background thread is joined and exit event is set."""
        exit_event = threading.Event()
        mock_thread = MagicMock(spec=threading.Thread)
        _stop_progress_display(
            progress="detailed", progress_thread=mock_thread, exit_event=exit_event
        )
        assert exit_event.is_set()
        mock_thread.join.assert_called_once_with(timeout=1)

    def test_live_update_writes_newline(self, capsys):
        """live-update mode writes a trailing newline after stopping."""
        exit_event = threading.Event()
        mock_thread = MagicMock(spec=threading.Thread)
        _stop_progress_display(
            progress="live-update", progress_thread=mock_thread, exit_event=exit_event
        )
        captured = capsys.readouterr()
        assert "\n" in captured.out

    def test_fancy_mode_does_not_write_newline(self, capsys):
        """Fancy mode does not write an extra newline after stopping."""
        exit_event = threading.Event()
        mock_thread = MagicMock(spec=threading.Thread)
        _stop_progress_display(
            progress="fancy", progress_thread=mock_thread, exit_event=exit_event
        )
        captured = capsys.readouterr()
        assert captured.out == ""


def _make_client_error(code: str, message: str = "test error") -> ClientError:
    """Build a botocore ClientError with the given error code."""
    return ClientError(
        {"Error": {"Code": code, "Message": message}},
        "ListObjectsV2",
    )


class TestHandleClientError:
    """Test cases for _handle_client_error function."""

    def test_no_such_bucket(self, capsys):
        """NoSuchBucket prints bucket-specific suggestions and exits."""
        with pytest.raises(SystemExit) as exc_info:
            _handle_client_error(
                _make_client_error("NoSuchBucket"), "s3://my-bucket/", "us-east-1"
            )
        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "S3 bucket does not exist" in captured.out
        assert "Double-check the bucket name" in captured.out

    def test_no_such_bucket_includes_aws_cli_hint(self, capsys):
        """NoSuchBucket includes an aws s3 ls suggestion with the bucket URI."""
        with pytest.raises(SystemExit):
            _handle_client_error(
                _make_client_error("NoSuchBucket"),
                "s3://my-bucket/prefix/",
                "eu-west-1",
            )
        captured = capsys.readouterr()
        assert "aws s3 ls s3://my-bucket" in captured.out
        assert "--region eu-west-1" in captured.out

    def test_access_denied(self, capsys):
        """AccessDenied prints permission-related suggestions and exits."""
        with pytest.raises(SystemExit) as exc_info:
            _handle_client_error(
                _make_client_error("AccessDenied"), "s3://my-bucket/", "us-east-1"
            )
        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "Access denied" in captured.out
        assert "aws sts get-caller-identity" in captured.out

    def test_invalid_access_key(self, capsys):
        """InvalidAccessKeyId prints credential suggestions and exits."""
        with pytest.raises(SystemExit) as exc_info:
            _handle_client_error(
                _make_client_error("InvalidAccessKeyId"), "s3://my-bucket/", "us-east-1"
            )
        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "Invalid AWS access key ID" in captured.out
        assert "aws configure" in captured.out

    def test_signature_does_not_match(self, capsys):
        """SignatureDoesNotMatch prints secret key suggestions and exits."""
        with pytest.raises(SystemExit) as exc_info:
            _handle_client_error(
                _make_client_error("SignatureDoesNotMatch"),
                "s3://my-bucket/",
                "us-east-1",
            )
        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "Invalid AWS secret access key" in captured.out

    def test_no_credentials(self, capsys):
        """NoCredentialsError prints setup instructions and exits."""
        with pytest.raises(SystemExit) as exc_info:
            _handle_client_error(
                _make_client_error("NoCredentialsError"), "s3://my-bucket/", "us-east-1"
            )
        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "No AWS credentials found" in captured.out

    def test_unknown_error_code(self, capsys):
        """Unknown error codes print a generic troubleshooting message and exit."""
        with pytest.raises(SystemExit) as exc_info:
            _handle_client_error(
                _make_client_error("InternalError", "something broke"),
                "s3://my-bucket/",
                "us-east-1",
            )
        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "AWS API error" in captured.out
        assert "InternalError" in captured.out


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

    def test_cli_invalid_progress_option(self):
        """Test that CLI with invalid --progress value shows proper error."""
        runner = CliRunner()
        result = runner.invoke(cli, ["s3://test-bucket", "--progress", "none"])
        assert result.exit_code == 2
        assert "Invalid value for '--progress'" in result.output

    def test_cli_quiet_and_progress_mutually_exclusive(self):
        """Test that --quiet and --progress together raise a UsageError."""
        runner = CliRunner()
        result = runner.invoke(
            cli, ["s3://test-bucket", "--quiet", "--progress", "detailed"]
        )
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output

    def test_cli_valid_progress_choices(self):
        """Test that all valid --progress choices are accepted by Click."""
        runner = CliRunner()
        for choice in ("simple", "detailed", "live-update", "fancy"):
            # Invoke with a bad URI so it fails fast after option parsing —
            # we just want to confirm the choice itself is accepted (exit 2
            # would mean a Click option error, not a URI error).
            result = runner.invoke(cli, ["invalid-uri", "--progress", choice])
            assert "Invalid value for '--progress'" not in result.output
