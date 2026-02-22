"""Integration tests for CLI error handling."""

import tempfile
from unittest.mock import patch

from botocore.exceptions import ClientError
from click.testing import CliRunner
from moto import mock_aws

from s3fetch.cli import cli
from s3fetch.exceptions import S3FetchError


def _make_client_error(code: str, message: str = "test error") -> ClientError:
    """Build a botocore ClientError with the given error code."""
    return ClientError(
        {"Error": {"Code": code, "Message": message}},
        "ListObjectsV2",
    )


@mock_aws
def test_cli_error_no_such_bucket():
    """NoSuchBucket ClientError shows a helpful error message."""
    runner = CliRunner()
    with patch(
        "s3fetch.cli.list_objects", side_effect=_make_client_error("NoSuchBucket")
    ):
        result = runner.invoke(cli, ["s3://no-such-bucket/"])

    assert result.exit_code == 1
    assert "S3 bucket does not exist" in result.output
    assert "Double-check the bucket name" in result.output


@mock_aws
def test_cli_error_access_denied():
    """AccessDenied ClientError shows a helpful error message."""
    runner = CliRunner()
    with patch(
        "s3fetch.cli.list_objects", side_effect=_make_client_error("AccessDenied")
    ):
        result = runner.invoke(cli, ["s3://some-bucket/"])

    assert result.exit_code == 1
    assert "Access denied" in result.output
    assert "aws sts get-caller-identity" in result.output


@mock_aws
def test_cli_error_invalid_access_key():
    """InvalidAccessKeyId ClientError shows a helpful error message."""
    runner = CliRunner()
    with patch(
        "s3fetch.cli.list_objects",
        side_effect=_make_client_error("InvalidAccessKeyId"),
    ):
        result = runner.invoke(cli, ["s3://some-bucket/"])

    assert result.exit_code == 1
    assert "Invalid AWS access key ID" in result.output
    assert "aws configure" in result.output


@mock_aws
def test_cli_error_signature_does_not_match():
    """SignatureDoesNotMatch ClientError shows a helpful error message."""
    runner = CliRunner()
    with patch(
        "s3fetch.cli.list_objects",
        side_effect=_make_client_error("SignatureDoesNotMatch"),
    ):
        result = runner.invoke(cli, ["s3://some-bucket/"])

    assert result.exit_code == 1
    assert "Invalid AWS secret access key" in result.output
    assert "AWS_SECRET_ACCESS_KEY" in result.output


@mock_aws
def test_cli_error_no_credentials():
    """NoCredentialsError ClientError shows a helpful error message."""
    runner = CliRunner()
    with patch(
        "s3fetch.cli.list_objects",
        side_effect=_make_client_error("NoCredentialsError"),
    ):
        result = runner.invoke(cli, ["s3://some-bucket/"])

    assert result.exit_code == 1
    assert "No AWS credentials found" in result.output
    assert "aws configure" in result.output


@mock_aws
def test_cli_error_unknown_client_error():
    """Unknown ClientError codes show a generic troubleshooting message."""
    runner = CliRunner()
    with patch(
        "s3fetch.cli.list_objects",
        side_effect=_make_client_error("InternalError", "Something went wrong"),
    ):
        result = runner.invoke(cli, ["s3://some-bucket/"])

    assert result.exit_code == 1
    assert "AWS API error" in result.output
    assert "InternalError" in result.output


@mock_aws
def test_cli_error_s3fetch_error_with_message():
    """S3FetchError with a message displays the message."""
    runner = CliRunner()
    with tempfile.TemporaryDirectory() as tmpdir:
        with patch(
            "s3fetch.cli.list_objects",
            side_effect=S3FetchError("something went wrong internally"),
        ):
            result = runner.invoke(cli, ["s3://some-bucket/", "--download-dir", tmpdir])

    assert result.exit_code == 1
    assert "something went wrong internally" in result.output


@mock_aws
def test_cli_error_s3fetch_error_without_message():
    """S3FetchError without a message shows the generic fallback."""
    runner = CliRunner()
    with tempfile.TemporaryDirectory() as tmpdir:
        with patch(
            "s3fetch.cli.list_objects",
            side_effect=S3FetchError(),
        ):
            result = runner.invoke(cli, ["s3://some-bucket/", "--download-dir", tmpdir])

    assert result.exit_code == 1
    assert "unexpected error" in result.output
