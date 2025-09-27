"""Command line interface for S3Fetch."""

import logging
import re
import sys
import threading
from pathlib import Path
from typing import Optional

import click
from botocore.exceptions import ClientError
from mypy_boto3_s3.client import S3Client

from . import api, aws, s3, utils
from .api import S3FetchQueue
from .exceptions import S3FetchError
from .utils import custom_print as print

logger = logging.getLogger(__name__)


def validate_s3_uri(s3_uri: str) -> None:
    """Validate that the S3 URI follows the correct format.

    Args:
        s3_uri (str): The S3 URI to validate.

    Raises:
        click.BadParameter: If the S3 URI format is invalid.
    """
    if not s3_uri.startswith("s3://"):
        raise click.BadParameter(
            f"S3 URI must start with 's3://'. Got: {s3_uri}\n"
            "Example: s3://my-bucket or s3://my-bucket/my/prefix"
        )

    # Remove s3:// prefix and check for valid bucket name
    uri_without_schema = s3_uri[5:]  # Remove "s3://"
    if not uri_without_schema:
        raise click.BadParameter(
            "S3 URI must include a bucket name after 's3://'.\n"
            "Example: s3://my-bucket or s3://my-bucket/my/prefix"
        )

    # The bucket name is everything before the first delimiter
    # We'll do basic validation here, more detailed parsing happens later with the
    # actual delimiter
    if not uri_without_schema.strip():
        raise click.BadParameter(
            "S3 URI must include a valid bucket name.\n"
            "Example: s3://my-bucket or s3://my-bucket/my/prefix"
        )


def validate_regex_pattern(regex: Optional[str]) -> None:
    """Validate that the regex pattern can be compiled.

    Args:
        regex (Optional[str]): The regex pattern to validate.

    Raises:
        click.BadParameter: If the regex pattern is invalid.
    """
    if regex is not None:
        try:
            re.compile(regex)
        except re.error as e:
            raise click.BadParameter(
                f"Invalid regular expression: {e}\n"
                f"Pattern: {regex}\n"
                "Please check your regex syntax and try again."
            ) from None


def validate_thread_count(threads: Optional[int]) -> None:
    """Validate that the thread count is within reasonable bounds.

    Args:
        threads (Optional[int]): The thread count to validate.

    Raises:
        click.BadParameter: If the thread count is invalid.
    """
    if threads is not None:
        if threads < 1:
            raise click.BadParameter(f"Thread count must be at least 1. Got: {threads}")
        if threads > 1000:
            raise click.BadParameter(
                f"Thread count must be 1000 or less. Got: {threads}\n"
                "Using too many threads may overwhelm your system or hit AWS rate"
                " limits."
            )


def validate_aws_region(region: str) -> None:
    """Validate that the AWS region follows expected format.

    Args:
        region (str): The AWS region to validate.

    Raises:
        click.BadParameter: If the region format seems invalid.
    """
    # Basic AWS region format validation (not exhaustive, but catches obvious errors)
    region_pattern = r"^[a-z]{2,3}-[a-z]+-\d+$"
    if not re.match(region_pattern, region):
        # Don't fail hard on this, just warn, as AWS may add new region formats
        logger.warning(
            f"Region '{region}' doesn't match typical AWS region format"
            " (e.g., 'us-east-1'). This may cause connection issues if the region is "
            "invalid."
        )


def validate_download_directory(download_dir: Optional[Path]) -> None:
    """Validate that the download directory exists and is accessible.

    Args:
        download_dir (Optional[Path]): The download directory to validate.

    Raises:
        click.BadParameter: If the directory doesn't exist or isn't accessible.
    """
    if download_dir is not None:
        if not download_dir.exists():
            raise click.BadParameter(
                f"Download directory does not exist: {download_dir}\n"
                "Please create the directory or specify a different path."
            )
        if not download_dir.is_dir():
            raise click.BadParameter(
                f"Path exists but is not a directory: {download_dir}\n"
                "Please specify a valid directory path."
            )
        # Check basic permissions
        try:
            download_dir.resolve()  # This will raise if we can't access it
        except (OSError, PermissionError):
            raise click.BadParameter(
                f"Cannot access download directory: {download_dir}\n"
                "Please check permissions and try again."
            ) from None


@click.command(name="S3Fetch")
@click.version_option()
@click.argument("s3_uri", type=str)
@click.option(
    "--region",
    type=str,
    default="us-east-1",
    help="Bucket region. Defaults to 'us-east-1'.",
)
@click.option("-d", "--debug", is_flag=True, help="Enable debug output.")
@click.option(
    "--download-dir",
    type=Path,
    help="Download directory. Defaults to current directory.",
)
@click.option(
    "-r", "--regex", type=str, help="Filter list of available objects by regex."
)
@click.option(
    "-t",
    "--threads",
    type=int,
    help="Number of threads to use. Defaults to available core count.",
)
@click.option(
    "--dry-run",
    "--list-only",
    is_flag=True,
    help="List objects only, but will create local directories.",
)
@click.option(
    "--delimiter",
    type=str,
    default="/",
    help="Specify the \"directory\" delimiter. Defaults to '/'.",
)
@click.option("-q", "--quiet", is_flag=True, help="Don't print to stdout.")
def cli(
    s3_uri: str,
    region: str,
    debug: bool,
    download_dir: Path,
    regex: str,
    threads: int,
    dry_run: bool,
    delimiter: str,
    quiet: bool,
) -> None:
    """Easily download objects from an S3 bucket.

    Example: s3fetch s3://my-test-bucket/my/birthday-photos/2020-01-01

    The above will download all S3 objects located under the
    `my/birthday-photos/2020-01-01` prefix.

    You can download all objects in a bucket by using `s3fetch s3://my-test-bucket/`
    """
    # Configure logging for CLI usage
    setup_logging(debug)

    # Validate all input parameters before proceeding
    validate_s3_uri(s3_uri)
    validate_regex_pattern(regex)
    validate_thread_count(threads)
    validate_aws_region(region)
    validate_download_directory(download_dir)

    run_cli(
        s3_uri=s3_uri,
        region=region,
        debug=debug,
        download_dir=download_dir,
        regex=regex,
        threads=threads,
        dry_run=dry_run,
        delimiter=delimiter,
        quiet=quiet,
    )


def setup_logging(debug: bool = False) -> None:
    """Configure logging for CLI usage."""
    level = logging.DEBUG if debug else logging.WARNING
    logging.basicConfig(level=level)


def prepare_download_dir_and_prefix(
    download_dir: Path, s3_uri: str, delimiter: str
) -> tuple[Path, str, str]:
    """Set up the download directory and split the S3 URI into bucket and prefix.

    Args:
        download_dir (Path): Directory to download files into.
        s3_uri (str): The S3 URI to fetch from.
        delimiter (str): Delimiter for S3 object keys.

    Returns:
        tuple[Path, str, str]: The download directory, bucket, and prefix.
    """
    download_dir = utils.set_download_dir(download_dir)
    bucket, prefix = s3.split_uri_into_bucket_and_prefix(s3_uri, delimiter)
    return download_dir, bucket, prefix


def get_thread_and_pool_size(threads: int) -> tuple[int, int]:
    """Determine thread count and connection pool size.

    Args:
        threads (int): Number of threads to use for downloading.

    Returns:
        tuple[int, int]: The thread count and connection pool size.
    """
    if not threads:
        threads = utils.get_available_threads()
    conn_pool_size = aws.calc_connection_pool_size(
        threads,
        s3.DEFAULT_S3TRANSFER_CONCURRENCY,
    )
    return threads, conn_pool_size


def create_client_and_queues(
    region: str, conn_pool_size: int
) -> tuple[S3Client, S3FetchQueue, S3FetchQueue]:
    """Create the S3 client and download/completed queues using the factory function.

    Args:
        region (str): AWS region for the S3 bucket.
        conn_pool_size (int): Connection pool size for the S3 client.

    Returns:
        tuple[S3Client, S3FetchQueue, S3FetchQueue]:
            The S3 client, download queue, and completed queue.
    """
    download_queue = s3.get_queue("download")
    completed_queue = s3.get_queue("completion")
    client = aws.get_client(region, conn_pool_size)
    return client, download_queue, completed_queue


def list_objects(
    client: S3Client,
    download_queue: S3FetchQueue,
    bucket: str,
    prefix: str,
    delimiter: str,
    regex: str,
    exit_event: threading.Event,
) -> None:
    """List objects in the S3 bucket and add them to the download queue.

    Args:
        client (S3Client): The S3 client.
        download_queue (S3FetchQueue): Queue for objects to download.
        bucket (str): S3 bucket name.
        prefix (str): S3 prefix to filter objects.
        delimiter (str): Delimiter for S3 object keys.
        regex (str): Regex pattern to filter objects.
        exit_event (threading.Event): Event to signal exit.
    """
    api.list_objects(
        client=client,
        download_queue=download_queue,
        bucket=bucket,
        prefix=prefix,
        delimiter=delimiter,
        regex=regex,
        exit_event=exit_event,
    )


def download_objects(
    client: S3Client,
    threads: int,
    download_queue: S3FetchQueue,
    completed_queue: S3FetchQueue,
    exit_event: threading.Event,
    bucket: str,
    prefix: str,
    download_dir: Path,
    delimiter: str,
    download_config: dict,
    dry_run: bool,
) -> None:
    """Download objects from the S3 bucket using the provided configuration.

    Args:
        client (S3Client): The S3 client.
        threads (int): Number of threads to use for downloading.
        download_queue (S3FetchQueue): Queue for objects to download.
        completed_queue (S3FetchQueue): Queue for completed downloads.
        exit_event (threading.Event): Event to signal exit.
        bucket (str): S3 bucket name.
        prefix (str): S3 prefix to filter objects.
        download_dir (Path): Directory to download files into.
        delimiter (str): Delimiter for S3 object keys.
        download_config (dict): Download configuration.
        dry_run (bool): If True, only list objects without downloading.
    """
    api.download_objects(
        client=client,
        threads=threads,
        download_queue=download_queue,
        completed_queue=completed_queue,
        exit_event=exit_event,
        bucket=bucket,
        prefix=prefix,
        download_dir=download_dir,
        delimiter=delimiter,
        download_config=download_config,
        dry_run=dry_run,
    )


def run_cli(
    s3_uri: str,
    region: str,
    debug: bool,
    download_dir: Path,
    regex: str,
    threads: int,
    dry_run: bool,
    delimiter: str,
    quiet: bool,
) -> None:
    """Run the main S3Fetch CLI logic.

    Args:
        s3_uri (str): The S3 URI to fetch from.
        region (str): AWS region for the S3 bucket.
        debug (bool): Enable debug output if True.
        download_dir (Path): Directory to download files into.
        regex (str): Regex pattern to filter objects.
        threads (int): Number of threads to use for downloading.
        dry_run (bool): If True, only list objects without downloading.
        delimiter (str): Delimiter for S3 object keys.
        quiet (bool): If True, suppress output to stdout.
    """
    download_dir, bucket, prefix = prepare_download_dir_and_prefix(
        download_dir, s3_uri, delimiter
    )
    threads, conn_pool_size = get_thread_and_pool_size(threads)
    client, download_queue, completed_queue = create_client_and_queues(
        region, conn_pool_size
    )
    exit_event = utils.create_exit_event()

    print(f"Starting to list objects from {s3_uri}", quiet)
    try:
        list_objects(
            client, download_queue, bucket, prefix, delimiter, regex, exit_event
        )
        download_config = s3.create_download_config(callback=None)
        if not quiet:
            api.create_completed_objects_thread(
                queue=completed_queue,
                func=utils.print_completed_objects,
            )
        print("Starting to download objects", quiet)
        download_objects(
            client,
            threads,
            download_queue,
            completed_queue,
            exit_event,
            bucket,
            prefix,
            download_dir,
            delimiter,
            download_config,
            dry_run,
        )
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.", False)  # Always show, even with --quiet
        sys.exit(1)
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        if error_code == "NoSuchBucket":
            print(
                f"Error: S3 bucket does not exist in the specified URI: {s3_uri}", False
            )
            print("Please check the bucket name and try again.", False)
        elif error_code == "AccessDenied":
            print(f"Error: Access denied to S3 bucket: {s3_uri}", False)
            print("Please check your AWS credentials and bucket permissions.", False)
        elif error_code == "InvalidAccessKeyId":
            print("Error: Invalid AWS access key ID.", False)
            print("Please check your AWS credentials configuration.", False)
        elif error_code == "SignatureDoesNotMatch":
            print("Error: AWS signature mismatch - invalid secret access key.", False)
            print("Please check your AWS credentials configuration.", False)
        else:
            print(f"Error: AWS API error ({error_code}): {e}", False)
            print("Please check your AWS configuration and try again.", False)
        sys.exit(1)
    except S3FetchError as e:
        if e.args and str(e.args[0]):
            print(f"Error: {e.args[0]}", False)
        else:
            print("Error: An unexpected error occurred during S3 operation.", False)
        sys.exit(1)
