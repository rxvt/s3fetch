"""Command line interface for S3Fetch."""

import importlib.util
import logging
import re
import sys
import threading
import time
from pathlib import Path
from typing import Optional

import click
from botocore.exceptions import ClientError
from mypy_boto3_s3.client import S3Client

from . import api, aws, s3, utils
from .exceptions import S3FetchError
from .s3 import DownloadResult, S3FetchQueue
from .utils import ProgressProtocol, ProgressTracker, format_bytes
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
            f"S3 URI must start with 's3://'.\n"
            f"Got: {s3_uri}\n"
            f"Examples:\n"
            f"  s3://my-bucket/\n"
            f"  s3://my-bucket/folder/subfolder/"
        )

    # Remove s3:// prefix and check for valid bucket name
    uri_without_schema = s3_uri[5:]  # Remove "s3://"
    if not uri_without_schema:
        raise click.BadParameter(
            "S3 URI must include a bucket name after 's3://'.\n"
            "Examples:\n"
            "  s3://my-bucket/\n"
            "  s3://my-bucket/my/prefix/"
        )

    # Check for bucket name validation
    bucket_part = uri_without_schema.split("/")[0]
    if not bucket_part.strip():
        raise click.BadParameter(
            "S3 URI must include a valid bucket name.\n"
            "Examples:\n"
            "  s3://my-bucket/\n"
            "  s3://my-bucket/my/prefix/"
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
            click.echo(
                f"Warning: Thread count {threads} exceeds 1000. "
                "Using too many threads may overwhelm your system or hit AWS"
                " rate limits.",
                err=True,
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
    help="AWS region for the S3 bucket (e.g., us-east-1, eu-west-1). "
    "Defaults to 'us-east-1'.",
)
@click.option("-d", "--debug", is_flag=True, help="Enable verbose debug output.")
@click.option(
    "--download-dir",
    type=Path,
    help="Local directory to save downloaded files. Creates if missing. "
    "Defaults to current directory.",
)
@click.option(
    "-r",
    "--regex",
    type=str,
    help="Filter objects using regular expressions (e.g., '.*\\.jpg$' for JPEG files).",
)
@click.option(
    "-t",
    "--threads",
    type=int,
    help="Number of concurrent download threads (minimum 1). Defaults to CPU core count.",  # noqa: E501
)
@click.option(
    "--dry-run",
    "--list-only",
    is_flag=True,
    help="Show what would be downloaded without actually downloading files.",
)
@click.option(
    "--delimiter",
    type=str,
    default="/",
    help="Object key delimiter for path structure. Defaults to '/'.",
)
@click.option("-q", "--quiet", is_flag=True, help="Suppress all output except errors.")
@click.option(
    "--progress",
    type=click.Choice(
        ["simple", "detailed", "live-update", "fancy"], case_sensitive=False
    ),
    default=None,
    help=(
        "Progress display mode. 'simple' (default) prints each object key as it "
        "downloads. 'detailed' adds a summary at the end. "
        "'live-update' shows a real-time status line and summary "
        "(no per-object output). "
        "'fancy' shows a Rich progress bar and summary "
        "(requires: pip install s3fetch[fancy])."
    ),
)
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
    progress: str,
) -> None:
    """Concurrently download objects from S3 buckets.

    Examples:\n
      s3fetch s3://my-bucket/\n
      s3fetch s3://my-bucket/photos/ --regex ".*\\.jpg$"\n
      s3fetch s3://my-bucket/data/ --dry-run --threads 10\n
    """  # noqa: D301
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
        progress=progress,
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
) -> tuple[S3Client, "S3FetchQueue[str]", "S3FetchQueue[DownloadResult]"]:
    """Create the S3 client and download/completed queues using the factory function.

    Args:
        region (str): AWS region for the S3 bucket.
        conn_pool_size (int): Connection pool size for the S3 client.

    Returns:
        tuple[S3Client, S3FetchQueue[str], S3FetchQueue[DownloadResult]]:
            The S3 client, download queue, and completed queue.
    """
    download_queue: S3FetchQueue[str] = S3FetchQueue()
    completed_queue: S3FetchQueue[DownloadResult] = S3FetchQueue()
    client = aws.get_client(region, conn_pool_size)
    return client, download_queue, completed_queue


def start_progress_monitoring(
    progress_tracker: ProgressTracker, exit_event: threading.Event
) -> threading.Thread:
    r"""Start a background thread that overwrites a single status line every 2 seconds.

    Used by ``--progress live-update``. Per-object output must be suppressed by the
    caller so the ``\r`` overwrite is not disrupted by other prints.

    Args:
        progress_tracker: The ProgressTracker instance to monitor.
        exit_event: Event to signal when to stop monitoring.

    Returns:
        The monitoring thread.
    """

    def monitor_progress() -> None:
        """Overwrite a single status line with current progress every 2 seconds."""
        while not exit_event.is_set():
            stats = progress_tracker.get_stats()
            speed = stats["bytes_downloaded"] / max(stats["elapsed_time"], 0.001)
            sys.stdout.write(
                f"\r[Found: {stats['objects_found']} | "
                f"Downloaded: {stats['objects_downloaded']} | "
                f"Speed: {format_bytes(speed, suffix='/s')}]"
            )
            sys.stdout.flush()
            time.sleep(2)

    progress_thread = threading.Thread(target=monitor_progress, daemon=True)
    progress_thread.start()
    return progress_thread


def start_fancy_progress(
    progress_tracker: ProgressTracker, exit_event: threading.Event
) -> threading.Thread:
    """Start a Rich progress bar in a background thread.

    Used by ``--progress fancy``. Requires the ``rich`` package
    (``pip install s3fetch[fancy]``).  The caller is responsible for verifying
    that ``rich`` is importable before calling this function.

    Args:
        progress_tracker: The ProgressTracker instance to monitor.
        exit_event: Event to signal when to stop monitoring.

    Returns:
        The monitoring thread.
    """
    from rich.progress import (  # type: ignore[import]
        BarColumn,
        Progress,
        SpinnerColumn,
        TaskProgressColumn,
        TextColumn,
        TimeElapsedColumn,
        TransferSpeedColumn,
    )

    def run_fancy() -> None:
        """Drive a Rich Progress display from progress_tracker stats."""
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TransferSpeedColumn(),
            TimeElapsedColumn(),
        ) as rich_progress:
            task = rich_progress.add_task("Downloading...", total=None)
            while not exit_event.is_set():
                stats = progress_tracker.get_stats()
                rich_progress.update(
                    task,
                    completed=stats["objects_downloaded"],
                    total=stats["objects_found"] or None,
                    description=(
                        f"Downloading... "
                        f"[{stats['objects_downloaded']}/{stats['objects_found']}]"
                    ),
                )
                time.sleep(0.5)
            # Final update so the bar reaches 100% before the summary prints
            stats = progress_tracker.get_stats()
            rich_progress.update(
                task,
                completed=stats["objects_downloaded"],
                total=stats["objects_found"] or None,
                description=(
                    f"Downloaded "
                    f"[{stats['objects_downloaded']}/{stats['objects_found']}]"
                ),
            )

    progress_thread = threading.Thread(target=run_fancy, daemon=True)
    progress_thread.start()
    return progress_thread


def print_progress_summary(progress_tracker: ProgressTracker, quiet: bool) -> None:
    """Print final progress summary.

    Args:
        progress_tracker: The ProgressTracker instance with final stats
        quiet: Whether to suppress output
    """
    if progress_tracker is None:
        return

    stats = progress_tracker.get_stats()
    print("\nProgress Summary:", quiet)
    print(f"  Objects found: {stats['objects_found']}", quiet)
    print(f"  Objects downloaded: {stats['objects_downloaded']}", quiet)
    speed = stats["bytes_downloaded"] / max(stats["elapsed_time"], 0.001)
    print(f"  Total data: {format_bytes(stats['bytes_downloaded'])}", quiet)
    print(f"  Average speed: {format_bytes(speed, suffix='/s')}", quiet)
    print(f"  Total time: {stats['elapsed_time']:.1f} seconds", quiet)


def list_objects(
    client: S3Client,
    download_queue: "S3FetchQueue[str]",
    bucket: str,
    prefix: str,
    delimiter: str,
    regex: str,
    exit_event: threading.Event,
    progress_tracker: Optional[ProgressProtocol] = None,
) -> None:
    """List objects in the S3 bucket and add them to the download queue.

    Args:
        client (S3Client): The S3 client.
        download_queue (S3FetchQueue[str]): Queue for objects to download.
        bucket (str): S3 bucket name.
        prefix (str): S3 prefix to filter objects.
        delimiter (str): Delimiter for S3 object keys.
        regex (str): Regex pattern to filter objects.
        exit_event (threading.Event): Event to signal exit.
        progress_tracker (Optional[ProgressProtocol]): Progress tracking instance.
    """
    api._list_objects(
        client=client,
        download_queue=download_queue,
        bucket=bucket,
        prefix=prefix,
        delimiter=delimiter,
        regex=regex,
        exit_event=exit_event,
        progress_tracker=progress_tracker,
    )


def download_objects(
    client: S3Client,
    threads: int,
    download_queue: "S3FetchQueue[str]",
    completed_queue: "S3FetchQueue[DownloadResult]",
    exit_event: threading.Event,
    bucket: str,
    prefix: str,
    download_dir: Path,
    delimiter: str,
    download_config: dict,
    dry_run: bool,
    progress_tracker: Optional[ProgressProtocol] = None,
) -> None:
    """Download objects from the S3 bucket using the provided configuration.

    Args:
        client (S3Client): The S3 client.
        threads (int): Number of threads to use for downloading.
        download_queue (S3FetchQueue[str]): Queue for objects to download.
        completed_queue (S3FetchQueue[DownloadResult]): Queue for completed downloads.
        exit_event (threading.Event): Event to signal exit.
        bucket (str): S3 bucket name.
        prefix (str): S3 prefix to filter objects.
        download_dir (Path): Directory to download files into.
        delimiter (str): Delimiter for S3 object keys.
        download_config (dict): Download configuration.
        dry_run (bool): If True, only list objects without downloading.
        progress_tracker (Optional[ProgressProtocol]): Progress tracking instance.
    """
    api._download_objects(
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
        progress_tracker=progress_tracker,
    )


def _validate_progress_mode(quiet: bool, progress: Optional[str]) -> str:
    """Validate progress/quiet combination and return the resolved progress mode.

    Args:
        quiet (bool): Whether --quiet was specified.
        progress (Optional[str]): The --progress value, or None for the default.

    Returns:
        str: The resolved progress mode (defaults to 'simple').

    Raises:
        click.UsageError: If --quiet and --progress are both specified, or if
            --progress fancy is requested but the 'rich' package is not installed.
    """
    if quiet and progress is not None:
        raise click.UsageError("--quiet and --progress are mutually exclusive.")

    if progress is None:
        progress = "simple"

    if progress == "fancy" and importlib.util.find_spec("rich") is None:
        raise click.UsageError(
            "--progress fancy requires the 'fancy' extra. "
            "Install it with: pip install s3fetch[fancy]"
        )

    return progress


def _setup_progress_display(
    progress: str,
    quiet: bool,
    progress_tracker: Optional[ProgressTracker],
    completed_queue: "S3FetchQueue[DownloadResult]",
    exit_event: threading.Event,
) -> Optional[threading.Thread]:
    """Wire up per-object printing and any background progress display thread.

    Args:
        progress (str): The resolved progress mode.
        quiet (bool): Whether --quiet was specified.
        progress_tracker (Optional[ProgressTracker]): Tracker for stat-based modes.
        completed_queue (S3FetchQueue[DownloadResult]): Queue of completed downloads.
        exit_event (threading.Event): Event used to stop the background thread.

    Returns:
        Optional[threading.Thread]: The background progress thread, if one was started.
    """
    # Per-object key printing: simple and detailed modes only
    if not quiet and progress in ("simple", "detailed"):
        api._create_completed_objects_thread(
            queue=completed_queue,
            func=utils.print_completed_objects,
        )

    # Background display thread for live-update / fancy modes
    if progress == "live-update" and progress_tracker:
        return start_progress_monitoring(progress_tracker, exit_event)
    if progress == "fancy" and progress_tracker:
        return start_fancy_progress(progress_tracker, exit_event)
    return None


def _stop_progress_display(
    progress: str,
    progress_thread: Optional[threading.Thread],
    exit_event: threading.Event,
) -> None:
    """Stop the background progress display thread and clean up its output.

    Args:
        progress (str): The resolved progress mode.
        progress_thread (Optional[threading.Thread]): The background thread to stop.
        exit_event (threading.Event): Event used to signal the thread to stop.
    """
    if progress_thread is None:
        return
    exit_event.set()
    progress_thread.join(timeout=1)
    if progress == "live-update":
        sys.stdout.write("\n")
        sys.stdout.flush()


def _handle_client_error(e: ClientError, s3_uri: str, region: str) -> None:
    """Print a user-friendly message for a botocore ClientError and exit.

    Args:
        e (ClientError): The botocore ClientError to handle.
        s3_uri (str): The S3 URI that was being accessed (used in error messages).
        region (str): The AWS region (used in suggested commands).
    """
    error_code = e.response.get("Error", {}).get("Code", "Unknown")
    if error_code == "NoSuchBucket":
        bucket_uri = s3_uri.split("/")[0] + "//" + s3_uri.split("/")[2]
        print(f"Error: S3 bucket does not exist: {s3_uri}", False)
        print("Suggestions:", False)
        print("  • Double-check the bucket name for typos", False)
        print("  • Verify the bucket exists in the specified region", False)
        print(f"  • Try: aws s3 ls {bucket_uri} --region {region}", False)
    elif error_code == "AccessDenied":
        print(f"Error: Access denied to S3 bucket: {s3_uri}", False)
        print("Possible solutions:", False)
        print("  • Check your AWS credentials: aws sts get-caller-identity", False)
        print(
            "  • Verify bucket permissions allow s3:ListBucket and s3:GetObject",
            False,
        )
        print("  • Ensure you're using the correct AWS profile", False)
    elif error_code == "InvalidAccessKeyId":
        print("Error: Invalid AWS access key ID.", False)
        print("Fix your credentials:", False)
        print("  • Run: aws configure", False)
        print(
            "  • Or set environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY",  # noqa: E501
            False,
        )
    elif error_code == "SignatureDoesNotMatch":
        print("Error: Invalid AWS secret access key.", False)
        print("Fix your credentials:", False)
        print("  • Run: aws configure", False)
        print("  • Verify your AWS_SECRET_ACCESS_KEY is correct", False)
    elif error_code == "NoCredentialsError":
        print("Error: No AWS credentials found.", False)
        print("Set up credentials:", False)
        print("  • Run: aws configure", False)
        print("  • Or use IAM roles if running on EC2", False)
        print(
            "  • Or set environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY",  # noqa: E501
            False,
        )
    else:
        print(f"Error: AWS API error ({error_code}): {e}", False)
        print("Troubleshooting steps:", False)
        print("  • Check your internet connection", False)
        print("  • Verify AWS region is correct", False)
        print("  • Try again in a few moments", False)
    sys.exit(1)


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
    progress: Optional[str],
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
        quiet (bool): If True, suppress all stdout; errors still go to stderr.
        progress (Optional[str]): Progress display mode. None resolves to 'simple'.
    """
    progress = _validate_progress_mode(quiet, progress)

    download_dir, bucket, prefix = prepare_download_dir_and_prefix(
        download_dir, s3_uri, delimiter
    )
    threads, conn_pool_size = get_thread_and_pool_size(threads)
    client, download_queue, completed_queue = create_client_and_queues(
        region, conn_pool_size
    )
    exit_event = utils.create_exit_event()

    # Progress tracker only needed for modes that show stats
    progress_tracker: Optional[ProgressTracker] = None
    if progress in ("detailed", "live-update", "fancy"):
        progress_tracker = ProgressTracker()

    try:
        list_objects(
            client,
            download_queue,
            bucket,
            prefix,
            delimiter,
            regex,
            exit_event,
            progress_tracker,
        )
        download_config = s3.create_download_config(callback=None)

        progress_thread = _setup_progress_display(
            progress, quiet, progress_tracker, completed_queue, exit_event
        )

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
            progress_tracker,
        )

        _stop_progress_display(progress, progress_thread, exit_event)

        # Print final summary for detailed, live-update, and fancy
        if progress in ("detailed", "live-update", "fancy") and progress_tracker:
            print_progress_summary(progress_tracker, quiet=False)

    except KeyboardInterrupt:
        print("\nOperation cancelled by user.", False)  # Always show, even with --quiet
        sys.exit(1)
    except ClientError as e:
        _handle_client_error(e, s3_uri, region)
    except S3FetchError as e:
        if e.args and str(e.args[0]):
            print(f"Error: {e.args[0]}", False)
        else:
            print("Error: An unexpected error occurred during S3 operation.", False)
        sys.exit(1)
