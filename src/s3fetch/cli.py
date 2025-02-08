"""Command line interface for S3Fetch."""

import logging
import sys
from pathlib import Path

import click
from botocore.exceptions import ClientError

from . import api, aws, s3, utils
from .exceptions import S3FetchError
from .utils import custom_print as print

logger = logging.getLogger(__name__)


@click.command()
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

    Example: s3fetch s3://my-test-bucket/my/birthdy-photos/2020-01-01

    The above will download all S3 objects located under the
    `my/birthday-photos/2020-01-01` prefix.

    You can download all objects in a bucket by using `s3fetch s3://my-test-bucket/`
    """
    if debug:
        utils.enable_debug()

    download_dir = utils.set_download_dir(download_dir)
    bucket, prefix = s3.split_uri_into_bucket_and_prefix(s3_uri, delimiter)

    if not threads:
        threads = utils.get_available_threads()

    conn_pool_size = aws.calc_connection_pool_size(
        threads,
        s3.DEFAULT_S3TRANSFER_CONCURRENCY,
    )
    download_queue = api.S3FetchQueue()
    completed_queue = api.S3FetchQueue()

    # Boto3 client is generally thread safe.
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/clients.html#multithreading-or-multiprocessing-with-clients
    client = aws.get_client(region, conn_pool_size)

    exit_event = utils.create_exit_event()

    print(f"Starting to list objects from {s3_uri}", quiet)
    try:
        api.list_objects(
            client=client,
            download_queue=download_queue,
            bucket=bucket,
            prefix=prefix,
            delimiter="/",
            regex=regex,
            exit_event=exit_event,
        )

        download_config = s3.create_download_config()

        print("Starting to download objects", quiet)
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
    except KeyboardInterrupt:
        pass
    except ClientError as e:
        raise e
    except S3FetchError as e:
        if e.args:
            raise
        sys.exit(1)
