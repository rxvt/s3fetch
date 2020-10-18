__version__ = "0.1.0"

import sys

import click
from botocore.exceptions import ClientError

from .command import S3dl
from .exceptions import S3dlError


@click.command()
@click.argument("bucket", type=str)
@click.argument("prefix", type=str)
@click.option("--region", type=str, default="us-east-1", help="Bucket region.")
@click.option("-d", "--debug", is_flag=True, help="Enable debug output.")
@click.option("--list-only", is_flag=True, help="List objects only, do not download.")
@click.option(
    "--download-dir",
    type=str,
    help="Download directory. Defaults to current directory.",
)
@click.option("--regex", type=str, help="Filter list of available objects by regex.")
def run(bucket, prefix, region, debug, list_only, download_dir, regex):
    """Easily download all objects under a prefix. Specify '/' as the prefix to download all objects in a bucket.

    Example: s3dl my-test-bucket /my/birthdy-photos/2020-01-01

    You can end the prefix at any point, not only on a delimiter. For example:
    's3dl my-test-bucket /my/birthdy-photos/2020-0' would download any object starting with 'my/birthdy-photos/2020-0'
    """
    try:
        s3dl = S3dl(
            bucket=bucket,
            prefix=prefix,
            region=region,
            debug=debug,
            download_dir=download_dir,
            regex=regex,
        )

        if list_only:
            s3dl.list_only()
            sys.exit()

        s3dl.download_objects()
    except KeyboardInterrupt:
        pass
    except ClientError as e:
        print(e)
    except S3dlError as e:
        print(e)
        sys.exit(1)
