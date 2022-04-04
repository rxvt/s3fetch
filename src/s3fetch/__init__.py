import sys

import click
import pkg_resources
from botocore.exceptions import ClientError

from .command import S3Fetch
from .exceptions import S3FetchError

__version__ = pkg_resources.get_distribution("s3fetch").version


@click.command()
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
    type=str,
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
    "--dry-run", "--list-only", is_flag=True, help="List objects only, do not download."
)
@click.option(
    "--delimiter",
    type=str,
    default="/",
    help="Specify the \"directory\" delimiter. Defaults to '/'.",
)
@click.option("-q", "--quiet", is_flag=True, help="Don't print to stdout.")
@click.option("--version", is_flag=True, help="Print out version information.")
def run(
    s3_uri: str,
    region: str,
    debug: bool,
    download_dir: str,
    regex: str,
    threads: int,
    dry_run: bool,
    delimiter: str,
    quiet: bool,
    version: bool,
):
    """Easily download objects from an S3 bucket.

    Example: s3fetch s3://my-test-bucket/my/birthdy-photos/2020-01-01

    The above will download all S3 objects located under the `my/birthdy-photos/2020-01-01` prefix.

    You can download all objects in a bucket by using `s3fetch s3://my-test-bucket/`
    """
    try:
        s3fetch = S3Fetch(
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
        s3fetch.run()
    except KeyboardInterrupt:
        pass
    except ClientError as e:
        print(e)
    except S3FetchError as e:
        if e.args:
            print(e)
        sys.exit(1)


def cmd():
    if "--version" in sys.argv:
        print(__version__)
        sys.exit(0)
    run()