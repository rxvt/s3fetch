import concurrent.futures
import logging
import os
import threading
from pathlib import Path
from typing import Optional, Tuple

import boto3
import botocore
from botocore.endpoint import MAX_POOL_CONNECTIONS
from botocore.exceptions import NoCredentialsError

from .exceptions import DirectoryDoesNotExistError, InvalidCredentialsError
from .exceptions import NoCredentialsError as S3FetchNoCredentialsError
from .exceptions import PermissionError as S3FetchPermissionError
from .exceptions import S3FetchQueueEmpty

from . import s3, fs, api
from .utils import tprint

logging.basicConfig()

MAX_S3TRANSFER_CONCURRENCY = 10


class S3Fetch:
    def __init__(
        self,
        s3_uri: str,
        region: str = "us-east-1",
        debug: bool = False,
        download_dir: Optional[str] = None,
        regex: Optional[str] = None,
        threads: Optional[int] = None,
        dry_run: bool = False,
        delimiter: str = "/",
        quiet: bool = False,
    ) -> None:
        """S3Fetch

        :param s3_uri: S3 URI
        :type s3_uri: str
        :param region: AWS region the bucket is located in, defaults to "us-east-1"
        :type region: str, optional
        :param debug: Enable debug output, defaults to False
        :type debug: bool, optional
        :param download_dir: Directory to download objects to, defaults to None
        :type download_dir: Optional[str], optional
        :param regex: Regex to use to filter objects, defaults to None
        :type regex: Optional[str], optional
        :param threads: Number of threads to use, 1 thread used per object, defaults to None
        :type threads: Optional[int], optional
        :param dry_run: Enable dry run mode, don't actually download anything, defaults to False
        :type dry_run: bool, optional
        :param delimiter: S3 object path delimiter, defaults to "/"
        :type delimiter: str, optional
        :param quiet: Don't print anything to stdout, defaults to False
        :type delimiter: bool, optional
        """

        self._logger = logging.getLogger("s3fetch")
        self._logger.setLevel(logging.DEBUG if debug else logging.INFO)

        self._bucket, self._prefix = self._parse_and_split_s3_uri(s3_uri, delimiter)

        self._debug = debug
        self._regex = regex
        self._dry_run = dry_run
        self._delimiter = delimiter
        self._quiet = quiet
        self._print_lock = threading.Lock()
        self._exit_requested = threading.Event()
        if self._dry_run:
            tprint(
                "Operating in dry run mode. Will not download objects.",
                self._print_lock,
                self._quiet,
            )

        self._download_dir = self._determine_download_dir(download_dir)

        # os.sched_getaffinity() is not available on MacOS so default back to
        # os.cpu_count()
        if threads:
            self._threads = threads
        else:
            try:
                self._threads = len(os.sched_getaffinity(0))  # type: ignore
            except AttributeError:
                self._threads = os.cpu_count()  # type: ignore

        self._logger.debug(f"Using {self._threads} threads.")

        # https://stackoverflow.com/questions/53765366/urllib3-connectionpool-connection-pool-is-full-discarding-connection
        # https://github.com/boto/botocore/issues/619#issuecomment-461859685
        # max_pool_connections here is passed to the max_size param of urllib3.HTTPConnectionPool()
        connection_pool_connections = max(MAX_POOL_CONNECTIONS, self._threads * MAX_S3TRANSFER_CONCURRENCY)  # type: ignore
        self._logger.debug(
            f"Setting urllib3 ConnectionPool(maxsize={connection_pool_connections})"
        )
        client_config = botocore.config.Config(  # type: ignore
            max_pool_connections=connection_pool_connections,
        )

        self.client = boto3.client("s3", region_name=region, config=client_config)
        self._object_queue = s3.get_download_queue()  # type: ignore
        self._failed_downloads = []  # type: ignore
        self._successful_downloads = 0

    def _parse_and_split_s3_uri(self, s3_uri: str, delimiter: str) -> Tuple[str, str]:
        """Parse and split the S3 URI into bucket and path prefix.

        :param s3_uri: S3 URI
        :type s3_uri: str
        :param delimiter: S3 path delimiter.
        :type delimiter: str
        :return: Tuple containing the S3 bucket and path prefix.
        :rtype: Tuple[str, str]
        """
        tmp_path = s3_uri.replace("s3://", "", 1)
        try:
            bucket, prefix = tmp_path.split(delimiter, maxsplit=1)
        except ValueError:
            bucket = tmp_path
            prefix = ""
        self._logger.debug(f"bucket={bucket}, prefix={prefix}")
        return bucket, prefix

    def _determine_download_dir(self, download_dir: Optional[str]) -> Path:
        """Determine the correct download directory to use.

        :param download_dir: Download directory, `None` results in current directory.
        :type download_dir: Optional[str]
        :raises DirectoryDoesNotExistError: Raised if the specified directory does not exist.
        :return: Path object representing the download directory.
        :rtype: Path
        """
        if not download_dir:
            download_directory = Path(os.getcwd())
        else:
            download_directory = Path(download_dir)
            if not download_directory.is_dir():
                raise DirectoryDoesNotExistError(
                    f"The directory '{download_directory}' does not exist."
                )
        self._logger.debug(f"download_directory={download_directory}")
        return Path(download_directory)

    def run(self) -> None:
        """Executes listing, filtering and downloading objects from the S3 bucket."""
        prefix = f"'{self._prefix}'" if self._prefix else "None"
        tprint(
            f"Listing objects in bucket '{self._bucket}' with prefix: {prefix}",
            self._print_lock,
            self._quiet,
        )

        try:
            api.list_objects(
                bucket=self._bucket,
                prefix=self._prefix,
                client=self.client,
                download_queue=self._object_queue,
                delimiter=self._delimiter,
                regex=self._regex,
                exit_event=self._exit_requested,
            )
            self._download_objects()
            self._check_for_failed_downloads()
        except (NoCredentialsError, InvalidCredentialsError) as e:
            raise S3FetchNoCredentialsError(e) from e
        except KeyboardInterrupt:
            self._exit_requested.set()
            raise KeyboardInterrupt

    def _download_objects(self) -> None:
        """Download objects from the specified S3 bucket and path prefix."""
        tprint("Starting downloads...", self._print_lock, self._quiet)

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self._threads
        ) as executor:
            try:
                futures = {}
                while True:
                    try:
                        item = self._object_queue.get(block=True)
                        futures[item] = executor.submit(
                            self._download_object, item, self._exit_requested
                        )
                    except S3FetchQueueEmpty:
                        break

                for key, future in futures.items():
                    try:
                        future.result()
                        self._successful_downloads += 1
                    except Exception as e:
                        self._failed_downloads.append((key, e))
            except KeyboardInterrupt:
                self._exit_requested.set()
                tprint(
                    "\nThreads are exiting (this might take a while depending on the amount of threads)...",
                    self._print_lock,
                )
                s3.shutdown_download_threads(executor)
                raise

    def _check_for_failed_downloads(self) -> None:
        """Print out a list of objects that failed to download (if any)."""
        if self._failed_downloads and not self._quiet:
            print()
            tprint(
                f"{len(self._failed_downloads)} objects failed to download.",
                self._print_lock,
            )
            for key, reason in self._failed_downloads:
                print(f"{key}: {reason}")

    def _download_object(self, key: str, exit_event: threading.Event) -> None:
        """Download an object from S3.

        Args:
            key (str): S3 object key.
            exit_event (threading.Event): Notify the script to exit.

        Raises:
            S3FetchPermissionError: _description_
        """
        if exit_event.is_set():
            self._logger.debug("Not downloading %s as exit_event is set", key)
            return

        local_object_key = s3.rollup_object_key_by_prefix(
            key=key, delimiter=self._delimiter, prefix=self._prefix
        )

        (
            tmp_dest_directory,
            tmp_dest_filename,
        ) = s3.split_object_key_into_dir_and_file(local_object_key, self._delimiter)

        absolute_dest_dir = fs.create_destination_directory(
            download_dir=self._download_dir,
            object_dir=tmp_dest_directory,
            delimiter=self._delimiter,
        )

        absolute_dest_filename = absolute_dest_dir / tmp_dest_filename

        self._logger.debug(f"Downloading s3://{self._bucket}{self._delimiter}{key}")
        s3transfer_config = s3.create_s3_transfer_config(
            use_threads=True,
            max_concurrency=MAX_S3TRANSFER_CONCURRENCY,
        )

        s3.download_object(
            client=self.client,
            config=s3transfer_config,
            bucket=self._bucket,
            key=key,
            dest_filename=str(absolute_dest_filename),
            exit_event=self._exit_requested,
            print_lock=self._print_lock,
            quiet=self._quiet,
            callback=self._download_callback,
        )

    def _download_callback(self, chunk):
        """boto3 callback, called whenever boto3 finishes downloading a chunk of an S3 object.

        :raises SystemExit: Raised if KeyboardInterrupt is raised in the main thread.
        """
        if self._exit_requested.is_set():
            self._logger.debug("Main thread is exiting, cancelling download thread")
            raise SystemExit(1)
