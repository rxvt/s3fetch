import concurrent.futures
import logging
import os
import re
import threading
from pathlib import Path
from typing import Optional, Tuple, Callable, Any

import boto3
import botocore
from botocore.endpoint import MAX_POOL_CONNECTIONS
from botocore.exceptions import NoCredentialsError

from .exceptions import DirectoryDoesNotExistError, InvalidCredentialsError
from .exceptions import NoCredentialsError as S3FetchNoCredentialsError
from .exceptions import PermissionError as S3FetchPermissionError
from .exceptions import RegexError, S3FetchQueueEmpty
from .s3 import get_download_queue, shutdown_download_threads, start_listing_objects
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
        self._object_queue = get_download_queue()  # type: ignore
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

    def run(self, on_download: Optional[Callable] = None) -> None:
        """Executes listing, filtering and downloading objects from the S3 bucket.

        :param on_download: Callback function invoked with context after every download, defaults to None
        :type delimiter: Callable, optional
        """
        prefix = f"'{self._prefix}'" if self._prefix else "None"
        tprint(
            f"Listing objects in bucket '{self._bucket}' with prefix: {prefix}",
            self._print_lock,
            self._quiet,
        )

        try:
            start_listing_objects(
                bucket=self._bucket,
                prefix=self._prefix,
                client=self.client,
                download_queue=self._object_queue,
                delimiter=self._delimiter,
                regex=self._regex,
                exit_event=self._exit_requested,
            )
            self._download_objects(on_download)
            self._check_for_failed_downloads()
        except (NoCredentialsError, InvalidCredentialsError) as e:
            raise S3FetchNoCredentialsError(e) from e
        except KeyboardInterrupt:
            self._exit_requested.set()
            raise KeyboardInterrupt

    def _download_objects(self, on_download: Optional[Callable] = None) -> None:
        """Download objects from the specified S3 bucket and path prefix.

        :param on_download: Callback function invoked with context after every download, defaults to None
        :type delimiter: Callable, optional
        """
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
                            self._download_object, item, self._exit_requested, on_download
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
                shutdown_download_threads(executor)
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

    def _rollup_prefix(self, key: str) -> Tuple[Optional[str], str]:
        # First roll up everything under the prefix to the right most delimiter, leaving us with the object key
        # after the rolled up prefix.
        # Example for prefix of '/example/obj'
        # /example/objects/obj1
        # /example/objects/obj2
        # Result: objects/obj1 & objects/obj2
        # Determine rollup prefix
        if self._prefix:
            # Get prefix up to last delimiter
            try:
                rollup_prefix, _ = self._prefix.rsplit(self._delimiter, maxsplit=1)
            except ValueError:
                rollup_prefix = None
        else:
            rollup_prefix = None

        # Remove prefix from key
        if rollup_prefix:
            _, tmp_key = key.rsplit(rollup_prefix + self._delimiter, maxsplit=1)
        else:
            tmp_key = key

        # Split object key into directory and filename
        try:
            directory, filename = tmp_key.rsplit(self._delimiter, maxsplit=1)
        except ValueError:
            directory = None
            filename = tmp_key

        return directory, filename

    def _download_object(self, key: str, exit_event: threading.Event, on_download: Optional[Callable] = None) -> None:
        """Download an object from S3.

        Args:
            key (str): S3 object key.
            exit_event (threading.Event): Notify the script to exit.
            on_download (Callable): Optional callback invoked with context after every download

        Raises:
            S3FetchPermissionError: _description_
        """
        if exit_event.is_set():
            self._logger.debug("Not downloading %s as exit_event is set", key)
            return

        tmp_dest_directory, tmp_dest_filename = self._rollup_prefix(key)

        if tmp_dest_directory:
            destination_directory = self._download_dir / Path(tmp_dest_directory)
        else:
            destination_directory = self._download_dir

        if not destination_directory.is_dir():
            try:
                destination_directory.mkdir(parents=True)
            except FileExistsError:
                pass

        destination_filename = os.path.join(destination_directory, Path(tmp_dest_filename))

        s3_uri: str = f"s3://{self._bucket}{self._delimiter}{key}"
        self._logger.debug(f"Downloading {s3_uri}")
        s3transfer_config = boto3.s3.transfer.TransferConfig(
            use_threads=True, max_concurrency=MAX_S3TRANSFER_CONCURRENCY
        )

        file_size: int = 0
        file_lock = threading.Lock()
        def _download_callback(chunk):
            """boto3 callback, called whenever boto3 finishes downloading a chunk of an S3 object.

            :raises SystemExit: Raised if KeyboardInterrupt is raised in the main thread.
            """
            nonlocal file_size, file_lock
            with file_lock:
                file_size += chunk
            if self._exit_requested.is_set():
                self._logger.debug("Main thread is exiting, cancelling download thread")
                raise SystemExit(1)

        try:
            if not self._dry_run:
                self.client.download_file(
                    Bucket=self._bucket,
                    Key=key,
                    Filename=str(destination_filename),
                    Callback=self._download_callback,
                    Config=s3transfer_config,
                )
                if on_download is not None:
                    try:
                        download_context: dict[str, Any] = {
                            "key": key,
                            "uri": s3_uri,
                            "size": file_size,
                            "filename": destination_filename,
                            "status": True
                        }
                        on_download(download_context)
                    except Exception as ex:
                        self._logger.debug(f"Error invoking download callback for {key}")
                        self._logger.debug(ex)
        except PermissionError as e:
            tprint(f"{key}...error", self._print_lock, self._quiet)
            raise S3FetchPermissionError(
                f"Permission error when attempting to write object to {destination_filename}"
            ) from e
        else:
            if not self._exit_requested.is_set():
                tprint(f"{key}...done", self._print_lock, self._quiet)

    def _download_callback(self, chunk):
        """boto3 callback, called whenever boto3 finishes downloading a chunk of an S3 object.

        :raises SystemExit: Raised if KeyboardInterrupt is raised in the main thread.
        """
        if self._exit_requested.is_set():
            self._logger.debug("Main thread is exiting, cancelling download thread")
            raise SystemExit(1)

    def _filter_object(self, key: str) -> bool:
        """Filter function for the `filter()` call used to determine if an
        object key should be included in the list of objects to download.

        :param key: S3 object key.
        :type key: str
        :returns: True if object key matches regex or no regex provided. False otherwise.
        :raises RegexError: Raised if the regular expression is invalid.
        """
        # Discard key if it's a 'directory'
        if key.endswith(self._delimiter):
            return False

        if not self._regex:
            self._logger.debug("No regex detected.")
            return True

        try:
            rexp = re.compile(rf"{self._regex}")
        except re.error as e:
            msg = f"Regex error: {repr(e)}"
            if self._debug:
                raise RegexError(msg) from e
            raise RegexError(msg)

        if rexp.search(key):
            self._logger.debug(f"Object {key} matched regex, added to object list.")
            return True
        else:
            self._logger.debug(f"Object {key} did not match regex, skipped.")
            return False
