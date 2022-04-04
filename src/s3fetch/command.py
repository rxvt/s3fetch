import concurrent.futures
import logging
import os
import queue
import re
import threading
from pathlib import Path
from typing import Optional, Tuple

import boto3
import botocore
from botocore.endpoint import MAX_POOL_CONNECTIONS
from botocore.exceptions import NoCredentialsError

from .exceptions import DirectoryDoesNotExistError
from .exceptions import NoCredentialsError as S3FetchNoCredentialsError
from .exceptions import PermissionError as S3FetchPermissionError
from .exceptions import RegexError

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
        if self._dry_run and not self._quiet:
            print("Operating in dry run mode. Will not download objects.")

        self._download_dir = self._determine_download_dir(download_dir)

        # os.sched_getaffinity() is not available on MacOS so default back to
        # os.cpu_count()
        if threads:
            self._threads = threads
        else:
            try:
                self._threads = len(os.sched_getaffinity(0))  # type: ignore
            except AttributeError:
                self._threads = os.cpu_count()

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
        self._object_queue = queue.Queue()
        self._failed_downloads = []
        self._successful_downloads = 0

        self._keyboard_interrupt_exit = threading.Event()
        self._print_lock = threading.Lock()

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

    def _retrieve_list_of_objects(self) -> None:
        """Retrieve a list of objects in the S3 bucket under the specified path prefix."""
        if not self._quiet:
            prefix = f"'{self._prefix}'" if self._prefix else "None"
            print(f"Listing objects in bucket '{self._bucket}' with prefix: {prefix}")

        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=self._prefix):
            if "Contents" not in page:
                if not self._quiet:
                    print("No objects found under prefix.")
                break

            if self._keyboard_interrupt_exit.is_set():
                raise KeyboardInterrupt

            for key in filter(
                self._filter_object,
                (obj["Key"] for obj in page["Contents"]),
            ):
                self._object_queue.put_nowait(key)

        # Send sentinel value indicating pagination complete.
        self._object_queue.put_nowait(None)

    def run(self) -> None:
        """Executes listing, filtering and downloading objects from the S3 bucket."""
        try:
            threading.Thread(target=self._retrieve_list_of_objects).start()
            self._download_objects()
            self._check_for_failed_downloads()
        except NoCredentialsError as e:
            raise S3FetchNoCredentialsError(e) from e

    def _download_objects(self) -> None:
        """Download objects from the specified S3 bucket and path prefix."""
        if not self._quiet:
            print("Starting downloads...")

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self._threads
        ) as executor:
            futures = {}
            while True:
                item = self._object_queue.get(block=True)
                if item is None:  # Check for sentinel value
                    break
                futures[item] = executor.submit(self._download_object, item)

            for key, future in futures.items():
                try:
                    future.result()
                    self._successful_downloads += 1
                except KeyboardInterrupt:
                    if not self._quiet:
                        print("\nThreads are exiting...")
                    executor.shutdown(wait=False)
                    self._keyboard_interrupt_exit.set()
                    raise
                except Exception as e:
                    self._failed_downloads.append((key, e))

    def _check_for_failed_downloads(self) -> None:
        """Print out a list of objects that failed to download (if any)."""
        if self._failed_downloads and not self._quiet:
            print()
            print(f"{len(self._failed_downloads)} objects failed to download.")
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

    def _download_object(self, key: str) -> None:
        """Download S3 object from the specified bucket.

        :param key: S3 object key
        :type key: str
        :raises KeyboardInterrupt: Raised hit user cancels operation with CTRL-C.
        :raises S3FetchPermissionError: Raised if a permission error is encountered when writing object to disk.
        """
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

        destination_filename = destination_directory / Path(tmp_dest_filename)

        if self._keyboard_interrupt_exit.is_set():
            raise KeyboardInterrupt

        self._logger.debug(f"Downloading s3://{self._bucket}{self._delimiter}{key}")
        s3transfer_config = boto3.s3.transfer.TransferConfig(
            use_threads=True, max_concurrency=MAX_S3TRANSFER_CONCURRENCY
        )
        try:
            if not self._dry_run:
                self.client.download_file(
                    Bucket=self._bucket,
                    Key=key,
                    Filename=str(destination_filename),
                    Callback=self._download_callback,
                    Config=s3transfer_config,
                )
        except PermissionError as e:
            if not self._quiet:
                self._tprint(f"{key}...error")
            raise S3FetchPermissionError(
                f"Permission error when attempting to write object to {destination_filename}"
            ) from e
        else:
            if not self._keyboard_interrupt_exit.is_set():
                if not self._quiet:
                    self._tprint(f"{key}...done")

    def _download_callback(self, *args, **kwargs):
        """boto3 callback, called whenever boto3 finishes downloading a chunk of an S3 object.

        :raises SystemExit: Raised if KeyboardInterrupt is raised in the main thread.
        """
        if self._keyboard_interrupt_exit.is_set():
            self._logger.debug("Main thread has told us to exit, so exiting.")
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

    def _tprint(self, msg: str) -> None:
        """Thread safe printing.

        :param msg: Text to print to the screen.
        :type msg: str
        """
        self._print_lock.acquire(timeout=1)
        print(msg)
        self._print_lock.release()
