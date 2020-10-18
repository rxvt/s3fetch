import concurrent.futures
import logging
import os
import re
import threading
from pathlib import Path
from typing import Optional, Tuple

import boto3
from botocore.exceptions import NoCredentialsError

from .exceptions import DirectoryDoesNotExistError
from .exceptions import NoCredentialsError as S3FetchNoCredentialsError
from .exceptions import NoObjectsFoundError
from .exceptions import PermissionError as S3FetchPermissionError
from .exceptions import RegexError

logging.basicConfig()


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

        self._threads = threads or os.cpu_count()
        self._logger.debug(f"Using {self._threads} threads.")

        self.client = boto3.client("s3", region_name=region)
        self._objects = []
        self._failed_downloads = []
        self._successful_downloads = 0

        self._thread_exit = threading.Event()

    def _parse_and_split_s3_uri(self, s3_uri: str, delimiter: str) -> Tuple[str, str]:
        """Parse and split the S3 URI into bucket and path prefix.

        :param s3_uri: S3 URI
        :type s3_uri: str
        :param delimiter: S3 path delimiter.
        :type delimiter: str
        :return: Tuple containing the S3 bucket and path prefix.
        :rtype: Tuple[str, str]
        """
        tmp_path = s3_uri.removeprefix("s3://")
        try:
            bucket, prefix = tmp_path.split(delimiter, maxsplit=1)
        except ValueError:
            bucket = tmp_path
            prefix = ""
        self._logger.debug(f"{bucket=}, {prefix=}")
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
        self._logger.debug(f"{download_directory=}")
        return Path(download_directory)

    def _retrieve_list_of_objects(self) -> None:
        """Retrieve a list of objects in the S3 bucket under the specified path prefix.

        :raises NoObjectsFoundError: Raised when no objects are found under the specified prefix.
        """
        self._logger.debug(
            f"Listing objects in '{self._bucket}' with prefix '{self._prefix}'"
        )

        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=self._prefix):
            if "Contents" not in page:
                raise NoObjectsFoundError(
                    "No objects were found to download using the specified criteria."
                )
            for obj in page["Contents"]:
                self._objects.append(obj["Key"])

    def run(self) -> None:
        """Executes listing, filtering and downloading objects from the S3 bucket."""
        try:
            self._retrieve_list_of_objects()
            self._filter_objects()
            self._remove_directories_from_object_listing()
            self._download_objects()
            self._check_for_failed_downloads()
        except NoCredentialsError as e:
            raise S3FetchNoCredentialsError(e) from e

    def _remove_directories_from_object_listing(self) -> None:
        """Remove "directory" objects from the object listing as they are not required."""
        self._logger.debug("Removing dangling directory objects from object list.")
        self._objects = [
            obj
            for obj in filter(lambda x: not x.endswith(self._delimiter), self._objects)
        ]

    def _download_objects(self) -> None:
        """Download objects from the specified S3 bucket and path prefix."""
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self._threads
        ) as executor:
            futures = {
                obj: executor.submit(self._download_object, obj)
                for obj in self._objects
            }
            for key, future in futures.items():
                try:
                    future.result()
                    self._successful_downloads += 1
                except KeyboardInterrupt:
                    if not self._quiet:
                        print("\nThreads are exiting...")
                    executor.shutdown(wait=False, cancel_futures=True)
                    self._thread_exit.set()
                    raise
                except Exception as e:
                    self._failed_downloads.append((key, e))

    def _check_for_failed_downloads(self) -> None:
        """Print out a list of objects that failed to download (if any)."""
        if self._failed_downloads and not self._quiet:
            print()
            print(f"{len(self._failed_downloads)} objects failed to download.")
            if self._debug:
                for key, reason in self._failed_downloads:
                    print(f"{key}: {reason}")
            else:
                print(f"Use --debug to see per object failure information.")

    def _download_object(self, key: str) -> None:
        """Download S3 object from the specified bucket.

        :param key: S3 object key
        :type key: str
        :raises KeyboardInterrupt: Raised hit user cancels operation with CTRL-C.
        :raises S3FetchPermissionError: Raised if a permission error is encountered when writing object to disk.
        """
        try:
            tmp_dest_directory, tmp_dest_filename = key.rsplit(
                self._delimiter, maxsplit=1
            )
        except ValueError:
            tmp_dest_directory = ""
            tmp_dest_filename = key

        destination_directory = self._download_dir / Path(tmp_dest_directory)

        if not destination_directory.is_dir():
            destination_directory.mkdir(parents=True)

        destination_filename = destination_directory / Path(tmp_dest_filename)

        if self._thread_exit.is_set():
            raise KeyboardInterrupt

        self._logger.debug(f"Downloading s3://{self._bucket}{self._delimiter}{key}")
        try:
            if not self._dry_run:
                self.client.download_file(
                    Bucket=self._bucket,
                    Key=key,
                    Filename=str(destination_filename),
                    Callback=self._download_callback,
                )
        except PermissionError as e:
            if not self._quiet:
                print(f"{key}...error")
            raise S3FetchPermissionError(
                f"Permission error when attempting to write object to {destination_filename}"
            ) from e
        else:
            if not self._thread_exit.is_set():
                if not self._quiet:
                    print(f"{key}...done")

    def _download_callback(self, *args, **kwargs):
        """boto3 callback, called whenever boto3 finishes downloading a chunk of an S3 object.

        :raises SystemExit: Raised if KeyboardInterrupt is raised in the main thread.
        """
        if self._thread_exit.is_set():
            self._logger.debug("Main thread has told us to exit, so exiting.")
            raise SystemExit(1)

    def _filter_objects(self) -> None:
        """Filter the list of S3 objects according to a regular expression.

        :raises RegexError: Raised if the regular expression is invalid.
        """
        if not self._regex:
            self._logger.debug("No regex detected.")
            return

        try:
            rexp = re.compile(rf"{self._regex}")
        except Exception as e:
            if self._debug:
                raise RegexError(e) from e
            raise RegexError(f"Regex error: {repr(e)}")

        filtered_object_list = []
        for obj in self._objects:
            if rexp.search(obj):
                self._logger.debug(f"Object {obj} matched regex, added to object list.")
                filtered_object_list.append(obj)

        if not filtered_object_list:
            raise NoObjectsFoundError(
                "No objects were matched using the specified regular expression."
            )

        self._objects = filtered_object_list
