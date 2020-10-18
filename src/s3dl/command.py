import logging
import os
import re
from pathlib import Path
from typing import Optional

import boto3

from .exceptions import (
    RegexError,
    DirectoryDoesNotExistError,
    PermissionError as S3dlPermissionError,
)

import concurrent.futures

logging.basicConfig()


class S3dl:
    def __init__(
        self,
        bucket: str,
        prefix: str,
        region: str = "us-east-1",
        debug: bool = False,
        download_dir: Optional[str] = None,
        regex: Optional[str] = None,
        threads: Optional[int] = None,
    ) -> None:
        self.logger = logging.getLogger("s3dl")
        self.logger.setLevel(logging.DEBUG if debug else logging.INFO)

        self.bucket = bucket
        self.prefix = prefix
        self.debug = debug
        self.regex = regex

        if not download_dir:
            self.download_dir = Path(os.getcwd())
        else:
            self.download_dir = Path(download_dir)
            if not self.download_dir.is_dir():
                raise DirectoryDoesNotExistError(
                    f"The directory '{self.download_dir}'' does not exist."
                )

        self.threads = threads or os.cpu_count()
        self.logger.debug(f"Using {self.threads} threads.")

        self.client = boto3.client("s3", region_name=region)
        self.objects = []

    def get_list_of_objects(self) -> None:
        if self.prefix[0] == "/":
            self.prefix = self.prefix[1:]

        if self.debug:
            print(f"Listing object in {self.bucket} with prefix {self.prefix}")

        paginator = self.client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=self.bucket, Prefix=self.prefix):
            for obj in page["Contents"]:
                self.objects.append(obj["Key"])

    def list_only(self) -> None:
        self.get_list_of_objects()
        for obj in self.objects:
            print(obj)

    def download_objects(self) -> None:
        self.get_list_of_objects()
        self.filter_objects()

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.threads
        ) as executor:
            futures = [
                executor.submit(self.download_object, self.bucket, obj)
                for obj in self.objects
            ]
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    raise DownloadError(e)

    def download_object(self, bucket: str, key: str) -> None:
        destination_filename = self.download_dir / Path(key)

            try:
            self.client.download_file(self.bucket, key, str(destination_filename))
            except PermissionError as e:
            print(f"{key}...error")
                raise S3dlPermissionError(
                    f"Permission error when attempting to write object to {destination_filename}"
                )
        else:
            print(f"{key}...done")

    def filter_objects(self) -> None:
        if not self.regex:
            return

        try:
            rexp = re.compile(self.regex)
        except Exception as e:
            if self.debug:
                raise e from RegexError(e)
            raise RegexError(f"Regex error: {e}")

        filtered_object_list = []
        for obj in self.objects:
            if rexp.search(obj):
                filtered_object_list.append(obj)

        self.objects = filtered_object_list
