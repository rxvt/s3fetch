import threading
from pathlib import Path

import boto3
import pytest
from boto3.s3.transfer import TransferConfig
from mypy_boto3_s3.client import S3Client

from s3fetch import s3
from s3fetch.exceptions import (
    InvalidCredentialsError,
    PrefixDoesNotExistError,
    RegexError,
    S3FetchQueueClosed,
)


def test_create_download_queue():
    queue = s3.get_queue("download")
    assert isinstance(queue, s3.S3FetchQueue)


def test_queue_raises_exception_when_sentinel_value_found():
    queue = s3.get_queue("download")
    queue.close()
    with pytest.raises(S3FetchQueueClosed):
        queue.get()


@pytest.mark.parametrize("delimiter", ["/", ":"])
def test_exclude_directory_from_objects(delimiter: str):
    key = f"small_files{delimiter}"
    result = s3.check_if_key_is_directory(key=key, delimiter=delimiter)
    assert result is True


@pytest.mark.parametrize("delimiter", ["/", ":"])
def test_not_excluding_non_directory_from_objects(delimiter: str):
    key = f"small_files{delimiter}my_photo"
    result = s3.check_if_key_is_directory(key=key, delimiter=delimiter)
    assert result is False


@pytest.mark.parametrize("key,", ["my_test_file", "my_dir/my_test_file"])
def test_skip_keys_containing_only_letters(key: str):
    result = s3.filter_by_regex(key=key, regex=r"\d")
    assert result is False


@pytest.mark.parametrize("key,", ["my_test_file", "my_dir/my_test_file"])
def test_include_keys_starting_with_my_(key: str):
    result = s3.filter_by_regex(key=key, regex=r"^my_")
    assert result is True


def test_putting_object_onto_download_queue():
    queue = s3.get_queue("download")
    key = "my_test_file"
    s3.add_object_to_download_queue(key=key, queue=queue)
    assert queue.get() == key
    queue.close()


def test_listing_objects_in_bucket_and_adding_objects_to_queue(s3_client: S3Client):
    bucket = "my_bucket"
    queue = s3.get_queue("download")
    key = "my_test_file"
    s3_client.create_bucket(Bucket=bucket)
    s3_client.put_object(Bucket=bucket, Key=key, Body=b"test data")
    exit_event = threading.Event()

    s3.list_objects(
        client=s3_client,
        queue=queue,
        bucket=bucket,
        prefix="",
        delimiter="/",
        regex=None,
        exit_event=exit_event,
    )

    assert queue.get() == key
    queue.close()

    with pytest.raises(S3FetchQueueClosed):
        queue.get()


def test_adding_single_directory_key_to_queue(s3_client: S3Client):
    bucket = "my_bucket"
    queue = s3.get_queue("download")
    key = "my_test_file/"
    s3_client.create_bucket(Bucket=bucket)
    s3_client.put_object(Bucket=bucket, Key=key, Body=b"test data")
    exit_event = threading.Event()

    s3.list_objects(
        client=s3_client,
        queue=queue,
        bucket=bucket,
        prefix="",
        delimiter="/",
        regex=None,
        exit_event=exit_event,
    )

    queue.close()

    with pytest.raises(S3FetchQueueClosed):
        queue.get()


def test_listing_objects_with_no_credentials():
    # Don't import mocked S3 client here as we want the credentials to be invalid.
    s3_client = boto3.client("s3", region_name="us-east-1")

    bucket = "my_bucket"
    queue = s3.get_queue("download")
    queue.close()
    exit_event = threading.Event()

    with pytest.raises(InvalidCredentialsError):
        s3.list_objects(
            client=s3_client,
            queue=queue,
            bucket=bucket,
            prefix="",
            delimiter="/",
            regex=None,
            exit_event=exit_event,
        )


def test_calling_exit_event_while_listing_objects(s3_client: S3Client):
    bucket = "my_bucket"
    queue = s3.get_queue("download")
    key = "my_test_file"
    s3_client.create_bucket(Bucket=bucket)
    s3_client.put_object(Bucket=bucket, Key=key, Body=b"test data")
    exit_event = threading.Event()
    exit_event.set()

    with pytest.raises(SystemExit):
        s3.list_objects(
            client=s3_client,
            queue=queue,
            bucket=bucket,
            prefix="",
            delimiter="/",
            regex=None,
            exit_event=exit_event,
        )
    queue.close()


def test_exit_requested():
    exit_event = threading.Event()
    assert s3.exit_requested(exit_event=exit_event) is False
    exit_event.set()
    assert s3.exit_requested(exit_event=exit_event) is True


@pytest.mark.parametrize(
    "key,delimiter,expected_result",
    [
        ("my_test_file/", "/", True),
        ("my_test_file:", ":", True),
        ("my_test_file", "/", False),
        ("my_test_file", ":", False),
        ("my_dir/my_test_file/", "/", True),
        ("my_dir:my_test_file:", ":", True),
        ("my_dir/my_test_file", "/", False),
        ("my_dir:my_test_file", ":", False),
    ],
)
def test_excluding_directory_objects_from_download_queue(
    key: str, delimiter: str, expected_result: bool
):
    result = s3.exclude_object(key=key, delimiter=delimiter, regex=None)
    assert result is expected_result


def test_excluding_objects_due_to_regex():
    delimiter = "/"
    key = "my_test_file"

    regex = r"\d"
    result = s3.exclude_object(key=key, delimiter=delimiter, regex=regex)
    assert result is True

    regex = r"\w"
    result = s3.exclude_object(key=key, delimiter=delimiter, regex=regex)
    assert result is False


def test_filtering_by_regex_throws_exception():
    key = "my_test_file"

    regex = r"["
    with pytest.raises(RegexError):
        s3.filter_by_regex(key=key, regex=regex)


@pytest.mark.parametrize(
    "prefix,delimiter,key,expected_result",
    [
        ("", "/", "my/test/prefix/my_test_file", "my/test/prefix/my_test_file"),
        ("my/test/prefix/", "/", "my/test/prefix/my_test_file", "my_test_file"),
        ("my/test/prefix", "/", "my/test/prefix/my_test_file", "prefix/my_test_file"),
        ("my/test/pre", "/", "my/test/prefix/my_test_file", "prefix/my_test_file"),
        ("my/tes", "/", "my/test/prefix/my_test_file", "test/prefix/my_test_file"),
        ("my:test:prefix:", ":", "my:test:prefix:my_test_file", "my_test_file"),
        ("my:test:prefix", ":", "my:test:prefix:my_test_file", "prefix:my_test_file"),
        ("my:test:pre", ":", "my:test:prefix:my_test_file", "prefix:my_test_file"),
    ],
)
def test_rolling_up_object_key_with_valid_prefix(
    prefix: str,
    delimiter: str,
    key: str,
    expected_result: bool,
):
    result = s3.rollup_object_key_by_prefix(key=key, delimiter=delimiter, prefix=prefix)
    assert expected_result == result


@pytest.mark.parametrize(
    "prefix,delimiter,key",
    [
        ("my/test/pre/", "/", "my/test/prefix/my_test_file"),
        ("my:test:pre:", ":", "my:test:prefix:my_test_file"),
    ],
)
def test_rolling_up_object_key_with_invalid_prefix(
    prefix: str,
    delimiter: str,
    key: str,
):
    with pytest.raises(PrefixDoesNotExistError):
        s3.rollup_object_key_by_prefix(key=key, delimiter=delimiter, prefix=prefix)


@pytest.mark.parametrize(
    "key, delimiter, expected_dir, expected_filename",
    [
        ("my/test/prefix/my_test_file", "/", "my/test/prefix", "my_test_file"),
        ("my/test/prefix/my_test_file", ":", "", "my/test/prefix/my_test_file"),
        ("my:test:prefix:my_test_file", ":", "my:test:prefix", "my_test_file"),
    ],
)
def test_splitting_object_key_into_local_directory_and_filename(
    key: str,
    delimiter: str,
    expected_dir: str,
    expected_filename: str,
):
    result_dir, result_filename = s3.split_object_key_into_dir_and_file(
        key=key, delimiter=delimiter
    )
    assert result_dir == expected_dir
    assert result_filename == expected_filename


@pytest.mark.parametrize(
    "use_threads, max_concurrency",
    [
        (True, 10),
        (True, 99),
        (False, 1),
    ],
)
def test_creating_s3_transfer_config(use_threads: bool, max_concurrency: int):
    result = s3.create_s3_transfer_config(
        use_threads=use_threads,
        max_concurrency=max_concurrency,
    )
    assert result.max_request_concurrency == max_concurrency
    assert result.use_threads == use_threads


def test_s3_transfer_config_raises_exception():
    with pytest.raises(ValueError):
        s3.create_s3_transfer_config(use_threads=True, max_concurrency=0)


def test_download_object(tmp_path: Path, s3_client: S3Client):
    bucket = "my_bucket"
    key = "my_test_file"
    completion_queue = s3.get_queue("completion")
    s3_client.create_bucket(Bucket=bucket)
    s3_client.put_object(Bucket=bucket, Key=key, Body=b"test data")
    s3.download(
        client=s3_client,
        bucket=bucket,
        key=key,
        exit_event=threading.Event(),
        delimiter="/",
        prefix="",
        download_dir=tmp_path,
        download_config={},  # TODO: Fix
        completed_queue=completion_queue,
    )
    # TODO: Read data from downloaded file and validate it


def test_creating_the_thread_to_list_objects(s3_client: S3Client):
    download_queue = s3.get_queue("download")
    exit_event = threading.Event()
    result = s3.create_list_objects_thread(
        bucket="fake_bucket",
        prefix="",
        client=s3_client,
        download_queue=download_queue,
        delimiter="/",
        exit_event=exit_event,
        regex=None,
    )
    assert isinstance(result, threading.Thread)


def test_create_download_thread_with_empty_download_queue(s3_client, tmp_path):
    download_queue = s3.get_queue("download")
    download_queue.close()
    completed_queue = s3.get_queue("completion")
    exit_event = threading.Event()
    successful_downloads, failed_downloads = s3.create_download_threads(
        client=s3_client,
        threads=4,
        download_queue=download_queue,
        completed_queue=completed_queue,
        exit_event=exit_event,
        bucket="fake_bucket",
        prefix="",
        download_dir=tmp_path,
        delimiter="/",
        download_config={},
        dry_run=False,
    )
    assert successful_downloads == 0
    assert failed_downloads == []


def test_create_download_thread_with_single_object_in_download_queue(
    s3_client, tmp_path
):
    bucket = "test_bucket"
    key = "test_object/filename"
    download_queue = s3.get_queue("download")
    download_queue.put(key)
    download_queue.close()
    completed_queue = s3.get_queue("completion")
    exit_event = threading.Event()
    s3_client.create_bucket(Bucket=bucket)
    s3_client.put_object(Bucket=bucket, Key=key, Body=b"test data")
    successful_downloads, failed_downloads = s3.create_download_threads(
        client=s3_client,
        threads=4,
        download_queue=download_queue,
        completed_queue=completed_queue,
        exit_event=exit_event,
        bucket=bucket,
        prefix="",
        download_dir=tmp_path,
        delimiter="/",
        download_config={},
        dry_run=False,
    )
    assert successful_downloads == 1
    assert failed_downloads == []


def test_create_download_thread_with_multiple_objects_in_download_queue(
    s3_client, tmp_path
):
    bucket = "test_bucket"
    key1 = "test_object/filename1"
    key2 = "test_object/filename2"
    key3 = "test_object/filename3"
    download_queue = s3.get_queue("download")
    download_queue.put(key1)
    download_queue.put(key2)
    download_queue.put(key3)
    download_queue.close()
    completed_queue = s3.get_queue("completion")
    exit_event = threading.Event()
    s3_client.create_bucket(Bucket=bucket)
    s3_client.put_object(Bucket=bucket, Key=key1, Body=b"test data")
    s3_client.put_object(Bucket=bucket, Key=key2, Body=b"test data")
    s3_client.put_object(Bucket=bucket, Key=key3, Body=b"test data")
    successful_downloads, failed_downloads = s3.create_download_threads(
        client=s3_client,
        threads=4,
        download_queue=download_queue,
        completed_queue=completed_queue,
        exit_event=exit_event,
        bucket=bucket,
        prefix="",
        download_dir=tmp_path,
        delimiter="/",
        download_config={},
        dry_run=False,
    )
    assert successful_downloads == 3
    assert failed_downloads == []


def test_download_config_return_data():
    result = s3.create_download_config()
    assert isinstance(result, dict)
    assert isinstance(result.get("Config"), TransferConfig)


def test_creating_the_download_config_without_callback():
    result = s3.create_download_config()
    assert not result.get("Callback")
    assert result.get("Config")


def test_creating_the_download_config_with_callback():
    def test_callback() -> None:
        pass

    result = s3.create_download_config(test_callback)
    assert result.get("Callback")
    assert result.get("Config")
