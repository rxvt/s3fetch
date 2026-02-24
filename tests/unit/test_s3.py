import re
import threading
from pathlib import Path
from unittest.mock import patch

import pytest
from boto3.s3.transfer import TransferConfig
from mypy_boto3_s3.client import S3Client

from s3fetch import s3
from s3fetch.exceptions import (
    InvalidCredentialsError,
    PermissionError,
    PrefixDoesNotExistError,
    S3FetchQueueClosed,
)
from s3fetch.s3 import DownloadResult


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
    result = s3.filter_by_regex(key=key, regex=re.compile(r"\d"))
    assert result is False


@pytest.mark.parametrize("key,", ["my_test_file", "my_dir/my_test_file"])
def test_include_keys_starting_with_my_(key: str):
    result = s3.filter_by_regex(key=key, regex=re.compile(r"^my_"))
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


def test_listing_objects_with_no_credentials(s3_client):
    from botocore.exceptions import NoCredentialsError

    bucket = "my_bucket"
    queue = s3.get_queue("download")
    exit_event = threading.Event()

    # Mock a NoCredentialsError
    with patch.object(s3_client, "get_paginator") as mock_paginator:
        mock_paginator.side_effect = NoCredentialsError()

        with pytest.raises(NoCredentialsError):
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


def test_listing_objects_with_invalid_access_key_id(s3_client):
    from botocore.exceptions import ClientError

    bucket = "my_bucket"
    queue = s3.get_queue("download")
    exit_event = threading.Event()

    # Mock a ClientError with InvalidAccessKeyId
    error_response = {
        "Error": {
            "Code": "InvalidAccessKeyId",
            "Message": "The AWS Access Key Id you provided does not exist in our "
            "records.",
        }
    }
    client_error = ClientError(error_response, "ListObjectsV2")

    with patch.object(s3_client, "get_paginator") as mock_paginator:
        mock_paginator.side_effect = client_error

        with pytest.raises(
            InvalidCredentialsError, match="Invalid AWS credentials: InvalidAccessKeyId"
        ):
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


def test_listing_objects_with_signature_mismatch(s3_client):
    from botocore.exceptions import ClientError

    bucket = "my_bucket"
    queue = s3.get_queue("download")
    exit_event = threading.Event()

    # Mock a ClientError with SignatureDoesNotMatch
    error_response = {
        "Error": {
            "Code": "SignatureDoesNotMatch",
            "Message": "The request signature we calculated does not match the "
            "signature you provided.",
        }
    }
    client_error = ClientError(error_response, "ListObjectsV2")

    with patch.object(s3_client, "get_paginator") as mock_paginator:
        mock_paginator.side_effect = client_error

        with pytest.raises(
            InvalidCredentialsError,
            match="Invalid AWS credentials: SignatureDoesNotMatch",
        ):
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


def test_listing_objects_with_token_refresh_required(s3_client):
    from botocore.exceptions import ClientError

    bucket = "my_bucket"
    queue = s3.get_queue("download")
    exit_event = threading.Event()

    # Mock a ClientError with TokenRefreshRequired
    error_response = {
        "Error": {
            "Code": "TokenRefreshRequired",
            "Message": "The provided token must be refreshed.",
        }
    }
    client_error = ClientError(error_response, "ListObjectsV2")

    with patch.object(s3_client, "get_paginator") as mock_paginator:
        mock_paginator.side_effect = client_error

        with pytest.raises(
            InvalidCredentialsError,
            match="SSO token has expired and needs to be refreshed",
        ):
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


def test_listing_objects_with_access_denied(s3_client):
    from botocore.exceptions import ClientError

    bucket = "my_bucket"
    queue = s3.get_queue("download")
    exit_event = threading.Event()

    # Mock a ClientError with AccessDenied
    error_response = {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}
    client_error = ClientError(error_response, "ListObjectsV2")

    with patch.object(s3_client, "get_paginator") as mock_paginator:
        mock_paginator.side_effect = client_error

        with pytest.raises(PermissionError, match="Access denied: AccessDenied"):
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


def test_download_object_with_invalid_credentials(s3_client, tmp_path):
    from botocore.exceptions import ClientError

    key = "test_key"
    dest_filename = tmp_path / "test_file"
    bucket = "test_bucket"
    download_config = {}
    completed_queue = s3.get_queue("completion")

    # Mock a ClientError with InvalidAccessKeyId during download
    error_response = {
        "Error": {
            "Code": "InvalidAccessKeyId",
            "Message": "The AWS Access Key Id you provided does not exist in our "
            "records.",
        }
    }
    client_error = ClientError(error_response, "GetObject")

    with patch.object(s3_client, "download_file") as mock_download:
        mock_download.side_effect = client_error

        with pytest.raises(
            InvalidCredentialsError,
            match="Invalid AWS credentials during download: InvalidAccessKeyId",
        ):
            s3.download_object(
                key=key,
                dest_filename=dest_filename,
                client=s3_client,
                bucket=bucket,
                download_config=download_config,
                completed_queue=completed_queue,
                dry_run=False,
            )
    completed_queue.close()


def test_download_object_with_sso_token_expired(s3_client, tmp_path):
    from botocore.exceptions import ClientError

    key = "test_key"
    dest_filename = tmp_path / "test_file"
    bucket = "test_bucket"
    download_config = {}
    completed_queue = s3.get_queue("completion")

    # Mock a ClientError with TokenRefreshRequired during download
    error_response = {
        "Error": {
            "Code": "TokenRefreshRequired",
            "Message": "The provided token must be refreshed.",
        }
    }
    client_error = ClientError(error_response, "GetObject")

    with patch.object(s3_client, "download_file") as mock_download:
        mock_download.side_effect = client_error

        with pytest.raises(
            InvalidCredentialsError, match="SSO token has expired during download"
        ):
            s3.download_object(
                key=key,
                dest_filename=dest_filename,
                client=s3_client,
                bucket=bucket,
                download_config=download_config,
                completed_queue=completed_queue,
                dry_run=False,
            )
    completed_queue.close()


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

    regex = re.compile(r"\d")
    result = s3.exclude_object(key=key, delimiter=delimiter, regex=regex)
    assert result is True

    regex = re.compile(r"\w")
    result = s3.exclude_object(key=key, delimiter=delimiter, regex=regex)
    assert result is False


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
        ("my_test_file", "/", "", "my_test_file"),
        ("my_test_file", ":", "", "my_test_file"),
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
    test_content = b"test data"
    completion_queue = s3.get_queue("completion")
    s3_client.create_bucket(Bucket=bucket)
    s3_client.put_object(Bucket=bucket, Key=key, Body=test_content)
    s3.download(
        client=s3_client,
        bucket=bucket,
        key=key,
        exit_event=threading.Event(),
        delimiter="/",
        prefix="",
        download_dir=tmp_path,
        download_config={},
        completed_queue=completion_queue,
    )
    # Verify downloaded file exists and contains correct content
    downloaded_file = tmp_path / key
    assert downloaded_file.exists(), f"Downloaded file {downloaded_file} not found"
    assert downloaded_file.read_bytes() == test_content, (
        "File content doesn't match expected data"
    )


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


@pytest.mark.parametrize(
    "s3_uri,delimiter,expected_bucket,expected_prefix",
    [
        ("s3://my-bucket/my/prefix", "/", "my-bucket", "my/prefix"),
        ("s3://my-bucket", "/", "my-bucket", ""),
        ("my-bucket/my/prefix", "/", "my-bucket", "my/prefix"),
        ("my-bucket", "/", "my-bucket", ""),
        ("s3://my:bucket:my:prefix", ":", "my", "bucket:my:prefix"),
        ("s3://my-bucket/", "/", "my-bucket", ""),
    ],
)
def test_split_uri_into_bucket_and_prefix(
    s3_uri: str, delimiter: str, expected_bucket: str, expected_prefix: str
):
    """Test splitting S3 URIs into bucket and prefix components."""
    bucket, prefix = s3.split_uri_into_bucket_and_prefix(
        s3_uri=s3_uri, delimiter=delimiter
    )
    assert bucket == expected_bucket
    assert prefix == expected_prefix


def test_download_object_no_temp_file_on_success(tmp_path: Path, s3_client: S3Client):
    """No temporary file should remain after a successful download."""
    bucket = "my_bucket"
    key = "my_test_file"
    test_content = b"test data"
    completion_queue = s3.get_queue("completion")
    s3_client.create_bucket(Bucket=bucket)
    s3_client.put_object(Bucket=bucket, Key=key, Body=test_content)
    dest_filename = tmp_path / key

    s3.download_object(
        key=key,
        dest_filename=dest_filename,
        client=s3_client,
        bucket=bucket,
        download_config={},
        completed_queue=completion_queue,
        dry_run=False,
    )

    assert dest_filename.exists()
    assert dest_filename.read_bytes() == test_content
    tmp_files = list(tmp_path.glob("*.s3fetch_tmp"))
    assert tmp_files == [], f"Unexpected temp files left behind: {tmp_files}"
    completion_queue.close()


def test_download_object_no_temp_file_on_failure(tmp_path: Path, s3_client: S3Client):
    """No temporary file should remain after a failed download."""
    from botocore.exceptions import ClientError as BotocoreClientError

    key = "test_key"
    dest_filename = tmp_path / "test_file"
    bucket = "test_bucket"
    completed_queue = s3.get_queue("completion")

    error_response = {
        "Error": {"Code": "SomeOtherError", "Message": "Something failed"}
    }
    client_error = BotocoreClientError(error_response, "GetObject")

    with patch.object(s3_client, "download_file") as mock_download:
        mock_download.side_effect = client_error
        with pytest.raises(BotocoreClientError):
            s3.download_object(
                key=key,
                dest_filename=dest_filename,
                client=s3_client,
                bucket=bucket,
                download_config={},
                completed_queue=completed_queue,
                dry_run=False,
            )

    assert not dest_filename.exists()
    tmp_files = list(tmp_path.glob("*.s3fetch_tmp"))
    assert tmp_files == [], f"Unexpected temp files left behind: {tmp_files}"
    completed_queue.close()


@pytest.mark.parametrize(
    "key, body",
    [
        ("file with spaces.txt", b"spaces"),
        ("file%20url%20encoded.txt", b"url encoded"),
        ("deep/nested/path/to/file.txt", b"nested"),
        ("unicode_\u30d5\u30a1\u30a4\u30eb.txt", b"unicode"),
    ],
)
def test_download_object_with_special_characters_in_key(
    tmp_path: Path, s3_client: S3Client, key: str, body: bytes
):
    """Objects with special characters or unicode keys should download correctly."""
    bucket = "my_bucket"
    completion_queue = s3.get_queue("completion")
    s3_client.create_bucket(Bucket=bucket)
    s3_client.put_object(Bucket=bucket, Key=key, Body=body)

    s3.download(
        client=s3_client,
        bucket=bucket,
        key=key,
        exit_event=threading.Event(),
        delimiter="/",
        prefix="",
        download_dir=tmp_path,
        download_config={},
        completed_queue=completion_queue,
    )

    expected_file = tmp_path / Path(key)
    assert expected_file.exists(), f"Expected file {expected_file} not found"
    assert expected_file.read_bytes() == body
    completion_queue.close()


def test_download_zero_byte_object(tmp_path: Path, s3_client: S3Client):
    """A 0-byte S3 object should produce a 0-byte local file, not an error."""
    bucket = "my_bucket"
    key = "empty_file.txt"
    completion_queue = s3.get_queue("completion")
    s3_client.create_bucket(Bucket=bucket)
    s3_client.put_object(Bucket=bucket, Key=key, Body=b"")

    s3.download(
        client=s3_client,
        bucket=bucket,
        key=key,
        exit_event=threading.Event(),
        delimiter="/",
        prefix="",
        download_dir=tmp_path,
        download_config={},
        completed_queue=completion_queue,
    )

    downloaded_file = tmp_path / key
    assert downloaded_file.exists(), f"Expected file {downloaded_file} not found"
    assert downloaded_file.stat().st_size == 0
    completion_queue.close()


def test_download_object_overwrites_existing_file(tmp_path: Path, s3_client: S3Client):
    """Downloading a key that already exists locally should silently overwrite it."""
    bucket = "my_bucket"
    key = "my_test_file"
    original_content = b"original data"
    new_content = b"new data from s3"
    completion_queue = s3.get_queue("completion")

    existing_file = tmp_path / key
    existing_file.write_bytes(original_content)

    s3_client.create_bucket(Bucket=bucket)
    s3_client.put_object(Bucket=bucket, Key=key, Body=new_content)

    s3.download(
        client=s3_client,
        bucket=bucket,
        key=key,
        exit_event=threading.Event(),
        delimiter="/",
        prefix="",
        download_dir=tmp_path,
        download_config={},
        completed_queue=completion_queue,
    )

    assert existing_file.read_bytes() == new_content, (
        "Existing file should have been overwritten with new S3 content"
    )
    completion_queue.close()


def test_download_object_oserror_raises_clear_message(tmp_path: Path, s3_client):
    """OSError during download (e.g. disk full) should produce a clear error message."""
    import errno

    key = "test_key"
    dest_filename = tmp_path / "test_file"
    bucket = "test_bucket"
    completed_queue = s3.get_queue("completion")

    disk_full_error = OSError(errno.ENOSPC, "No space left on device")

    with patch.object(s3_client, "download_file") as mock_download:
        mock_download.side_effect = disk_full_error
        with pytest.raises(OSError, match="I/O error writing"):
            s3.download_object(
                key=key,
                dest_filename=dest_filename,
                client=s3_client,
                bucket=bucket,
                download_config={},
                completed_queue=completed_queue,
                dry_run=False,
            )
    completed_queue.close()


# ---------------------------------------------------------------------------
# DownloadResult tests
# ---------------------------------------------------------------------------


def test_download_object_puts_download_result_on_success(
    tmp_path: Path, s3_client: S3Client
):
    """A successful download puts a DownloadResult(success=True) on the completed queue."""  # noqa: E501
    bucket = "my_bucket"
    key = "result_test_file"
    body = b"hello result"
    completed_queue: s3.S3FetchQueue[DownloadResult] = s3.S3FetchQueue()
    s3_client.create_bucket(Bucket=bucket)
    s3_client.put_object(Bucket=bucket, Key=key, Body=body)
    dest_filename = tmp_path / key

    s3.download_object(
        key=key,
        dest_filename=dest_filename,
        client=s3_client,
        bucket=bucket,
        download_config={},
        completed_queue=completed_queue,
        dry_run=False,
    )

    result = completed_queue.get()
    assert isinstance(result, DownloadResult)
    assert result.key == key
    assert result.dest_filename == dest_filename
    assert result.success is True
    assert result.file_size == len(body)
    assert result.error is None
    completed_queue.close()


def test_download_object_puts_download_result_on_failure(
    tmp_path: Path, s3_client: S3Client
):
    """A failed download puts a DownloadResult(success=False) on the completed queue."""
    from botocore.exceptions import ClientError as BotocoreClientError

    key = "failing_key"
    dest_filename = tmp_path / "failing_file"
    bucket = "test_bucket"
    completed_queue: s3.S3FetchQueue[DownloadResult] = s3.S3FetchQueue()

    error_response = {"Error": {"Code": "SomeOtherError", "Message": "boom"}}
    client_error = BotocoreClientError(error_response, "GetObject")

    with patch.object(s3_client, "download_file") as mock_download:
        mock_download.side_effect = client_error
        with pytest.raises(BotocoreClientError):
            s3.download_object(
                key=key,
                dest_filename=dest_filename,
                client=s3_client,
                bucket=bucket,
                download_config={},
                completed_queue=completed_queue,
                dry_run=False,
            )

    result = completed_queue.get()
    assert isinstance(result, DownloadResult)
    assert result.key == key
    assert result.dest_filename == dest_filename
    assert result.success is False
    assert result.file_size == 0
    assert result.error is client_error
    completed_queue.close()


def test_download_result_dry_run_has_zero_file_size(
    tmp_path: Path, s3_client: S3Client
):
    """In dry-run mode the DownloadResult is still emitted with file_size=0."""
    bucket = "my_bucket"
    key = "dry_run_file"
    completed_queue: s3.S3FetchQueue[DownloadResult] = s3.S3FetchQueue()
    s3_client.create_bucket(Bucket=bucket)
    s3_client.put_object(Bucket=bucket, Key=key, Body=b"content")
    dest_filename = tmp_path / key

    s3.download_object(
        key=key,
        dest_filename=dest_filename,
        client=s3_client,
        bucket=bucket,
        download_config={},
        completed_queue=completed_queue,
        dry_run=True,
    )

    result = completed_queue.get()
    assert isinstance(result, DownloadResult)
    assert result.key == key
    assert result.success is True
    assert result.file_size == 0
    completed_queue.close()


def test_get_file_size_returns_zero_on_oserror(tmp_path: Path):
    """_get_file_size returns 0 and logs a warning when stat() fails."""
    missing = tmp_path / "ghost.txt"
    # File does not exist, so stat() will raise OSError
    result = s3._get_file_size(missing)
    assert result == 0


def test_raise_download_client_error_access_denied(tmp_path: Path):
    """_raise_download_client_error raises PermissionError for AccessDenied."""
    from botocore.exceptions import ClientError

    error_response = {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}
    client_error = ClientError(error_response, "GetObject")
    dest = tmp_path / "file.txt"
    with pytest.raises(PermissionError, match="Access denied during download"):
        s3._raise_download_client_error(client_error, dest)


def test_raise_download_client_error_unauthorized_operation(tmp_path: Path):
    """_raise_download_client_error raises PermissionError for UnauthorizedOperation."""
    from botocore.exceptions import ClientError

    error_response = {
        "Error": {"Code": "UnauthorizedOperation", "Message": "Not authorized"}
    }
    client_error = ClientError(error_response, "GetObject")
    dest = tmp_path / "file.txt"
    with pytest.raises(PermissionError, match="Access denied during download"):
        s3._raise_download_client_error(client_error, dest)


def test_download_object_permission_error_emits_result_and_raises(
    tmp_path: Path, s3_client: S3Client
):
    """PermissionError during download emits DownloadResult(success=False) and re-raises."""  # noqa: E501
    key = "perm_test_key"
    dest_filename = tmp_path / key
    bucket = "test_bucket"
    completed_queue: s3.S3FetchQueue[DownloadResult] = s3.S3FetchQueue()

    with patch.object(s3_client, "download_file") as mock_download:
        mock_download.side_effect = PermissionError("read-only filesystem")
        with pytest.raises(PermissionError, match="Permission error when attempting"):
            s3.download_object(
                key=key,
                dest_filename=dest_filename,
                client=s3_client,
                bucket=bucket,
                download_config={},
                completed_queue=completed_queue,
                dry_run=False,
            )

    result = completed_queue.get()
    assert isinstance(result, DownloadResult)
    assert result.success is False
    assert result.key == key
    completed_queue.close()


def test_s3fetch_queue_generic_typing():
    """S3FetchQueue is generic and can hold any item type."""
    str_queue: s3.S3FetchQueue[str] = s3.S3FetchQueue()
    str_queue.put("hello")
    assert str_queue.get() == "hello"

    result_queue: s3.S3FetchQueue[DownloadResult] = s3.S3FetchQueue()
    r = DownloadResult(key="k", dest_filename=Path("/tmp/k"), success=True)  # noqa: S108
    result_queue.put(r)
    assert result_queue.get() is r
