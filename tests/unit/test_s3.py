import threading

import pytest
from s3fetch import s3


def test_create_download_queue():
    queue = s3.get_download_queue()
    assert isinstance(queue, s3.S3FetchQueue)


def test_queue_raises_exception_when_sentinel_value_found():
    queue = s3.get_download_queue()
    queue.close()
    with pytest.raises(s3.S3FetchQueueEmpty):
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
    queue = s3.get_download_queue()
    key = "my_test_file"
    s3.add_object_to_download_queue(key=key, queue=queue)
    assert queue.get() == key
    queue.close()


def test_listing_objects_in_bucket_and_adding_objects_to_queue(s3_client):
    bucket = "my_bucket"
    queue = s3.get_download_queue()
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

    with pytest.raises(s3.S3FetchQueueEmpty):
        queue.get()


def test_adding_single_directory_key_to_queue(s3_client):
    bucket = "my_bucket"
    queue = s3.get_download_queue()
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

    with pytest.raises(s3.S3FetchQueueEmpty):
        queue.get()


def test_calling_exit_event_while_listing_objects(s3_client):
    bucket = "my_bucket"
    queue = s3.get_download_queue()
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
    key, delimiter, expected_result
):
    result = s3.exclude_object(key=key, delimiter=delimiter, regex=None)
    assert result is expected_result
