import pytest

from s3fetch.exceptions import S3FetchQueueEmpty
from s3fetch.s3 import (
    S3FetchQueue,
    check_if_key_is_directory,
    filter_by_regex,
    filter_object,
    get_download_queue,
)


def test_create_download_queue():
    queue = get_download_queue()
    assert isinstance(queue, S3FetchQueue)


def test_queue_raises_exception_when_sentinel_value_found():
    queue = get_download_queue()
    queue.close()
    with pytest.raises(S3FetchQueueEmpty):
        queue.get()


@pytest.mark.parametrize("delimiter", ["/", ":"])
def test_exclude_directory_from_objects(delimiter):
    key = f"small_files{delimiter}"
    result = check_if_key_is_directory(key=key, delimiter=delimiter)
    assert result is True


@pytest.mark.parametrize("delimiter", ["/", ":"])
def test_not_excluding_non_directory_from_objects(delimiter):
    key = f"small_files{delimiter}my_photo"
    result = check_if_key_is_directory(key=key, delimiter=delimiter)
    assert result is False


@pytest.mark.parametrize("key,", ["my_test_file", "my_dir/my_test_file"])
def test_skip_keys_containing_only_letters(key):
    result = filter_by_regex(key=key, regex=r"\d")
    assert result is False


@pytest.mark.parametrize("key,", ["my_test_file", "my_dir/my_test_file"])
def test_include_keys_starting_with_my_(key):
    result = filter_by_regex(key=key, regex=r"^my_")
    assert result is True
