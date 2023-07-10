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
def test_exclude_directory_from_objects(delimiter):
    key = f"small_files{delimiter}"
    result = s3.check_if_key_is_directory(key=key, delimiter=delimiter)
    assert result is True


@pytest.mark.parametrize("delimiter", ["/", ":"])
def test_not_excluding_non_directory_from_objects(delimiter):
    key = f"small_files{delimiter}my_photo"
    result = s3.check_if_key_is_directory(key=key, delimiter=delimiter)
    assert result is False


@pytest.mark.parametrize("key,", ["my_test_file", "my_dir/my_test_file"])
def test_skip_keys_containing_only_letters(key):
    result = s3.filter_by_regex(key=key, regex=r"\d")
    assert result is False


@pytest.mark.parametrize("key,", ["my_test_file", "my_dir/my_test_file"])
def test_include_keys_starting_with_my_(key):
    result = s3.filter_by_regex(key=key, regex=r"^my_")
    assert result is True
