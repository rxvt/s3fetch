import pytest

from s3fetch import fs
from s3fetch.exceptions import DirectoryDoesNotExistError


@pytest.mark.parametrize(
    "object_dir, delimiter, expected_dir",
    [
        ("my/test/prefix", "/", "my/test/prefix"),
        ("my/test/prefix/", "/", "my/test/prefix"),
        ("my:test:prefix/", ":", "my/test/prefix"),
        (None, "/", ""),
    ],
)
def test_creating_destination_directory(tmp_path, object_dir, delimiter, expected_dir):
    result = fs.create_destination_directory(
        download_dir=tmp_path, delimiter=delimiter, object_dir=object_dir
    )
    if object_dir:
        expected_dir = tmp_path / expected_dir
    else:
        expected_dir = tmp_path
    assert expected_dir == result
    assert expected_dir.is_dir()


def test_raise_exception_if_directory_doesnt_exist(tmp_path):
    with pytest.raises(DirectoryDoesNotExistError):
        fs.check_download_dir_exists(tmp_path / "blah")


def test_dont_raise_exception_if_directory_exists(tmp_path):
    fs.check_download_dir_exists(tmp_path)
