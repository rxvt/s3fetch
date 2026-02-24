from unittest.mock import patch

import pytest

from s3fetch import fs
from s3fetch.exceptions import DirectoryDoesNotExistError, PathTraversalError


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


@pytest.mark.parametrize(
    "object_dir",
    [
        "../../etc/shadow",
        "../sibling_dir/file.txt",
        "valid/path/../../../../../../etc/passwd",
    ],
)
def test_path_traversal_raises_exception(tmp_path, object_dir):
    """Object keys with '..' that escape the download dir must be rejected."""
    with pytest.raises(PathTraversalError):
        fs.create_destination_directory(
            download_dir=tmp_path,
            object_dir=object_dir,
            delimiter="/",
        )


@pytest.mark.parametrize(
    "object_dir",
    [
        "normal/path/to/file",
        "single",
        "a/b/c/d/e",
    ],
)
def test_safe_paths_do_not_raise(tmp_path, object_dir):
    """Normal object keys without traversal components must work correctly."""
    result = fs.create_destination_directory(
        download_dir=tmp_path,
        object_dir=object_dir,
        delimiter="/",
    )
    assert result.is_dir()
    assert result.is_relative_to(tmp_path)


def test_create_destination_directory_dry_run_does_not_create_dir(tmp_path):
    """In dry_run mode no directory is created on disk."""
    target = tmp_path / "sub" / "dir"
    result = fs.create_destination_directory(
        download_dir=tmp_path,
        object_dir="sub/dir",
        delimiter="/",
        dry_run=True,
    )
    assert result == target
    assert not target.exists()


def test_check_download_dir_exists_raises_when_path_is_file(tmp_path):
    """A path that exists but is a file raises DirectoryDoesNotExistError."""
    file_path = tmp_path / "some_file.txt"
    file_path.write_text("data")
    with pytest.raises(DirectoryDoesNotExistError, match="not a directory"):
        fs.check_download_dir_exists(file_path)


def test_check_download_dir_exists_raises_on_permission_error(tmp_path):
    """A directory that fails the os.access check raises PermissionError."""
    with patch("s3fetch.fs.os.access", return_value=False):
        with pytest.raises(PermissionError, match="Insufficient permissions"):
            fs.check_download_dir_exists(tmp_path)
