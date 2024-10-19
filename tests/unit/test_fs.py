import pytest

from s3fetch import fs


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
