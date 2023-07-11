import os
from pathlib import Path

import pytest
from s3fetch import __version__
from s3fetch.command import S3Fetch
from s3fetch.exceptions import DirectoryDoesNotExistError, NoObjectsFoundError


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture
def s3fetch(aws_credentials):
    bucket = "my-test-bucket"
    prefix = "my/test/objects/"
    regex = "_mytestobject_"

    s3_path = f"{bucket}/{prefix}"

    s3fetch = S3Fetch(s3_uri=s3_path, regex=regex, debug=False)
    s3fetch._objects = [
        "one_mytestobject_one",
        "two_mytestobject_two",
        "three_mytestobject_three",
        "four*mytestobject*four",
        "five)mytestobject_five",
        "six!mytestdirectoryobject!six/",
    ]
    return s3fetch


def test_s3fetch_obj(s3fetch):
    assert s3fetch._bucket == "my-test-bucket"
    assert s3fetch._prefix == "my/test/objects/"
    assert s3fetch._regex == "_mytestobject_"
    assert s3fetch._objects == [
        "one_mytestobject_one",
        "two_mytestobject_two",
        "three_mytestobject_three",
        "four*mytestobject*four",
        "five)mytestobject_five",
        "six!mytestdirectoryobject!six/",
    ]


# TODO: Fixup once moto tests are working.
# NoObjectsFoundError now raised by_retrieve_list_of_objects
# def test_filter_object_no_matching_objects(s3fetch):
#     s3fetch._regex = r"^sdfasdfasdfsa$"
#     with pytest.raises(NoObjectsFoundError):
#         s3fetch._filter_object()

# TODO: Fixup once moto tests are working.
# NoObjectsFoundError now raised by_retrieve_list_of_objects
# def test_filter_object_empty_object_list(s3fetch):
#     s3fetch._objects = []
#     s3fetch._regex = r"^\w+_\w+_\w+$"
#     with pytest.raises(NoObjectsFoundError):
#         s3fetch._filter_object()


def test_check_for_failed_downloads(s3fetch, capfd):
    s3fetch._failed_downloads = [
        (
            "my/test/objects/one_mytestobject_one",
            "reason",
        )
    ]

    s3fetch._check_for_failed_downloads()
    out, _ = capfd.readouterr()
    assert "objects failed to download" in out

    s3fetch._debug = True
    s3fetch._check_for_failed_downloads()
    out, _ = capfd.readouterr()
    assert f"my/test/objects/one_mytestobject_one: reason" in out


def test_check_for_failed_downloads_no_failures(s3fetch, capfd):
    s3fetch._failed_downloads = []
    s3fetch._check_for_failed_downloads()
    out, _ = capfd.readouterr()
    assert "objects failed to download" not in out


def test_dry_run_detected(s3fetch, capfd):
    s3_path = "s3://my-test-bucket/my/test/objects/"
    S3Fetch(s3_uri=s3_path, dry_run=True, debug=True)
    out, _ = capfd.readouterr()
    assert "Operating in dry run mode. Will not download objects." in out


def test_determine_download_dir_none_dir_specified(s3fetch, mocker):
    os_mock = mocker.patch("os.getcwd")
    expected_directory = Path("/home/test")
    os_mock.return_value = expected_directory
    assert s3fetch._determine_download_dir(None) == expected_directory


def test_determine_download_dir_dir_specified_and_exists(s3fetch, mocker):
    is_dir_mock = mocker.patch("pathlib.Path.is_dir")
    is_dir_mock.return_value = True
    expected_directory = Path("/home/test/Downloads")
    assert s3fetch._determine_download_dir("/home/test/Downloads") == expected_directory


def test_determine_download_dir_dir_specified_and_raises(s3fetch, mocker):
    is_dir_mock = mocker.patch("pathlib.Path.is_dir")
    is_dir_mock.return_value = False
    expected_directory = "/home/test/Downloads"
    with pytest.raises(DirectoryDoesNotExistError):
        s3fetch._determine_download_dir(expected_directory)


def test_parse_and_split_s3_uri_full_path(s3fetch):
    bucket, prefix = s3fetch._parse_and_split_s3_uri(
        s3_uri="s3://testbucket/files", delimiter="/"
    )
    assert bucket == "testbucket"
    assert prefix == "files"

    bucket, prefix = s3fetch._parse_and_split_s3_uri(
        s3_uri="s3://testbucket/files/", delimiter="/"
    )
    assert bucket == "testbucket"
    assert prefix == "files/"


def test_parse_and_split_s3_uri_no_prefix(s3fetch):
    bucket, prefix = s3fetch._parse_and_split_s3_uri(
        s3_uri="s3://testbucket", delimiter="/"
    )
    assert bucket == "testbucket"
    assert prefix == ""

    bucket, prefix = s3fetch._parse_and_split_s3_uri(
        s3_uri="s3://testbucket/", delimiter="/"
    )
    assert bucket == "testbucket"
    assert prefix == ""


def test_rollup_prefix(s3fetch):
    # (prefix, object_key, expected directory, expected filename)
    prefix_and_keys = [
        ("", "object1", None, "object1"),
        ("storage", "storage/object1", "storage", "object1"),
        ("sto", "storage/object1", "storage", "object1"),
        ("storage/obj", "storage/object1", None, "object1"),
        ("test/an", "test/another_folder/console", "another_folder", "console"),
        ("", "test/another_folder/console", "test/another_folder", "console"),
    ]

    for prefix, key, directory, filename in prefix_and_keys:
        s3fetch._prefix = prefix
        tmp_directory, tmp_filename = s3fetch._rollup_prefix(key)
        assert (directory, filename) == (tmp_directory, tmp_filename)
