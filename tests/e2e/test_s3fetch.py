from pathlib import Path

import pytest

from s3fetch.command import S3Fetch


@pytest.mark.e2e
def test_downloading_single_small_file(tmp_path):
    config = {
        "s3_uri": "s3://s3fetch-testing-us-east-2/small_files/01_small_test_file",
        "region": "us-east-2",
        "download_dir": tmp_path,
    }
    s3fetch = S3Fetch(**config)
    s3fetch.run()
    testfile = Path(tmp_path) / "01_small_test_file"
    assert testfile.is_file()
    file_contents = testfile.read_text()
    assert file_contents == "This is the first test file.\n"


@pytest.mark.e2e
def test_only_single_small_file_is_downloaded(tmp_path):
    config = {
        "s3_uri": "s3://s3fetch-testing-us-east-2/small_files/01_small_test_file",
        "region": "us-east-2",
        "download_dir": tmp_path,
    }
    s3fetch = S3Fetch(**config)
    s3fetch.run()
    download_dir = Path(tmp_path)
    assert len([download_dir.iterdir()]) == 1
