from pathlib import Path

import pytest

from s3fetch.command import S3Fetch


@pytest.fixture()
def bucket_config() -> dict:
    config = {
        "bucket": "s3://s3fetch-testing-us-east-2",
        "region": "us-east-2",
    }
    return config


def test_downloading_single_small_file(bucket_config, tmp_path):
    config = {
        "s3_uri": f"{bucket_config['bucket']}/small_files/01_small_test_file",
        "region": bucket_config["region"],
        "download_dir": tmp_path,
    }
    s3fetch = S3Fetch(**config)
    s3fetch.run()
    testfile = Path(tmp_path) / "01_small_test_file"
    assert testfile.is_file()
    file_contents = testfile.read_text()
    assert file_contents == "This is the first test file.\n"


def test_only_single_small_file_is_downloaded(bucket_config, tmp_path):
    config = {
        "s3_uri": f"{bucket_config['bucket']}/small_files/01_small_test_file",
        "region": bucket_config["region"],
        "download_dir": tmp_path,
    }
    s3fetch = S3Fetch(**config)
    s3fetch.run()
    download_dir = Path(tmp_path)
    assert len([download_dir.iterdir()]) == 1
