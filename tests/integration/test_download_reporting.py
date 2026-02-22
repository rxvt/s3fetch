"""Integration test for S3 download reporting functionality."""

import threading
from pathlib import Path

import boto3
from moto import mock_aws

from s3fetch import api, download, s3
from s3fetch.exceptions import S3FetchQueueClosed
from s3fetch.s3 import DownloadResult


@mock_aws
def test_download_on_complete_callback(tmpdir):
    """on_complete callback is invoked once per successfully downloaded object."""
    s3_client = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    keys = ["data/file1.txt", "data/file2.txt", "data/file3.txt"]
    for key in keys:
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=f"body of {key}")

    completed_keys: list[str] = []

    def on_done(key: str) -> None:
        completed_keys.append(key)

    success_count, failures = download(
        f"s3://{bucket_name}/data/",
        download_dir=str(tmpdir),
        client=s3_client,
        on_complete=on_done,
    )

    assert success_count == 3
    assert failures == []
    assert sorted(completed_keys) == sorted(keys)


@mock_aws
def test_download_on_complete_not_called_on_failure(tmpdir):
    """on_complete callback is NOT called for objects that fail to download."""
    s3_client = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    # Put one valid object; the second key won't exist
    s3_client.put_object(Bucket=bucket_name, Key="good.txt", Body="ok")

    completed_keys: list[str] = []

    # Manually drive the internals so we can inject a bad key alongside a good one
    download_queue = s3.get_queue("download")
    completed_queue: s3.S3FetchQueue[DownloadResult] = s3.S3FetchQueue()
    exit_event = threading.Event()

    download_queue.put("good.txt")
    download_queue.put("missing.txt")
    download_queue.close()

    def on_done(key: str) -> None:
        completed_keys.append(key)

    def _consumer(queue: s3.S3FetchQueue[DownloadResult]) -> None:
        from s3fetch.exceptions import S3FetchQueueClosed as Closed

        while True:
            try:
                result = queue.get(block=True)
                if result.success:
                    on_done(result.key)
            except Closed:
                break

    api.create_completed_objects_thread(queue=completed_queue, func=_consumer)

    success_count, failures = api._download_objects(
        client=s3_client,
        threads=1,
        download_queue=download_queue,
        completed_queue=completed_queue,
        exit_event=exit_event,
        bucket=bucket_name,
        prefix="",
        download_dir=Path(tmpdir),
        delimiter="/",
        download_config=s3.create_download_config(),
    )

    import time

    time.sleep(0.1)

    assert success_count == 1
    assert len(failures) == 1
    assert completed_keys == ["good.txt"]


@mock_aws
def test_download_success_and_failure_reporting(tmpdir):
    """Test that successful and failed downloads are correctly reported."""
    # Setup mock S3
    s3_client = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    # Create valid test objects
    valid_keys = [
        "valid/file1.txt",
        "valid/file2.txt",
    ]

    for key in valid_keys:
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=f"Content of {key}")

    download_dir = Path(tmpdir)

    # Download valid objects — should all succeed
    success_count, failures = download(
        f"s3://{bucket_name}/valid/",
        download_dir=download_dir,
        client=s3_client,
    )

    assert success_count == len(valid_keys), "Should have 2 successful downloads"
    assert len(failures) == 0, "No failures expected for existing objects"

    # Verify files were actually downloaded
    for key in valid_keys:
        file_name = key.rsplit("/", 1)[-1]
        file_path = download_dir / file_name
        assert file_path.exists(), f"Downloaded file {file_path} should exist"
        assert file_path.read_text() == f"Content of {key}"


@mock_aws
def test_failed_download_emits_download_result_with_success_false(tmpdir):
    """Failed downloads emit a DownloadResult(success=False) on the completed queue."""
    s3_client = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    download_queue = s3.get_queue("download")
    completed_queue = s3.get_queue("completion")
    exit_event = threading.Event()

    # Only enqueue a key that does not exist in S3
    non_existent_key = "does/not/exist.txt"
    download_queue.put(non_existent_key)
    download_queue.close()

    download_dir = Path(tmpdir)
    download_config = s3.create_download_config()

    success_count, failures = api._download_objects(
        client=s3_client,
        threads=1,
        download_queue=download_queue,
        completed_queue=completed_queue,
        exit_event=exit_event,
        bucket=bucket_name,
        prefix="",
        download_dir=download_dir,
        delimiter="/",
        download_config=download_config,
        dry_run=False,
    )

    assert success_count == 0
    assert len(failures) == 1

    # The failure should still emit a DownloadResult on the completed queue
    completed_items = []
    try:
        while True:
            completed_items.append(completed_queue.get())
    except S3FetchQueueClosed:
        pass

    assert len(completed_items) == 1
    result = completed_items[0]
    assert isinstance(result, DownloadResult)
    assert result.key == non_existent_key
    assert result.success is False
    assert result.error is not None
    assert result.file_size == 0


@mock_aws
def test_create_completed_objects_thread_with_custom_handler(tmpdir):
    """create_completed_objects_thread works with a custom DownloadResult handler."""
    s3_client = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    test_objects = {"file1.txt": "aaa", "file2.txt": "bb"}
    for key, body in test_objects.items():
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=body)

    download_queue = s3.get_queue("download")
    completed_queue: s3.S3FetchQueue[DownloadResult] = s3.S3FetchQueue()
    exit_event = threading.Event()

    for key in test_objects:
        download_queue.put(key)
    download_queue.close()

    # Custom handler that collects results
    collected: list[DownloadResult] = []

    def my_handler(queue: s3.S3FetchQueue[DownloadResult]) -> None:
        from s3fetch.exceptions import S3FetchQueueClosed as Closed

        while True:
            try:
                collected.append(queue.get(block=True))
            except Closed:
                break

    api.create_completed_objects_thread(queue=completed_queue, func=my_handler)

    download_dir = Path(tmpdir)
    download_config = s3.create_download_config()

    success_count, failures = api._download_objects(
        client=s3_client,
        threads=2,
        download_queue=download_queue,
        completed_queue=completed_queue,
        exit_event=exit_event,
        bucket=bucket_name,
        prefix="",
        download_dir=download_dir,
        delimiter="/",
        download_config=download_config,
    )

    # Give the handler thread a moment to finish draining
    import time

    time.sleep(0.1)

    assert success_count == 2
    assert len(failures) == 0
    assert len(collected) == 2
    assert all(isinstance(r, DownloadResult) for r in collected)
    assert all(r.success for r in collected)
    assert {r.key for r in collected} == set(test_objects)
    # file_size should reflect actual content sizes
    for r in collected:
        expected_size = len(test_objects[r.key].encode())
        assert r.file_size == expected_size
