"""Integration tests for progress tracking functionality."""

import threading
from pathlib import Path

import boto3
from moto import mock_aws

from s3fetch.s3 import (
    S3FetchQueue,
    add_object_to_download_queue,
    create_download_config,
    create_download_threads,
    create_list_objects_thread,
    download_object,
    list_objects,
)
from s3fetch.utils import ProgressTracker


@mock_aws
def test_progress_tracking_list_objects(tmpdir):
    """Test that progress tracking correctly counts objects found during listing."""
    # Setup mock S3
    s3_client = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    # Create test objects
    test_objects = [
        "data/file1.txt",
        "data/file2.txt",
        "data/subdir/file3.txt",
        "config/settings.json",
        "logs/app.log",
    ]

    for obj_key in test_objects:
        s3_client.put_object(
            Bucket=bucket_name, Key=obj_key, Body=f"Content of {obj_key}"
        )

    # Setup progress tracking
    progress_tracker = ProgressTracker()
    download_queue = S3FetchQueue()
    exit_event = threading.Event()

    # List objects with progress tracking
    list_objects(
        client=s3_client,
        queue=download_queue,
        bucket=bucket_name,
        prefix="",
        delimiter="/",
        regex=None,
        exit_event=exit_event,
        progress_tracker=progress_tracker,
    )

    # Verify progress tracking
    stats = progress_tracker.get_stats()
    assert stats["objects_found"] == len(test_objects)
    assert stats["objects_downloaded"] == 0  # No downloads yet
    assert stats["bytes_downloaded"] == 0

    # Verify queue has all objects
    found_objects = []
    try:
        while True:
            found_objects.append(download_queue.get(block=False))
    except Exception:  # noqa: S110
        pass

    assert len(found_objects) == len(test_objects)


@mock_aws
def test_progress_tracking_downloads(tmpdir):
    """Test that progress tracking correctly counts downloaded objects and bytes."""
    # Setup mock S3
    s3_client = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    # Create test objects with known content sizes
    test_data = {
        "file1.txt": "This is file 1 content",
        "file2.txt": "This is file 2 with more content than file 1",
        "file3.txt": "Short",
    }

    for obj_key, content in test_data.items():
        s3_client.put_object(Bucket=bucket_name, Key=obj_key, Body=content)

    download_dir = Path(tmpdir)
    progress_tracker = ProgressTracker()
    completed_queue = S3FetchQueue()
    download_config = create_download_config()

    # Download each object individually and track progress
    for obj_key in test_data.keys():
        dest_filename = download_dir / obj_key
        download_object(
            key=obj_key,
            dest_filename=dest_filename,
            client=s3_client,
            bucket=bucket_name,
            download_config=download_config,
            completed_queue=completed_queue,
            dry_run=False,
            progress_tracker=progress_tracker,
        )

    # Verify progress tracking
    stats = progress_tracker.get_stats()
    assert stats["objects_downloaded"] == len(test_data)

    # Calculate expected total bytes
    expected_bytes = sum(len(content.encode()) for content in test_data.values())
    assert stats["bytes_downloaded"] == expected_bytes

    # Verify files were actually created
    for obj_key in test_data.keys():
        assert (download_dir / obj_key).exists()


@mock_aws
def test_progress_tracking_with_regex_filtering(tmpdir):
    """Test progress tracking with regex filtering."""
    # Setup mock S3
    s3_client = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    # Create test objects - some matching regex, some not
    all_objects = [
        "data/important.txt",
        "data/temp.tmp",
        "logs/important.log",
        "logs/temp.tmp",
        "config/important.json",
        "config/temp.tmp",
    ]

    for obj_key in all_objects:
        s3_client.put_object(
            Bucket=bucket_name, Key=obj_key, Body=f"Content of {obj_key}"
        )

    # Use regex to only match files with "important" in the name
    regex = r".*important.*"
    expected_matches = [obj for obj in all_objects if "important" in obj]

    progress_tracker = ProgressTracker()
    download_queue = S3FetchQueue()
    exit_event = threading.Event()

    # List objects with regex filtering
    list_objects(
        client=s3_client,
        queue=download_queue,
        bucket=bucket_name,
        prefix="",
        delimiter="/",
        regex=regex,
        exit_event=exit_event,
        progress_tracker=progress_tracker,
    )

    # Verify only matching objects were tracked
    stats = progress_tracker.get_stats()
    assert stats["objects_found"] == len(expected_matches)


@mock_aws
def test_progress_tracking_dry_run_mode(tmpdir):
    """Test that progress tracking works correctly in dry run mode."""
    # Setup mock S3
    s3_client = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    # Create test object
    test_content = "This is test content for dry run"
    s3_client.put_object(Bucket=bucket_name, Key="test.txt", Body=test_content)

    download_dir = Path(tmpdir)
    progress_tracker = ProgressTracker()
    completed_queue = S3FetchQueue()
    download_config = create_download_config()

    # Download in dry run mode
    download_object(
        key="test.txt",
        dest_filename=download_dir / "test.txt",
        client=s3_client,
        bucket=bucket_name,
        download_config=download_config,
        completed_queue=completed_queue,
        dry_run=True,
        progress_tracker=progress_tracker,
    )

    # In dry run mode, no bytes should be tracked (file not downloaded)
    stats = progress_tracker.get_stats()
    assert stats["objects_downloaded"] == 0
    assert stats["bytes_downloaded"] == 0

    # File should not exist
    assert not (download_dir / "test.txt").exists()


@mock_aws
def test_progress_tracking_integration_with_threading(tmpdir):
    """Test progress tracking with the full threading model."""
    # Setup mock S3
    s3_client = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    # Create multiple test objects
    test_objects = {}
    for i in range(10):
        key = f"file_{i:02d}.txt"
        content = f"This is test file {i} with some content"
        test_objects[key] = content
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=content)

    download_dir = Path(tmpdir)
    progress_tracker = ProgressTracker()
    download_queue = S3FetchQueue()
    completed_queue = S3FetchQueue()
    exit_event = threading.Event()

    # Start listing thread
    list_thread = create_list_objects_thread(
        bucket=bucket_name,
        prefix="",
        client=s3_client,
        download_queue=download_queue,
        delimiter="/",
        regex=None,
        exit_event=exit_event,
        progress_tracker=progress_tracker,
    )
    list_thread.start()

    # Start download threads
    download_config = create_download_config()
    success_count, failed_downloads = create_download_threads(
        client=s3_client,
        threads=3,
        download_queue=download_queue,
        completed_queue=completed_queue,
        exit_event=exit_event,
        bucket=bucket_name,
        prefix="",
        download_dir=download_dir,
        delimiter="/",
        download_config=download_config,
        dry_run=False,
        progress_tracker=progress_tracker,
    )

    # Wait for listing to complete
    list_thread.join()

    # Verify final progress tracking results
    stats = progress_tracker.get_stats()
    assert stats["objects_found"] == len(test_objects)
    assert stats["objects_downloaded"] == len(test_objects)

    # Calculate expected bytes
    expected_bytes = sum(len(content.encode()) for content in test_objects.values())
    assert stats["bytes_downloaded"] == expected_bytes

    # Verify download stats
    assert success_count == len(test_objects)
    assert len(failed_downloads) == 0

    # Verify all files were downloaded
    for obj_key in test_objects.keys():
        assert (download_dir / obj_key).exists()


def test_add_object_to_download_queue_with_progress_tracking():
    """Test the add_object_to_download_queue function with progress tracking."""
    progress_tracker = ProgressTracker()
    queue = S3FetchQueue()

    # Add objects with progress tracking
    add_object_to_download_queue("file1.txt", queue, progress_tracker)
    add_object_to_download_queue("file2.txt", queue, progress_tracker)
    add_object_to_download_queue("file3.txt", queue, progress_tracker)

    # Verify progress tracking
    assert progress_tracker.objects_found == 3

    # Verify queue contains the objects
    assert queue.get() == "file1.txt"
    assert queue.get() == "file2.txt"
    assert queue.get() == "file3.txt"


def test_add_object_to_download_queue_without_progress_tracking():
    """Test that add_object_to_download_queue works without progress tracking."""
    queue = S3FetchQueue()

    # Add objects without progress tracking (should not raise errors)
    add_object_to_download_queue("file1.txt", queue, None)
    add_object_to_download_queue("file2.txt", queue)  # Default None

    # Verify queue contains the objects
    assert queue.get() == "file1.txt"
    assert queue.get() == "file2.txt"


@mock_aws
def test_api_download_with_string_path(tmpdir):
    """Test that download() accepts a string download_dir path."""
    import boto3

    from s3fetch import download

    # Setup mock S3
    s3_client = boto3.client("s3", region_name="us-east-1")
    bucket_name = "test-bucket"
    s3_client.create_bucket(Bucket=bucket_name)

    # Create test object
    s3_client.put_object(Bucket=bucket_name, Key="test.txt", Body="test content")

    # Test with string path (not Path object)
    success_count, failed_downloads = download(
        f"s3://{bucket_name}/",
        download_dir=str(tmpdir),  # Pass string instead of Path
        client=s3_client,
    )

    # Verify download worked
    assert success_count == 1
    assert len(failed_downloads) == 0
    assert (tmpdir / "test.txt").exists()
