#!/usr/bin/env python3
"""Script to populate S3 test bucket with test data for s3fetch.

This script creates approximately 500 test objects in the S3 bucket to thoroughly
test s3fetch functionality including:

File Types Created:
- Small files (120): 1KB-100KB for basic download testing
- Medium files (80): 1-10MB for concurrent download testing
- Large files (40): 50-100MB for streaming and performance testing
- Extension variety (120): Various file extensions for regex filtering
- Date patterns (80): Files with date formats for regex pattern testing
- Sequential files (100): Numbered sequences (image_001.jpg to image_100.jpg)
- Directory structures (50): Files at different nesting levels
- Special characters (20): Files with hyphens, spaces, underscores, etc.
- Edge cases (10): Empty files, very long filenames, unicode characters

Usage:
    # See what would be created without uploading
    python scripts/populate_test_bucket.py --dry-run

    # Populate the default test bucket
    python scripts/populate_test_bucket.py

    # Use custom bucket and region
    python scripts/populate_test_bucket.py --bucket my-test-bucket --region us-west-2

Requirements:
- boto3 installed
- AWS credentials configured
- Target S3 bucket must already exist
"""

import argparse
import sys

import boto3
from botocore.exceptions import ClientError


def generate_content(size_bytes: int) -> bytes:
    """Generate content of specified size."""
    return b"\x00" * size_bytes


def create_test_files() -> list[tuple[str, int]]:
    """Create list of (key, size) tuples for test files."""
    files = []

    # Small files (120: 1KB-100KB)
    for i in range(120):
        files.append((f"small/file_{i:03d}.txt", 1024 + i * 800))

    # Medium files (80: 1MB-10MB)
    for i in range(80):
        files.append((f"medium/data_{i:03d}.jpg", 1024 * 1024 + i * 120000))

    # Large files (40: 50MB-100MB)
    for i in range(40):
        files.append((f"large/bigfile_{i:03d}.bin", 50 * 1024 * 1024 + i * 1280000))

    # Different extensions for regex testing (120)
    extensions = [
        ".txt",
        ".csv",
        ".json",
        ".log",
        ".jpg",
        ".png",
        ".pdf",
        ".zip",
        ".xml",
        ".yaml",
        ".mp4",
        ".tar.gz",
        ".sql",
        ".py",
        ".js",
    ]
    for i in range(120):
        ext = extensions[i % len(extensions)]
        files.append((f"extensions/file_{i:03d}{ext}", 5000 + i * 100))

    # Date patterns for regex testing (80)
    for i in range(40):
        files.append((f"logs/log-2024-01-{i + 1:02d}.txt", 2000 + i * 50))
        files.append((f"backups/backup_202401{i + 1:02d}.zip", 10000 + i * 200))

    # Sequential numbering (100)
    for i in range(1, 101):
        files.append((f"sequences/image_{i:03d}.jpg", 8000 + i * 100))

    # Different directory depths (50)
    for i in range(50):
        depth = (i % 6) + 1
        path = "/".join([f"level{j}" for j in range(depth)])
        files.append((f"{path}/file_{i:03d}.txt", 1000 + i * 50))

    # Special characters in names (20)
    special_chars = [
        "-",
        "_",
        " ",
        "@",
        "+",
        ".",
        ",",
        ";",
        "'",
        '"',
        "%",
        "&",
        "(",
        ")",
        "[",
        "]",
        "{",
        "}",
        "=",
        "!",
    ]
    for i, char in enumerate(special_chars):
        files.append((f"special/file{char}with{char}chars_{i:03d}.txt", 1500 + i * 100))

    # Edge cases (10)
    files.append(("edge_cases/empty_file.txt", 0))  # Zero byte file
    files.append(("edge_cases/" + "x" * 200 + ".txt", 100))  # Very long name
    files.append(("edge_cases/файл_русский.txt", 200))  # Unicode
    files.append(("edge_cases/文件_中文.json", 300))  # Unicode Chinese
    files.append(("edge_cases/file_with_emoji_🚀.txt", 400))  # Emoji
    files.append(("edge_cases/FILE_UPPERCASE.TXT", 500))  # All caps
    files.append(("edge_cases/file.multiple.dots.txt", 600))  # Multiple dots
    files.append(("edge_cases/file,with,commas.csv", 700))  # Commas
    files.append(("edge_cases/file with multiple   spaces.txt", 800))  # Multiple spaces
    files.append(("edge_cases/file_ending_with_dot.", 900))  # Ending with dot

    return files


def main() -> None:
    """Main function."""
    parser = argparse.ArgumentParser(description="Populate S3 test bucket")
    parser.add_argument("--bucket", default="s3fetch-cicd-test-bucket")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    # Create S3 client
    try:
        s3_client = boto3.client("s3", region_name=args.region)
        s3_client.head_bucket(Bucket=args.bucket)
    except ClientError as e:
        print(f"Error accessing bucket: {e}")
        sys.exit(1)

    # Generate test files
    files = create_test_files()
    total_size = sum(size for _, size in files)

    print(
        f"Generated {len(files)} files, total size: {total_size / (1024 * 1024):.1f} MB"
    )

    if args.dry_run:
        for key, size in files[:10]:
            print(f"  {key} ({size} bytes)")
        print(f"  ... and {len(files) - 10} more files")
        return

    # Upload files
    print(f"Uploading to s3://{args.bucket}...")

    for i, (key, size) in enumerate(files):
        try:
            content = generate_content(size)
            s3_client.put_object(Bucket=args.bucket, Key=key, Body=content)
            if (i + 1) % 50 == 0:
                print(f"  Uploaded {i + 1}/{len(files)} files...")
        except Exception as e:
            print(f"Failed to upload {key}: {e}")
            sys.exit(1)

    print(f"Upload complete! {len(files)} files uploaded.")


if __name__ == "__main__":
    main()
