"""Filesystem utilities for S3Fetch."""

import logging
import os
from pathlib import Path
from typing import Optional

from .exceptions import DirectoryDoesNotExistError

logger = logging.getLogger(__name__)


def create_destination_directory(
    download_dir: Path,
    object_dir: Optional[str],
    delimiter: str,
) -> Path:
    """Create the local destination directory for the object.

    Args:
        download_dir (Path): The local download base directory.
        object_dir (Optional[str]): The directory structure we will create for the
            object under the base download directory.
        delimiter (str): The delimiter used to split the object key into directories.

    Returns:
        Path: The absolute path to the local destination directory.
    """
    # Build the absolute directory path by converting the S3 object key's delimiter
    # (e.g., '/') into the local filesystem's directory structure. This ensures that
    # the downloaded file is placed in a directory hierarchy that mirrors the S3 key
    # structure under the specified download base directory.
    if object_dir:
        directories = object_dir.split(delimiter)
        local_download_path = Path(*directories)
        absolute_directory = download_dir / Path(local_download_path)
        logger.debug(
            "Creating destination directory with object_dir: '%s', "
            "delimiter: '%s', resolved path: '%s'",
            object_dir,
            delimiter,
            absolute_directory,
        )
    else:
        absolute_directory = download_dir
        logger.debug(
            "Creating destination directory with no object_dir, "
            "using base download_dir: '%s'",
            absolute_directory,
        )

    absolute_directory.mkdir(parents=True, exist_ok=True)
    logger.info("Ensured directory exists: '%s'", absolute_directory)
    return absolute_directory


def check_download_dir_exists(download_dir: Path) -> None:
    """Check if the download directory exists and is accessible.

    Args:
        download_dir (Path): Download directory, e.g. /tmp.

    Returns:
        None

    Raises:
        DirectoryDoesNotExistError: If the download directory does not exist or is not a
            directory.
        PermissionError: If the directory exists but is not accessible due to
            permissions.
    """
    if not download_dir.exists():
        logger.error("Download directory does not exist: '%s'", download_dir)
        raise DirectoryDoesNotExistError(
            f"The directory '{download_dir}' does not exist."
        )
    if not download_dir.is_dir():
        logger.error("Path exists but is not a directory: '%s'", download_dir)
        raise DirectoryDoesNotExistError(
            f"The path '{download_dir}' exists but is not a directory."
        )
    if not os.access(download_dir, os.R_OK | os.W_OK | os.X_OK):
        logger.error("Insufficient permissions for directory: '%s'", download_dir)
        raise PermissionError(
            f"Insufficient permissions to access directory '{download_dir}'."
        )
    logger.info("Download directory exists and is accessible: '%s'", download_dir)
