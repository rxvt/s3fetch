"""Filesystem utilities for S3Fetch."""

from pathlib import Path
from typing import Optional

from .exceptions import DirectoryDoesNotExistError


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
    # Build the absolute directory path converting the object delimiter into a local
    # directory delimiter.
    if object_dir:
        directories = object_dir.split(delimiter)
        tmp_dir = Path(*directories)
        absolute_directory = download_dir / Path(tmp_dir)
    else:
        absolute_directory = download_dir

    absolute_directory.mkdir(parents=True, exist_ok=True)
    return absolute_directory


def check_download_dir_exists(download_dir: Path) -> None:
    """Check if the download directory exists.

    Args:
        download_dir (Path): Download directory, e.g. /tmp.

    Raises:
        DirectoryDoesNotExistError: If the download directory does not exist.
    """
    if not download_dir.is_dir():
        raise DirectoryDoesNotExistError(
            f"The directory '{download_dir}' does not exist."
        )
