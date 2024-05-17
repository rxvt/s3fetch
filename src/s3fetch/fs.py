"""Filesystem utilities for S3Fetch."""

from pathlib import Path
from typing import Optional


def create_destination_directory(
    download_dir: Path,
    object_dir: Optional[str],
    delimiter: str,
) -> Path:
    """Create the local destination directory for the object.

    Args:
        download_dir (Path): The local download base dirctory.
        object_dir (Union[str, Path]): The directory structure we will create for the
            object under the base download directory.
        delimiter (str): The delimiter used to split the object key into directories.

    Returns:
        Path: The absolute path to the local destination directory.
    """
    # Build the absolute directory path converting the object delimiter into a local
    # directory delimiter.
    if object_dir:
        directories = object_dir.split(delimiter)
        tmp_dir = Path()
        for directory in directories:
            tmp_dir = tmp_dir / Path(directory)
        absolute_directory = download_dir / Path(tmp_dir)
    else:
        absolute_directory = download_dir

    absolute_directory.mkdir(parents=True, exist_ok=True)
    return absolute_directory
