import logging

from .api import download
from .s3 import DownloadResult
from .utils import ProgressProtocol

logger = logging.getLogger("s3fetch")

__version__ = "2.0.0"

__all__ = [
    "download",
    "DownloadResult",
    "ProgressProtocol",
]
