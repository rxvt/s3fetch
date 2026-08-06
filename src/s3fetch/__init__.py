import logging

from .api import download
from .s3 import DownloadResult
from .utils import ProgressProtocol

logger = logging.getLogger("s3fetch")

__version__ = "2.1.0"

__all__ = [
    "DownloadResult",
    "ProgressProtocol",
    "download",
]
