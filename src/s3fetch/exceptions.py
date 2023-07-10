"""Exceptions for the S3Fetch package."""
class S3FetchError(Exception):
    """Base class for all S3Fetch errors."""
    pass


class RegexError(S3FetchError):
    """Raised when the regex cannot be compiled due to an error."""
    pass


class DirectoryDoesNotExistError(S3FetchError):
    """Raised when the directory does not exist."""
    pass


class PermissionError(S3FetchError):
    """Raised when the user does not have permission to access the S3 bucket."""
    pass


class DownloadError(S3FetchError):
    """Raised when an error occurs during download."""
    pass


class NoObjectsFoundError(S3FetchError):
    """Raised when no objects are found in the bucket."""
    pass


class NoCredentialsError(S3FetchError):
    """Raised when no credentials are found."""
    pass


class InvalidCredentialsError(S3FetchError):
    """Raised when invalid credentials are found."""
    pass


class S3FetchQueueEmpty(S3FetchError):
    """Raised when the queue is empty."""
    pass
