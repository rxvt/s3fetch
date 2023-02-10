class S3FetchError(Exception):
    pass


class RegexError(S3FetchError):
    pass


class DirectoryDoesNotExistError(S3FetchError):
    pass


class PermissionError(S3FetchError):
    pass


class DownloadError(S3FetchError):
    pass


class NoObjectsFoundError(S3FetchError):
    pass


class NoCredentialsError(S3FetchError):
    pass


class InvalidCredentialsError(S3FetchError):  # noqa: D101
    pass


class S3FetchQueueEmpty(S3FetchError):  # noqa: D101
    pass
