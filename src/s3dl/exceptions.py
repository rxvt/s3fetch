class S3dlError(Exception):
    pass


class RegexError(S3dlError):
    pass


class DirectoryDoesNotExistError(S3dlError):
    pass


class PermissionError(S3dlError):
    pass