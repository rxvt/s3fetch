import os
from typing import Generator

import boto3
import pytest
from moto import mock_s3


@pytest.fixture(scope="package", autouse=True)
def aws_credentials() -> None:
    """Mock AWS Credentials for Moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"  # noqa: S105
    os.environ["AWS_SECURITY_TOKEN"] = "testing"  # noqa: S105
    os.environ["AWS_SESSION_TOKEN"] = "testing"  # noqa: S105
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture()
def s3_client(scope="package") -> Generator:  # noqa: ANN001
    """Return a mock S3Client object."""
    with mock_s3():
        client = boto3.client("s3", region_name="us-east-1")
        yield client
