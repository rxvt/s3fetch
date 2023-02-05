"""S3Fetch conftest."""
import os
from typing import Generator

import boto3
import pytest


@pytest.fixture(scope="package", autouse=True)
def aws_credentials():
    """Mock AWS Credentials for Moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"  # noqa: S105
    os.environ["AWS_SECURITY_TOKEN"] = "testing"  # noqa: S105
    os.environ["AWS_SESSION_TOKEN"] = "testing"  # noqa: S105
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
