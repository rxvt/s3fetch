"""Tests for aws.py module."""

from unittest.mock import patch

import pytest
from botocore.exceptions import NoCredentialsError, ProfileNotFound, TokenRetrievalError

from s3fetch import aws
from s3fetch.exceptions import InvalidCredentialsError


def test_get_client_success():
    """Test successful client creation."""
    region = "us-east-1"
    max_conn_pool = 10

    client = aws.get_client(region=region, max_conn_pool=max_conn_pool)
    assert client is not None


def test_get_client_with_no_credentials():
    """Test client creation when no credentials are found."""
    region = "us-east-1"
    max_conn_pool = 10

    with patch("boto3.client") as mock_boto_client:
        mock_boto_client.side_effect = NoCredentialsError()

        with pytest.raises(NoCredentialsError):
            aws.get_client(region=region, max_conn_pool=max_conn_pool)


def test_get_client_with_sso_token_error():
    """Test client creation when SSO token is invalid or expired."""
    region = "us-east-1"
    max_conn_pool = 10

    with patch("boto3.client") as mock_boto_client:
        mock_boto_client.side_effect = TokenRetrievalError(
            provider="sso", error_msg="Token expired"
        )

        with pytest.raises(
            InvalidCredentialsError, match="SSO token is invalid or expired"
        ):
            aws.get_client(region=region, max_conn_pool=max_conn_pool)


def test_get_client_with_profile_not_found():
    """Test client creation when AWS profile is not found."""
    region = "us-east-1"
    max_conn_pool = 10

    with patch("boto3.client") as mock_boto_client:
        mock_boto_client.side_effect = ProfileNotFound(profile="nonexistent")

        with pytest.raises(InvalidCredentialsError, match="AWS profile not found"):
            aws.get_client(region=region, max_conn_pool=max_conn_pool)


def test_calc_connection_pool_size():
    """Test connection pool size calculation."""
    # Test with small values
    result = aws.calc_connection_pool_size(threads=2, max_concurrency=5)
    assert result == 10  # 2 * 5 = 10

    # Test with larger values that exceed MAX_POOL_CONNECTIONS
    result = aws.calc_connection_pool_size(threads=20, max_concurrency=20)
    assert result == 400  # 20 * 20 = 400

    # Test that it uses MAX_POOL_CONNECTIONS when that's larger
    result = aws.calc_connection_pool_size(threads=1, max_concurrency=1)
    from botocore.endpoint import MAX_POOL_CONNECTIONS

    assert result == MAX_POOL_CONNECTIONS  # Should use the default max
