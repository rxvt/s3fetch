"""This module contains functions to interact with AWS."""

import logging

import boto3
from botocore.config import Config
from botocore.endpoint import MAX_POOL_CONNECTIONS
from mypy_boto3_s3.client import S3Client

logger = logging.getLogger(__name__)


def calc_connection_pool_size(threads: int, max_concurrency: int) -> int:
    """Calculate the connection pool size.

    Args:
        threads (int): Number of threads to use.
        max_concurrency (int): Maximum concurrency per thread.

    Returns:
        int: Connection pool size.
    """
    # The connection pool is shared across all threads.
    # To avoid connection starvation, set the pool size to the greater of:
    # - botocore's default MAX_POOL_CONNECTIONS
    # - the total possible concurrent S3 transfers (threads * max_concurrency)
    conn_pool_size = max(
        MAX_POOL_CONNECTIONS,
        threads * max_concurrency,
    )
    logger.debug(f"Setting urllib3 ConnectionPool(maxsize={conn_pool_size})")
    return conn_pool_size


def get_client(region: str, max_conn_pool: int) -> S3Client:
    """Get an S3 client.

    Args:
        region (str): AWS region, e.g. 'us-east-1'.
        max_conn_pool (int): Maximum connection pool size.

    Returns:
        S3Client: Boto3 S3 client.
    """
    client_config = Config(
        max_pool_connections=max_conn_pool,
    )
    client = boto3.client("s3", region_name=region, config=client_config)
    return client
