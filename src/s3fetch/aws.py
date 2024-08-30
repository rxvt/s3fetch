"""This module contains functions to interact with AWS."""

import logging

import boto3
import botocore
from botocore.endpoint import MAX_POOL_CONNECTIONS
from mypy_boto3_s3.client import S3Client

logger = logging.getLogger(__name__)


def calc_connection_pool_size(threads: int, max_conncurrency: int) -> int:
    """Calculate the connection pool size.

    Args:
        threads (int): Number of threads to use.
        max_conncurrency (int): Maximum concurrency per thread.

    Returns:
        int: Connection pool size.
    """
    # https://stackoverflow.com/questions/53765366/urllib3-connectionpool-connection-pool-is-full-discarding-connection
    # https://github.com/boto/botocore/issues/619#issuecomment-461859685
    # https://urllib3.readthedocs.io/en/latest/reference/urllib3.connection.html#urllib3.connection.HTTPConnectionPool
    # max_pool_connections is passed to max_size param of urllib3.HTTPConnectionPool()
    #
    # The connection pool is shared across all threads so we need to make sure
    # the connection pool size is large enough to handle all the thread connections
    # taking into the concurrency of each thread connection.
    conn_pool_size = max(
        MAX_POOL_CONNECTIONS,
        threads * max_conncurrency,
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
    client_config = botocore.config.Config(  # type: ignore
        max_pool_connections=max_conn_pool,
    )
    client = boto3.client("s3", region_name=region, config=client_config)
    return client
