def test_default_max_pool_connections_is_10():
    from botocore import endpoint

    assert getattr(endpoint, "MAX_POOL_CONNECTIONS") == 10  # noqa: B009


def test_default_s3_concurrency_is_10():
    from boto3.s3.transfer import TransferConfig

    config = TransferConfig()
    assert getattr(config, "max_concurrency") == 10  # noqa: B009
