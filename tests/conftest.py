import time
import typing as tp

import boto3
import common
import pytest
import with_cloud_blob.backend_intf as intf


@pytest.fixture
def s3_bucket() -> tp.Any:
    name = f"with-cloud-blob-test-{time.time_ns()}"
    session = boto3.Session()
    s3 = session.resource("s3", endpoint_url=common.ENDPOINT)
    bucket = s3.Bucket(name)
    bucket.create()
    try:
        yield bucket
    finally:
        bucket.objects.all().delete()
        bucket.delete()


@pytest.fixture
def s3_read_options() -> tp.Generator[intf.Options, None, None]:
    opts = intf.Options(
        {
            "endpoint": common.ENDPOINT,
        },
    )
    yield opts
    opts.fail_on_unused()


@pytest.fixture
def s3_modify_options(delay_put: float) -> tp.Generator[intf.Options, None, None]:
    opts = intf.Options(common.s3_modify_options_dict(delay_put=delay_put))
    yield opts
    opts.fail_on_unused()
