import typing as _tp

import boto3 as _boto3
import with_cloud_blob.backend_intf as _intf


def boto_session(
    opts: _intf.Options,
) -> _boto3.Session:
    kw = opts.mapped_update({
        "profile": "profile_name",
    })
    return _boto3.Session(**kw)


def boto_resource_s3(
    session: _boto3.Session,
    opts: _intf.Options,
) -> _tp.Any:
    kw = opts.mapped_update({
        "region": "region_name",
        "endpoint": "endpoint_url",
    })
    return session.resource("s3", **kw)


def boto_resource_dynamodb(
    session: _boto3.Session,
    opts: _intf.Options,
) -> _tp.Any:
    kw = opts.mapped_update({
        "region": "region_name",
        "dynamodb_endpoint": "endpoint_url",
    })
    return session.resource("dynamodb", **kw)
