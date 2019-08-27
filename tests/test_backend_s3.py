import io
import typing as tp

import botocore
import pytest
import with_cloud_blob.backend_intf as intf
import with_cloud_blob.backends.storage_s3


DATA = b"\x00\x80\xff\xf0\xc2\x80"
DATA = b"abcd"

storage_backend = tp.cast(intf.IStorageBackend, with_cloud_blob.backends.storage_s3.Backend)


def read_s3_obj(s3_bucket: tp.Any, key: str) -> bytes:
    with io.BytesIO() as f:
        s3_bucket.download_fileobj(key, f)
        return f.getvalue()


def s3_key_exists(s3_bucket: tp.Any, key: str) -> bool:
    try:
        s3_bucket.Object(key).load()
        return True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            return False

        raise


def test_read(
    s3_bucket: tp.Any,
    s3_read_options: intf.Options,
) -> None:
    s3_bucket.put_object(Key="file1", Body=DATA)

    data = storage_backend.load(
        loc=f"{s3_bucket.name}/file1",
        opts=s3_read_options,
    )

    assert data == DATA


def test_read_bad_opts() -> None:
    with pytest.raises(intf.UnsupportedOptionsError):
        storage_backend.load(loc="", opts=intf.Options({"x": "y"}))


def test_read_bad_loc(s3_read_options: intf.Options) -> None:
    with pytest.raises(intf.BackendError, match=r".*one slash.*"):
        storage_backend.load(loc="", opts=s3_read_options)


def test_read_nonexistent_bucket(s3_read_options: intf.Options) -> None:
    with pytest.raises(intf.BackendError):
        storage_backend.load(loc="with-cloud-blob-test-nonexistent/file1", opts=s3_read_options)


def test_read_nonexistent_key(s3_bucket: tp.Any, s3_read_options: intf.Options) -> None:
    with pytest.raises(intf.BackendError):
        storage_backend.load(loc=f"{s3_bucket.name}/file1", opts=s3_read_options)


@pytest.mark.parametrize('delay_put', [0])
def test_modify_nonexistent_bucket(s3_modify_options: intf.Options) -> None:
    def modifier(data: tp.Optional[bytes]) -> tp.Optional[bytes]:
        assert False, "should not reach here"

    with pytest.raises(intf.BackendError, match=r".*bucket.*"):
        storage_backend.modify(
            loc="with-cloud-blob-test-nonexistent/file1",
            modifier=modifier,
            opts=s3_modify_options,
        )


# TODO test with and without dynamodb

@pytest.mark.parametrize('delay_put', [0, 1])
def test_modify(
    s3_bucket: tp.Any,
    s3_modify_options: intf.Options,
    delay_put: int,
) -> None:
    loc = f"{s3_bucket.name}/file1"

    def modifier1(data: tp.Optional[bytes]) -> tp.Optional[bytes]:
        assert data is None
        return DATA

    storage_backend.modify(
        loc=loc,
        modifier=modifier1,
        opts=s3_modify_options,
    )

    if not delay_put:
        assert read_s3_obj(s3_bucket, "file1") == DATA

    def modifier2(data: tp.Optional[bytes]) -> tp.Optional[bytes]:
        assert data == DATA
        return DATA + DATA

    storage_backend.modify(
        loc=loc,
        modifier=modifier2,
        opts=s3_modify_options,
    )

    if not delay_put:
        assert read_s3_obj(s3_bucket, "file1") == DATA + DATA

        mtime_before3 = s3_bucket.Object("file1").last_modified

    def modifier3(data: tp.Optional[bytes]) -> tp.Optional[bytes]:
        return data

    storage_backend.modify(
        loc=loc,
        modifier=modifier3,
        opts=s3_modify_options,
    )

    if not delay_put:
        mtime_after3 = s3_bucket.Object("file1").last_modified

        assert mtime_before3 == mtime_after3
        assert read_s3_obj(s3_bucket, "file1") == DATA + DATA

    def modifier4(data: tp.Optional[bytes]) -> tp.Optional[bytes]:
        assert data == DATA + DATA
        return None

    storage_backend.modify(
        loc=loc,
        modifier=modifier4,
        opts=s3_modify_options,
    )

    if not delay_put:
        assert not s3_key_exists(s3_bucket, "file1")
