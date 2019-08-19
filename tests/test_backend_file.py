import contextlib
import os
import pathlib
import typing as tp

import filelock
import pytest
import with_cloud_blob.backend_intf as intf
import with_cloud_blob.backends.file as bf


DATA = b"\x00\x80\xff\xf0\xc2\x80"


storage_backend = tp.cast(intf.IStorageBackend, bf.StorageBackend)
lock_backend = tp.cast(intf.ILockBackend, bf.LockBackend)


def test_read(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "file1"
    path.write_bytes(DATA)

    data = storage_backend.load(loc=str(path), opts={})

    assert data == DATA


def test_read_bad_opts() -> None:
    with pytest.raises(intf.UnsupportedOptionsError):
        storage_backend.load(loc=os.path.devnull, opts={"x": "y"})


def test_read_nonexistent(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "file1"
    with pytest.raises(intf.BackendError):
        storage_backend.load(loc=str(path), opts={})


@pytest.mark.parametrize('count', [1, 5, 50])
def test_lock(tmp_path: pathlib.Path, count: int) -> None:
    path = tmp_path / "file1"

    with contextlib.ExitStack() as es:
        for i in range(count):
            lock_name = f"{path}.{i}"
            lock_fname = f"{lock_name}.lock"
            es.enter_context(lock_backend.make_lock(loc=lock_name, opts={}))
            assert pathlib.Path(lock_fname).exists()

            with pytest.raises(filelock.Timeout):
                with filelock.FileLock(lock_fname, timeout=0):
                    pass  # pragma: no cover


def test_write(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "file1"

    def modifier1(data: tp.Optional[bytes]) -> tp.Optional[bytes]:
        assert data is None
        return DATA

    storage_backend.modify(
        loc=str(path),
        modifier=modifier1,
        opts={},
    )

    data = path.read_bytes()
    assert data == DATA

    def modifier2(data: tp.Optional[bytes]) -> tp.Optional[bytes]:
        assert data == DATA
        return DATA + DATA

    storage_backend.modify(
        loc=str(path),
        modifier=modifier2,
        opts={},
    )

    data = path.read_bytes()
    assert data == DATA + DATA

    mtime_before = path.stat().st_mtime_ns

    def modifier3(data: tp.Optional[bytes]) -> tp.Optional[bytes]:
        return data

    storage_backend.modify(
        loc=str(path),
        modifier=modifier3,
        opts={},
    )

    mtime_after = path.stat().st_mtime_ns

    assert mtime_before == mtime_after

    def modifier4(data: tp.Optional[bytes]) -> tp.Optional[bytes]:
        assert data == DATA + DATA
        return None

    storage_backend.modify(
        loc=str(path),
        modifier=modifier4,
        opts={},
    )

    assert not path.exists()
