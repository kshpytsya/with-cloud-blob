import os
import pathlib
import typing as tp

import filelock
import pytest
import with_cloud_blob.backend_intf as intf
import with_cloud_blob.backends.file as bf


DATA = b"\x00\x80\xff\xf0\xc2\x80"


def test_read(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "file1"
    path.write_bytes(DATA)

    i = tp.cast(intf.IBackend, bf.FileBackend)

    data = i.load(loc=str(path), opts={})

    assert data == DATA


def test_read_bad_opts() -> None:
    i = tp.cast(intf.IBackend, bf.FileBackend)
    with pytest.raises(intf.UnsupportedOptionsError):
        i.load(loc=os.path.devnull, opts={"x": "y"})


def test_read_nonexistent(tmp_path: pathlib.Path) -> None:
    i = tp.cast(intf.IBackend, bf.FileBackend)
    path = tmp_path / "file1"
    with pytest.raises(intf.BackendError):
        i.load(loc=str(path), opts={})


def test_write(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "file1"

    i = tp.cast(intf.IBackend, bf.FileBackend)

    def modifier1(data: tp.Optional[bytes]) -> tp.Optional[bytes]:
        assert data is None
        lock_fname = str(path) + ".lock"
        assert pathlib.Path(lock_fname).exists()
        with pytest.raises(filelock.Timeout):
            with filelock.FileLock(lock_fname, timeout=0):
                pass  # pragma: no cover

        return DATA

    i.modify(
        loc=str(path),
        extra_locks=[],
        modifier=modifier1,
        opts={},
    )

    data = path.read_bytes()
    assert data == DATA

    extra_locks = [str(path) + f".{i}" for i in range(5)]

    def modifier2(data: tp.Optional[bytes]) -> tp.Optional[bytes]:
        assert data == DATA

        for i in [str(path)] + extra_locks:
            lock_fname = i + ".lock"
            assert pathlib.Path(lock_fname).exists()
            with pytest.raises(filelock.Timeout):
                with filelock.FileLock(lock_fname, timeout=0):
                    pass  # pragma: no cover

        return DATA + DATA

    i.modify(
        loc=str(path),
        extra_locks=extra_locks,
        modifier=modifier2,
        opts={},
    )

    data = path.read_bytes()
    assert data == DATA + DATA

    mtime_before = os.stat(path).st_mtime_ns

    def modifier3(data: tp.Optional[bytes]) -> tp.Optional[bytes]:
        return data

    i.modify(
        loc=str(path),
        extra_locks=[],
        modifier=modifier3,
        opts={},
    )

    mtime_after = os.stat(path).st_mtime_ns

    assert mtime_before == mtime_after

    def modifier4(data: tp.Optional[bytes]) -> tp.Optional[bytes]:
        assert data == DATA + DATA
        return None

    i.modify(
        loc=str(path),
        extra_locks=[],
        modifier=modifier4,
        opts={},
    )

    assert not path.exists()
