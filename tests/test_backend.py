import typing as tp

import pytest
import with_cloud_blob.backend_intf as intf
import with_cloud_blob.backends as be


def test_nonexistent_storage_backend() -> None:
    with pytest.raises(intf.BackendError):
        be.storage_backend("")


def test_nonexistent_lock_backend() -> None:
    with pytest.raises(intf.BackendError):
        be.lock_backend("")


def test_list_backends() -> None:
    items = be.list_backends()
    assert set(items.keys()) == {"lock", "storage"}
    assert "file" in items["lock"]
    assert "file" in items["storage"]


def test_mapped_update() -> None:
    d1: tp.Dict[str, str] = {
        "kw3": "val3a",
    }
    d2 = intf.Options({
        "o1": "val1",
        "o2": "val2",
        "o3": "val3b",
    }).mapped_update(
        {
            "o1": "kw1",
            "o2": "kw2",
        }, d1,
    )
    assert d2 is d1
    assert d2 == {
        "kw1": "val1",
        "kw2": "val2",
        "kw3": "val3a",
    }
