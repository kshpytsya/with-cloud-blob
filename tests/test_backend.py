import pytest
import with_cloud_blob.backend_intf as intf
import with_cloud_blob.backends as be


def test_nonexistent_storage_backend() -> None:
    with pytest.raises(intf.BackendError):
        be.storage_backend("")


def test_nonexistent_lock_backend() -> None:
    with pytest.raises(intf.BackendError):
        be.lock_backend("")
