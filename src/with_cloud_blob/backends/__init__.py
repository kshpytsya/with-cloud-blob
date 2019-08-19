import functools as _functools
import typing as _tp

import pkg_resources as _pkg_resources
import with_cloud_blob.backend_intf as intf


@_functools.lru_cache()
def _backends() -> _tp.Dict[str, _tp.Dict[str, _tp.Callable[[], _tp.Any]]]:
    return {
        kind: {
            i.name: i.load
            for i in _pkg_resources.iter_entry_points(f"with_cloud_blob.{kind}_backends")
        }
        for kind in ("storage", "lock")
    }


@_functools.lru_cache()
def storage_backend(name: str) -> intf.IStorageBackend:
    try:
        return _tp.cast(intf.IStorageBackend, _backends()["storage"][name]())
    except KeyError:
        raise intf.BackendError(f"unknown storage backend: {name}")


@_functools.lru_cache()
def lock_backend(name: str) -> intf.ILockBackend:
    try:
        return _tp.cast(intf.ILockBackend, _backends()["lock"][name]())
    except KeyError:
        raise intf.BackendError(f"unknown lock backend: {name}")
