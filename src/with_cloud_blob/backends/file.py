import pathlib
import typing as tp

import atomicwrites
import filelock
import implements
import with_cloud_blob.backend_intf as intf


@implements.implements(intf.ILockBackend)
class LockBackend:
    @staticmethod
    def make_lock(
        *,
        loc: str,
        opts: intf.Options,
    ) -> tp.ContextManager[tp.Any]:
        intf.check_unknown_options(opts)
        return filelock.FileLock(loc + ".lock")


@implements.implements(intf.IStorageBackend)
class StorageBackend:
    @staticmethod
    def modify(
        *,
        loc: str,
        modifier: intf.StorageModifier,
        opts: intf.Options,
    ) -> None:
        intf.check_unknown_options(opts)

        path = pathlib.Path(loc)
        data: tp.Optional[bytes]
        if path.exists():
            data = path.read_bytes()
        else:
            data = None

        new_data = modifier(data)

        if new_data != data:
            if new_data is None:
                path.unlink()
            else:
                with atomicwrites.atomic_write(str(path), overwrite=True, mode="wb") as f:
                    f.write(new_data)

    @staticmethod
    def load(
        *,
        loc: str,
        opts: intf.Options,
    ) -> bytes:
        intf.check_unknown_options(opts)

        try:
            return pathlib.Path(loc).read_bytes()
        except OSError as e:
            raise intf.BackendError(e)
