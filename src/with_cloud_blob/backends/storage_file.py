import pathlib
import typing as tp

import atomicwrites
import implements
import with_cloud_blob.backend_intf as intf


@implements.implements(intf.IStorageBackend)
class Backend:
    @staticmethod
    def modify(
        *,
        loc: str,
        modifier: intf.StorageModifier,
        opts: intf.Options,
    ) -> None:
        opts.fail_on_unused()
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
        opts.fail_on_unused()
        try:
            return pathlib.Path(loc).read_bytes()
        except OSError as e:
            raise intf.BackendError(e)
