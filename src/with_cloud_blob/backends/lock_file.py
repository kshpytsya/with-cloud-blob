import typing as tp

import filelock
import implements
import with_cloud_blob.backend_intf as intf


class _Lock(filelock.FileLock):
    def __enter__(self) -> None:
        try:
            filelock.FileLock.__enter__(self)
        except filelock.Timeout:
            raise intf.TimeoutError()


@implements.implements(intf.ILockBackend)
class Backend:
    @staticmethod
    def make_lock(
        *,
        loc: str,
        opts: intf.Options,
        timeout: float,
    ) -> tp.ContextManager[None]:
        return _Lock(loc + ".lock", timeout=timeout)
