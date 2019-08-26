import typing as tp

import filelock
import implements
import with_cloud_blob.backend_intf as intf


class MyFileLock(filelock.FileLock):
    def __enter__(self) -> tp.Any:
        try:
            return filelock.FileLock.__enter__(self)
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
    ) -> tp.ContextManager[tp.Any]:
        return MyFileLock(loc + ".lock", timeout=timeout)
