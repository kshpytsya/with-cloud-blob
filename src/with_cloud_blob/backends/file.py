import contextlib
import pathlib
import typing as tp

import atomicwrites
import filelock
import implements
import with_cloud_blob.backend_intf as intf


@implements.implements(intf.IBackend)
class FileBackend:
    @staticmethod
    def modify(
        *,
        loc: str,
        extra_locks: tp.Iterable[str],
        modifier: tp.Callable[[tp.Optional[bytes]], tp.Optional[bytes]],
        opts: intf.Options,
    ) -> None:
        intf.check_unknown_options(opts)

        with contextlib.ExitStack() as es:
            for i in list(extra_locks) + [loc]:
                es.enter_context(filelock.FileLock(i + ".lock"))

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
