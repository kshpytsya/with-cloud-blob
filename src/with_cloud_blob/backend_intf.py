import typing as tp

import implements


StorageModifier = tp.Callable[[tp.Optional[bytes]], tp.Optional[bytes]]


class BackendError(RuntimeError):
    pass


class TimeoutError(BackendError):
    pass


class UnsupportedOptionsError(BackendError):
    pass


class Options:
    def __init__(self, d: tp.Mapping[str, str]) -> None:
        self._opts = d
        self._used: tp.Set[str] = set()

    def get(self, key: str) -> tp.Optional[str]:
        self._used.add(key)
        return self._opts.get(key)

    def mapped_update(
        self,
        mapping: tp.Mapping[str, str],
        d: tp.Optional[tp.Dict[str, str]] = None,
    ) -> tp.Dict[str, str]:
        if d is None:
            d = {}

        for opt_name, dest_name in mapping.items():
            value = self.get(opt_name)
            if value is not None:
                d[dest_name] = value

        return d

    def fail_on_unused(self) -> None:
        unused = [f"{k}={v}" for k, v in sorted(self._opts.items()) if k not in self._used]
        if unused:
            raise UnsupportedOptionsError(f"unsupported options passed: {', '.join(unused)}")


class IStorageBackend(implements.Interface):
    @staticmethod
    def modify(
        *,
        loc: str,
        modifier: StorageModifier,
        opts: Options,
    ) -> None:
        """
        """

    @staticmethod
    def load(
        *,
        loc: str,
        opts: Options,
    ) -> bytes:
        """
        """


class ILockBackend(implements.Interface):
    @staticmethod
    def make_lock(
        *,
        loc: str,
        opts: Options,
        timeout: float,
    ) -> tp.ContextManager[None]:
        """
        """
