import typing as tp

import implements


Options = tp.Dict[str, str]
StorageModifier = tp.Callable[[tp.Optional[bytes]], tp.Optional[bytes]]


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
    ) -> tp.ContextManager[tp.Any]:
        """
        """


class BackendError(RuntimeError):
    pass


class UnsupportedOptionsError(BackendError):
    pass


def check_unknown_options(opts: Options) -> None:
    if opts:
        raise UnsupportedOptionsError(f"unsupported options passed: {opts}")
