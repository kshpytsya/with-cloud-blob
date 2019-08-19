import typing as tp

import implements


Options = tp.Dict[str, str]


class IBackend(implements.Interface):
    @staticmethod
    def modify(
        *,
        loc: str,
        extra_locks: tp.Iterable[str],
        modifier: tp.Callable[[tp.Optional[bytes]], tp.Optional[bytes]],
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


class BackendError(RuntimeError):
    pass


class UnsupportedOptionsError(BackendError):
    pass


def check_unknown_options(opts: Options) -> None:
    if opts:
        raise UnsupportedOptionsError(f"unsupported options passed: {opts}")
