import logging as _logging
import typing as _tp

import click_log as _click_log


class _LazyLogger(_logging.Logger):
    def _log(
        self,
        level: int,
        msg: str,
        args: _tp.Tuple[_tp.Any, ...],
        **kwargs: _tp.Any
    ) -> None:
        def maybe_callable(x: _tp.Any) -> _tp.Any:
            return x() if callable(x) else x

        _logging.Logger._log(  # type: ignore
            self,
            level,
            maybe_callable(msg),
            tuple(maybe_callable(i) for i in args),
            **kwargs
        )


_logging.setLoggerClass(_LazyLogger)
logger = _logging.getLogger(__name__)
_click_log.basic_config(logger)
