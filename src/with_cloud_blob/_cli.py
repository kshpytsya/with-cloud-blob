import functools
import logging
import pathlib
import sys
import typing as tp

import click
import click_log
import nacl.secret
import nacl.utils
# import jsonschema
# import subprocess
# import tempfile

logger = logging.getLogger(__name__)
click_log.basic_config(logger)


class PathType(click.Path):
    def coerce_path_result(self, rv) -> pathlib.Path:  # type: ignore
        return pathlib.Path(super().coerce_path_result(rv))


def click_wrapper(wrapper: tp.Callable[..., None], wrapped: tp.Callable[..., None]) -> tp.Callable[..., None]:
    wrapped_params = getattr(wrapped, "__click_params__", [])
    wrapper_params = getattr(wrapper, "__click_params__", [])
    result = functools.update_wrapper(wrapper, wrapped)
    result.__click_params__ = wrapped_params + wrapper_params  # type: ignore
    return result


def base_command(func: tp.Callable[..., None]) -> tp.Callable[..., None]:
    @click_log.simple_verbosity_option(logger)  # type: ignore
    def wrapper(**opts: tp.Any) -> None:
        func(**opts)

    return click_wrapper(wrapper, func)


@click.group()
@click.version_option()
def main(**opts: tp.Any) -> None:
    """
    """


@main.command()
@base_command
def newkey(**opts: tp.Any) -> None:
    """
    Generate a new master key
    """
    key = nacl.utils.random(nacl.secret.SecretBox.KEY_SIZE)
    sys.stdout.buffer.write(nacl.encoding.HexEncoder.encode(key))
