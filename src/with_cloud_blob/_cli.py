import contextlib
import functools
import pathlib
import subprocess
import sys
import tempfile
import typing as tp
from dataclasses import dataclass

import click
import nacl.secret
import nacl.utils

from . import backend_intf
from . import backends
# import logging
# import click_log
# import jsonschema

# logger = logging.getLogger(__name__)
# click_log.basic_config(logger)


# TODO sys exit code range
# TODO extrypoints conditional on extras
# TODO call error() for click exceptions


def error(s: str) -> None:
    click.secho(f"error: {s}", err=True, fg="red")


def tempdir() -> tp.ContextManager[str]:
    return tempfile.TemporaryDirectory(prefix="with-cloud-blob-")


@dataclass
class Locator:
    backend: str
    opts: backend_intf.Options
    loc: str


def parse_locator(s: str) -> Locator:
    delim = s[:1]
    if not delim:
        raise click.BadParameter("cannot be an empty string")

    if delim == "=":
        raise click.BadParameter("cannot use equals sign as blob locator delimiter")

    fields = s.split(delim)
    if len(fields) < 3:
        raise click.BadParameter(
            "must contain at least two fields separated by delimiter defined "
            "by the first character in the string",
        )

    return Locator(
        backend=fields[1],
        loc=fields[2],
        opts={
            i[0]: (i[1:] or [""])[0]
            for i in (j.split("=", 1) for j in fields[3:])
        },
    )


def modify_blob_with_locks(
    *,
    storage: Locator,
    locks: tp.Iterable[Locator],
    modifier: backend_intf.StorageModifier,
) -> None:
    storage_backend = backends.storage_backend(storage.backend)
    lock_backends = [backends.lock_backend(lock.backend) for lock in locks]

    with contextlib.ExitStack() as es:
        for lock_backend, lock in zip(lock_backends, locks):
            es.enter_context(
                lock_backend.make_lock(
                    loc=lock.loc,
                    opts=lock.opts,
                ),
            )

        storage_backend.modify(
            loc=storage.loc,
            opts=storage.opts,
            modifier=modifier,
        )


# class PathType(click.Path):
#     def coerce_path_result(self, rv) -> pathlib.Path:  # type: ignore
#         return pathlib.Path(super().coerce_path_result(rv))


def click_wrapper(wrapper: tp.Callable[..., None], wrapped: tp.Callable[..., None]) -> tp.Callable[..., None]:
    wrapped_params = getattr(wrapped, "__click_params__", [])
    wrapper_params = getattr(wrapper, "__click_params__", [])
    result = functools.update_wrapper(wrapper, wrapped)
    result.__click_params__ = wrapped_params + wrapper_params  # type: ignore
    return result


def base_command(func: tp.Callable[..., None]) -> tp.Callable[..., None]:
    # @click_log.simple_verbosity_option(logger)  # type: ignore
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


def read_validate_blob(
    ctx: tp.Any,
    param: tp.Any,
    values: tp.Tuple[str, ...],
) -> tp.Dict[str, Locator]:
    result: tp.Dict[str, Locator] = {}

    for i in values:
        fields = i.split("=", 1)

        if len(fields) != 2:
            raise click.BadParameter("missing a required equals sign")

        if fields[0] in result:
            raise click.BadParameter(f"\"{fields[0]}\" blob name is specified multiple times")

        result[fields[0]] = parse_locator(fields[1])

    return result


@main.command()
@base_command
@click.option(
    "--allow-errors/--disallow-errors",
    help="Run command even if some blobs cannot be read.",
)
@click.option(
    "--blob",
    multiple=True,
    callback=read_validate_blob,
    metavar="<name>=<blob-locator>",
    help="Read <blob-locator> and store it as <name> in the temp "
    + "directory used as the current working directory for running the command.",
)
@click.argument("cmd", nargs=-1)
def read(**opts: tp.Any) -> None:
    """
    Read blobs and execute given command.
    Note: to pass any options to the command, prepend it with "--".
    """
    with tempdir() as td:
        tdp = pathlib.Path(td)

        errors = False

        for name, loc in opts["blob"].items():
            try:
                backend = backends.storage_backend(loc.backend)
                data = backend.load(
                    loc=loc.loc,
                    opts=loc.opts,
                )
                tdp.joinpath(name).write_bytes(data)
            except backend_intf.BackendError as e:
                error(str(e))
                errors = True

            if errors and not opts["allow_errors"]:
                sys.exit(1)

        rc = subprocess.call(
            opts["cmd"],
            cwd=td,
        )

        if rc:
            sys.exit(rc)
