import contextlib
import functools
import pathlib
import subprocess
import sys
import tempfile
import time
import typing as tp
from dataclasses import dataclass

import click
import click_log
import nacl.secret
import nacl.utils

from . import backend_intf
from . import backends
from ._log import logger
# import jsonschema

# TODO sys exit code range
# TODO extrypoints conditional on extras
# TODO proper errors on str to int/float casts
# TODO test: time before lock timeout exception is approximately equal to requested timeout


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
            "must contain at least two fields separated by a delimiter defined "
            "by the first character in the string",
        )

    return Locator(
        backend=fields[1],
        loc=fields[2],
        opts=backend_intf.Options({
            i[0]: (i[1:] or [""])[0]
            for i in (j.split("=", 1) for j in fields[3:])
        }),
    )


def short_locator_descr(loc: Locator) -> str:
    for delim in ":~!@#$%^&*()_-+=[{}]\\|;'\"<>,.?/":
        if delim not in loc.backend and delim not in loc.loc:
            return f"{delim}{loc.backend}{delim}{loc.loc}"

    return f"{loc.backend} {loc.loc}"


def modify_blob_with_locks(
    *,
    storage: Locator,
    locks: tp.Iterable[Locator],
    modifier: backend_intf.StorageModifier,
    first_timeout: float,
    timeout_step: float,
) -> None:
    try:
        storage_backend = backends.storage_backend(storage.backend)
        lock_backends = [backends.lock_backend(lock.backend) for lock in locks]
    except backend_intf.BackendError as e:
        raise click.ClickException(str(e))

    with contextlib.ExitStack() as es:
        for lock_backend, lock in zip(lock_backends, locks):
            loc_prefix = lock.opts.get("prefix") or ""
            total_timeout = float(lock.opts.get("timeout") or "0")
            deadline = time.time() + total_timeout

            lock.loc = loc_prefix + (lock.loc or storage.loc)
            lock_descr = short_locator_descr(lock)

            attempt = 0

            while True:
                remaining = deadline - time.time()

                if attempt > 0 and remaining < 0:
                    raise click.ClickException(f"timed out waiting for {lock_descr}")
                else:
                    try:
                        if attempt > 0:
                            logger.info(lambda: f"waiting for lock {lock_descr}, deadline in {remaining:1.1f}s")

                        attempt += 1

                        es.enter_context(
                            lock_backend.make_lock(
                                loc=lock.loc,
                                opts=lock.opts,
                                timeout=max(0, min(remaining, timeout_step if attempt > 1 else first_timeout)),
                            ),
                        )
                    except backend_intf.TimeoutError:
                        continue
                    except backend_intf.BackendError as e:
                        raise click.ClickException(f"{lock_descr}: {e}")

                break

        try:
            storage_backend.modify(
                loc=storage.loc,
                opts=storage.opts,
                modifier=modifier,
            )
        except backend_intf.BackendError as e:
            raise click.ClickException(f"{short_locator_descr(storage)}: {e}")


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
    @click_log.simple_verbosity_option(logger, show_default=True)  # type: ignore
    def wrapper(**opts: tp.Any) -> None:
        func(**opts)

    return click_wrapper(wrapper, func)


def main() -> None:
    try:
        root(standalone_mode=False)
    except click.ClickException as e:
        logger.error(e.format_message())
        sys.exit(1)
    except click.exceptions.Abort:
        logger.error("aborted")
        sys.exit(1)


@click.group()
@click.version_option()
def root(**opts: tp.Any) -> None:
    """
    """


@root.command(name="newkey")
@base_command
def cmd_newkey(**opts: tp.Any) -> None:
    """
    Generate a new master key.
    """
    key = nacl.utils.random(nacl.secret.SecretBox.KEY_SIZE)
    sys.stdout.buffer.write(nacl.encoding.HexEncoder.encode(key))


def modify_validate_lock(
    ctx: tp.Any,
    param: tp.Any,
    values: tp.Tuple[str, ...],
) -> tp.List[Locator]:
    return [parse_locator(i) for i in values]


def modify_validate_blob(
    ctx: tp.Any,
    param: tp.Any,
    value: str,
) -> Locator:
    return parse_locator(value)


@root.command(name="modify")
@base_command
@click.option(
    "--lock",
    multiple=True,
    callback=modify_validate_lock,
    metavar="<lock-locator>",
    help="Locator of a lock to hold while executing <command>. May be specified multiple times.",
)
@click.option(
    "--first-timeout",
    default=10.0,
    metavar="<T>",
    help="Time in seconds to silently wait for each lock before starting to periodic reports.",
    show_default=True,
)
@click.option(
    "--timeout-step",
    default=30.0,
    metavar="<T>",
    help="Time in seconds between periodic reports about waiting for lock acquisition.",
    show_default=True,
)
@click.argument(
    "blob",
    callback=modify_validate_blob,
)
@click.argument("command", nargs=1)
@click.argument("command_args", metavar="[ARGS]...", nargs=-1)
def cmd_modify(**opts: tp.Any) -> None:
    """
    Modify blob.

    Read <blob>, execute given <command>, and, in case of success, update <blob>.
    Note: to pass any options to the command, prepend it with "--".
    If <blob> exists, its contents will be accessible as a file named "blob"
    in the temp directory used as the current working directory for running
    the command. If command deletes the file, <blob> will be deleted.
    """

    def modifier(blob: tp.Optional[bytes]) -> tp.Optional[bytes]:
        with tempdir() as td:
            tdp = pathlib.Path(td)
            blob_path = tdp / "blob"

            if blob is not None:
                blob_path.write_bytes(blob)

            try:
                rc = subprocess.call(
                    [opts["command"]] + list(opts["command_args"]),
                    cwd=td,
                )
            except Exception as e:
                raise click.ClickException(str(e))

            if rc:
                sys.exit(rc)

            if blob_path.exists():
                return blob_path.read_bytes()
            else:
                return None

    modify_blob_with_locks(
        storage=opts["blob"],
        locks=opts["lock"],
        modifier=modifier,
        first_timeout=opts["first_timeout"],
        timeout_step=opts["timeout_step"],
    )


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


@root.command(name="read")
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
    + "directory used as the current working directory for running the command. "
    + "May be specified multiple times.",
)
@click.argument("command", nargs=1)
@click.argument("command_args", metavar="[ARGS]...", nargs=-1)
def cmd_read(**opts: tp.Any) -> None:
    """
    Read blobs and execute given <command>.
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
                logger.error(f"{short_locator_descr(loc)}: {e}")
                errors = True

            if errors and not opts["allow_errors"]:
                sys.exit(1)

        try:
            rc = subprocess.call(
                [opts["command"]] + list(opts["command_args"]),
                cwd=td,
            )
        except Exception as e:
            raise click.ClickException(str(e))

        if rc:
            sys.exit(rc)


@root.group(name="backends")
def cmd_backends(**opts: tp.Any) -> None:
    """
    Provide info about available backends.
    """


@cmd_backends.command(name="list")
def cmd_backends_list(**opts: tp.Any) -> None:
    """
    List available backends.
    """
    for i, j in sorted(backends.list_backends().items()):
        for k in sorted(j):
            click.echo(f"{i}: {k}")


@cmd_backends.command(name="info")
def cmd_backends_info(**opts: tp.Any) -> None:
    """
    """
