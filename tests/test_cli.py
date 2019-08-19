import typing as tp

import nacl.secret
import nacl.utils
from with_cloud_blob._cli import main
# import pytest
# from contextlib import contextmanager


def cli(args: tp.List[str]) -> None:
    main(args, standalone_mode=False)


def test_empty() -> None:
    cli([])


def test_newkey(capfd: tp.Any) -> None:
    cli(["newkey"])
    captured = capfd.readouterr()
    assert captured.err == ""
    assert len(nacl.encoding.HexEncoder.decode(captured.out)) == nacl.secret.SecretBox.KEY_SIZE
