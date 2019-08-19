import pathlib
import typing as tp

import click
import nacl.secret
import nacl.utils
import pytest
from with_cloud_blob._cli import main


def cli(args: tp.List[str]) -> None:
    main(args, standalone_mode=False)


def test_empty() -> None:
    cli([])


def test_newkey(capfd: tp.Any) -> None:
    cli(["newkey"])
    captured = capfd.readouterr()
    assert captured.err == ""
    assert len(nacl.encoding.HexEncoder.decode(captured.out)) == nacl.secret.SecretBox.KEY_SIZE


@pytest.mark.parametrize(
    "args,cmd,expected_rc,expected_out",
    [
        ([], ["true"], 0, ""),
        ([], ["false"], 1, ""),
        (["--blob=a=:file:/dev/null"], ["cat", "a"], 0, ""),
        (
            ["--blob=a=:file:/dev/null", "--blob=b=:file:/dev/null"],
            ["cat", "a", "b"],
            0,
            "",
        ),
        (["--blob=a=:file:/"], ["cat", "a"], 1, ""),
        (["--blob=a=:file:/"], ["true"], 1, ""),
        (["--blob=a=:file:/", "--allow-errors"], ["true"], 0, ""),
        (["*abcxyz*ABC"], ["cat", "abcxyz"], 0, "ABC"),
    ],
)
def test_read(
    tmp_path: pathlib.Path,
    capfd: tp.Any,
    args: tp.List[str],
    cmd: tp.List[str],
    expected_rc: int,
    expected_out: str,
) -> None:
    def maybe_make_blob(s: str) -> str:
        f = s.split("*", 2)
        if not s or f[:1][0]:
            return s

        path = tmp_path.joinpath(f[1])
        path.write_text(f[2])

        return f"--blob={f[1]}=:file:{path}"

    try:
        cli(
            ["read"]
            + [maybe_make_blob(i) for i in args]
            + ["--"]
            + cmd,
        )
    except SystemExit as e:
        assert e.code == expected_rc

    captured = capfd.readouterr()
    assert captured.out == expected_out


def test_read_param_exceptions() -> None:
    with pytest.raises(click.BadParameter):
        cli(["read", "--blob="])

    with pytest.raises(click.BadParameter):
        cli(["read", "--blob=a=:x:y", "--blob=a=:x:y"])

    with pytest.raises(click.BadParameter):
        cli(["read", "--blob=a=x"])

    with pytest.raises(click.BadParameter):
        cli(["read", "--blob=a="])

    with pytest.raises(click.BadParameter):
        cli(["read", "--blob=a=="])
