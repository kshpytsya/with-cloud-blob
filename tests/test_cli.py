import concurrent.futures
import pathlib
import typing as tp

import click
import common
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
        (["*alpha*ONE", "*beta*TWO"], ["cat", "alpha", "beta"], 0, "ONETWO"),
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


def test_backends_list(
    capfd: tp.Any,
) -> None:
    cli(["backends", "list"])
    captured = capfd.readouterr()
    assert captured.err == ""
    lines = captured.out.splitlines()
    assert "lock: file" in lines
    assert "storage: file" in lines


def _test_parallel_modify(
    *,
    tmp_path: pathlib.Path,
    count: int,
    jobs: int,
    args: tp.List[str],
) -> None:
    args = ["modify"] + args + ["--", "bash", "-c"]

    def f(i: int) -> None:
        cli(args + [f"echo {i} >> blob"])

    if jobs > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=jobs) as tpe:
            for i in range(count):
                tpe.submit(f, i)
    else:
        for i in range(count):
            f(i)

    result_file = tmp_path / "result"

    cli(args + [f"cat blob >> {result_file}"])

    result = set(result_file.read_text().splitlines())

    assert result == {str(i) for i in range(count)}


@pytest.mark.parametrize(
    "count,jobs,args",
    [
        (10, 1, ["--lock", ":file:@f", ":file:@f"]),
        (100, 10, ["--lock", ":file:@f", ":file:@f"]),
    ],
)
def test_parallel_modify_file(
    tmp_path: pathlib.Path,
    count: int,
    jobs: int,
    args: tp.List[str],
) -> None:
    file1 = tmp_path / "file1"

    _test_parallel_modify(
        tmp_path=tmp_path,
        count=count,
        jobs=jobs,
        args=[j.replace("@f", str(file1)) for j in args],
    )


@pytest.mark.parametrize(
    "count,jobs,lock,delay_put",
    [
        (10, 1, "file", 0),
        (50, 5, "file", 0),
        (10, 1, "file", 0.05),
        (20, 5, "file", 0.05),
        (10, 1, "dynamodb", 0.1),
        (20, 5, "dynamodb", 0.1),
    ],
)
def test_parallel_modify_s3_dynamodb(
    s3_bucket: tp.Any,
    tmp_path: pathlib.Path,
    count: int,
    jobs: int,
    lock: str,
    delay_put: float,
) -> None:
    if lock == "file":
        file1 = tmp_path / "file1"
        lock_loc = f"|file|{file1}"
    elif lock == "dynamodb":
        lock_loc = f"|dynamodb|{s3_bucket.name}/file1|dynamodb_endpoint={common.DYNAMODB_ENDPOINT}|region=us-east-1"
    else:
        assert 0

    s3_opts = common.s3_modify_options_dict(delay_put=delay_put)
    # s3_opts["max_lag"] = "0"
    # s3_opts.pop("dynamodb_endpoint", None)
    # s3_opts.pop("dynamodb_table", None)

    s3_loc = f"|s3|{s3_bucket.name}/file1" + "".join(f"|{k}={v}" for k, v in s3_opts.items())

    _test_parallel_modify(
        tmp_path=tmp_path,
        count=count,
        jobs=jobs,
        args=["--lock", lock_loc, s3_loc],
    )
