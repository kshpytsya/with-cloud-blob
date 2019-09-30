import io
import pathlib
import typing as tp

import fastavro
import nacl
import pytest
import with_cloud_blob._crypto as cr


def test_dump_load_blob() -> None:
    cb1 = cr.CryptoBlob()
    cb1.version = 1
    cb1.max_id = 11
    cb1.xpartitions = [b"\x01", b"\x02"]
    cb1.xmaster = b"\xff\xfe"
    cb1.xtenants = {
        1: b"tenant1",
        2: b"tenant2",
    }
    blob = cb1.dump_to_blob()

    assert isinstance(blob, bytes)

    with io.BytesIO(blob) as f:
        version = fastavro.schemaless_reader(f, "int")

    assert version == 1

    cb2 = cr.CryptoBlob()
    cb2.load_from_blob(blob)

    assert cb1.max_id == cb2.max_id
    assert cb1.xpartitions == cb2.xpartitions
    assert cb1.xmaster == cb2.xmaster
    assert cb1.xtenants == cb2.xtenants


def test_compressed_avro_dump_load() -> None:
    data = [b"\x01\x02", b"\x03\x04\x05"]
    blob = cr.compressed_avro_dump(data, schema_name="partition", schema_version=1)
    assert isinstance(blob, bytes)
    loaded_data = cr.compressed_avro_load(blob, schema_name="partition", schema_version=1)
    assert loaded_data == data


def test_encrypt_descrypt() -> None:
    key = cr.new_key()

    blob = b"You may my glories and my state depose,"

    encrypted = cr.encrypt(blob, key)
    assert len(encrypted) > len(blob)
    decrypted = cr.decrypt(encrypted, key)
    assert decrypted == blob

    with pytest.raises(nacl.exceptions.CryptoError):
        cr.decrypt(encrypted + b"x", key)

    with pytest.raises(nacl.exceptions.CryptoError):
        cr.decrypt(encrypted, key[::-1])


def test_asymm_encrypt_decrypt() -> None:
    writer_key, reader_key = cr.asymm_new_keypair()

    blob = b"But not my griefs; still am I king of those."

    encrypted = cr.asymm_encrypt(blob, writer_key)
    assert len(encrypted) > len(blob)
    decrypted = cr.asymm_decrypt(encrypted, reader_key)
    assert decrypted == blob

    with pytest.raises(nacl.exceptions.CryptoError):
        cr.asymm_decrypt(encrypted + b"x", reader_key)

    with pytest.raises(nacl.exceptions.CryptoError):
        cr.asymm_decrypt(encrypted, reader_key[::-1])


@pytest.mark.parametrize(
    "files",
    [
        {},
        {"a": b""},
        {"a/b": b""},
        {"x": b"abc", "y/z": b"cde"},
        {
            "big": b"\x00" * 10000,
            "z/big2": b"\x00" * 10000,
            "big3": b"\x01" * 1000,
        },
    ],
)
def test_collect_files(
    tmpdir: tp.Any,
    files: tp.Dict[str, bytes],
) -> None:
    tmpdir = pathlib.Path(tmpdir)

    mtimes: tp.Dict[str, int] = {}
    identities: tp.Dict[bytes, int] = {}
    fidentities: tp.Dict[str, int] = {}

    for fname, fbody in files.items():
        fpath = tmpdir.joinpath(fname)
        fpath.parent.mkdir(parents=True, exist_ok=True)
        fpath.write_bytes(fbody)
        mtimes[fname] = fpath.stat().st_mtime_ns
        fidentities[fname] = identities.setdefault(fbody, len(identities))

    collected = cr.collect_files(tmpdir)

    assert collected.files.keys() == files.keys()

    bodyid2id: tp.Dict[int, int] = {}
    id2bodyid: tp.Dict[int, int] = {}

    for fname, fbody in files.items():
        f = collected.files[fname]
        assert collected.bodies[f.body_id] == fbody
        assert f.metadata.mtime_ns == mtimes[fname]

        fidentity = fidentities[fname]
        assert bodyid2id.setdefault(f.body_id, fidentity) == fidentity
        assert id2bodyid.setdefault(fidentity, f.body_id) == f.body_id


def test_partition_files_empty() -> None:
    collection = cr.FilesCollection(bodies=[], files={})
    expected_result = cr.FilesPartitions(
        partitions=[],
        used_partitions={},
        files={},
    )
    result = cr.partition_files(collection)
    assert result == expected_result


def test_partition_files() -> None:
    prefixes = ["master/", "tenants/one/", "tenants/two/"]
    collection = cr.FilesCollection(
        bodies=[
            f"body{k}_{i}".encode()
            for k in range(2)
            for i in range(1, 8)
        ],
        files={
            f"{prefix}{k}_{j}_{i}": cr.FilesCollectionItem(
                metadata=cr.FileMetadata(mtime_ns=100 * j + i),
                body_id=i - 1 + k * 7,
            )
            for k in range(2)
            for j, prefix in enumerate(prefixes)
            for i in range(1, 8)
            if (i >> j) & 1
        },
    )
    result = cr.partition_files(collection)

    keys = ["", "one", "two"]
    keys_set = set(keys)
    seen_keys: tp.Set[str] = set()

    for key, files in result.files.items():
        assert key in keys
        seen_keys.add(key)
        prefix = prefixes[keys.index(key)]

        actually_used_partitions = set()

        for fname, f in files.items():
            actually_used_partitions.add(f.partition_id)
            orig_f = collection.files[prefix + fname]
            assert f.metadata == orig_f.metadata
            assert result.partitions[f.partition_id][f.body_id] == collection.bodies[orig_f.body_id]

        assert result.used_partitions[key] == actually_used_partitions

        accessible_bodies = set()
        for partition_id in result.used_partitions[key]:
            for body in result.partitions[partition_id]:
                accessible_bodies.add(body)

        expected_accessible_bodies = {
            collection.bodies[f.body_id]
            for fname, f in collection.files.items()
            if fname.startswith(prefix)
        }

        assert accessible_bodies == expected_accessible_bodies

    assert seen_keys == keys_set


def test_writeout(
    tmpdir: tp.Any,
) -> None:
    tmpdir = pathlib.Path(tmpdir)

    partitions = [
        [
            b"",
            b"body1",
            b"body2",
        ],
        [
            b"body3",
        ],
    ]
    files = {
        "x/": {
            "empty": cr.FilesPartitionsItem(cr.FileMetadata(mtime_ns=1), partition_id=0, body_id=0),
            "a1": cr.FilesPartitionsItem(cr.FileMetadata(mtime_ns=2), partition_id=0, body_id=1),
            "d1/d2/a2": cr.FilesPartitionsItem(cr.FileMetadata(mtime_ns=3), partition_id=0, body_id=2),
            "d1/a2": cr.FilesPartitionsItem(cr.FileMetadata(mtime_ns=4), partition_id=0, body_id=2),
            "b": cr.FilesPartitionsItem(cr.FileMetadata(mtime_ns=5), partition_id=1, body_id=0),
        },
    }

    cr.writeout(partitions, files, tmpdir)

    for prefix, pfiles in files.items():
        for fname, f in pfiles.items():
            fpath = tmpdir.joinpath(prefix, fname)

            assert fpath.exists()
            assert fpath.stat().st_mtime_ns == f.metadata.mtime_ns
            assert fpath.read_bytes() == partitions[f.partition_id][f.body_id]


def test_collect_writeout(
    tmpdir: tp.Any,
) -> None:
    tmpdir = pathlib.Path(tmpdir)
    tenants = ["one", "two"]
    files: tp.Dict[str, bytes] = {
        "master/a": b"a",
        "master/x": b"x",
        "tenants/one/a": b"a",
        "tenants/one/b": b"b",
        "tenants/two/b": b"b",
    }

    collect_dir = tmpdir / "collect"

    mtimes: tp.Dict[str, int] = {}

    for fname, fbody in files.items():
        fpath = collect_dir.joinpath(fname)
        fpath.parent.mkdir(parents=True, exist_ok=True)
        fpath.write_bytes(fbody)
        mtimes[fname] = fpath.stat().st_mtime_ns

    master_key = cr.new_key()
    cb = cr.CryptoBlob()
    cb.collect(collect_dir, master_key=master_key, existing_tenants_keys=[])

    assert len(cb.xpartitions) == 3

    master_writeout_dir = tmpdir / "master"
    master_writeout_dir.mkdir()

    master_data = cb.unseal_master(master_key)

    cb.writeout_master(master_data, master_writeout_dir)

    for fname, fbody in files.items():
        fpath = master_writeout_dir.joinpath(fname)

        assert fpath.exists()
        assert fpath.stat().st_mtime_ns == mtimes[fname]
        assert fpath.read_bytes() == fbody

    tenants_keys = {i.tenant_name: i for i in cb.get_tenants_keys(master_data)}

    tenants_writeout_dir = tmpdir / "tenants"
    tenants_writeout_dir.mkdir()

    for tenant in tenants:
        tenant_writeout_dir = tenants_writeout_dir / tenant
        tenant_writeout_dir.mkdir()

        cb.writeout_tenant(
            tenant_writeout_dir,
            key_id=tenants_keys[tenant].key_id,
            tenant_key=tenants_keys[tenant].reader_key,
        )

        for fname, fbody in files.items():
            if not fname.startswith(f"tenants/{tenant}/"):
                continue

            fpath = tmpdir.joinpath(fname)

            assert fpath.exists()
            assert fpath.stat().st_mtime_ns == mtimes[fname]
            assert fpath.read_bytes() == fbody
