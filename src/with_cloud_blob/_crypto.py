import enum
import functools
import io
import json
import lzma
import os
import pathlib
import struct
import typing as tp
from dataclasses import dataclass

import fastavro
import nacl.encoding
import nacl.public
import nacl.secret
import nacl.signing
import nacl.utils


__spec__: tp.Any


@functools.lru_cache()
def schemas() -> tp.Any:
    with __spec__.loader.open_resource("crypto_schemas.json") as f:
        data = json.load(f)

    def stitch(
        x: tp.Any,
        *,
        name: tp.Optional[str] = None
    ) -> tp.Any:
        if isinstance(x, str) and x.startswith("*"):
            return stitch(data[x[1:]])
        elif isinstance(x, list):
            return [stitch(i) for i in x]
        elif isinstance(x, dict):
            if name is not None:
                x.setdefault("name", name)

            tp = x.get("type")
            if tp == "record":
                x["fields"] = [dict(i, type=stitch(i["type"])) for i in x["fields"]]
            elif tp == "array":
                x["items"] = stitch(x["items"])
            elif tp == "map":
                x["values"] = stitch(x["values"])

        return x

    data = {
        k: stitch(v, name=k)
        for k, v in data.items()
    }

    return {
        k: fastavro.parse_schema(v)
        for k, v in data.items()
    }


@functools.lru_cache()
def supported_schema_versions(name: str) -> tp.List[str]:
    return sorted(j for i in schemas() for i, j in [strip_prefix(i, [f"{name}."])] if i == 0)


@functools.lru_cache()
def schema(name: str, version: int) -> tp.Any:
    result = schemas().get(f"{name}.{version}")

    if result is None:
        raise Error(
            "Unsupported {} version {}. Supported versions are {}.".format(
                name,
                version,
                ", ".join(supported_schema_versions(name)),
            ),
        )

    return result


class Error(Exception):
    pass


def new_key() -> bytes:
    return tp.cast(bytes, nacl.utils.random(nacl.secret.SecretBox.KEY_SIZE))


def encrypt(blob: bytes, key: bytes) -> bytes:
    assert len(key) == nacl.secret.SecretBox.KEY_SIZE
    return bytes(nacl.secret.SecretBox(key).encrypt(blob))


def decrypt(blob: bytes, key: bytes) -> bytes:
    assert len(key) == nacl.secret.SecretBox.KEY_SIZE
    return tp.cast(bytes, nacl.secret.SecretBox(key).decrypt(blob))


# https://pynacl.readthedocs.io/en/stable/signing/#nacl.signing.SigningKey
SIGNING_KEY_SIZE = 32
ASYMM_HEADER_STRUCT = struct.Struct("!H")
ASYMM_WRITER_KEYS_STRUCT = struct.Struct(f"{nacl.public.PublicKey.SIZE}s{SIGNING_KEY_SIZE}s")
ASYMM_READER_KEYS_STRUCT = struct.Struct(f"{nacl.public.PrivateKey.SIZE}s{SIGNING_KEY_SIZE}s")


def asymm_new_keypair() -> tp.Tuple[bytes, bytes]:
    reader_key = nacl.public.PrivateKey.generate()
    signing_key = nacl.signing.SigningKey.generate()

    return (
        ASYMM_WRITER_KEYS_STRUCT.pack(reader_key.public_key.encode(), signing_key.encode()),
        ASYMM_READER_KEYS_STRUCT.pack(reader_key.encode(), signing_key.verify_key.encode()),
    )


def asymm_encrypt(blob: bytes, writer_key: bytes) -> bytes:
    reader_public_key_bytes, signing_key_bytes = ASYMM_WRITER_KEYS_STRUCT.unpack(writer_key)
    reader_public_key = nacl.public.PublicKey(reader_public_key_bytes)
    signing_key = nacl.signing.SigningKey(signing_key_bytes)
    asymm_box = nacl.public.SealedBox(reader_public_key)
    ephemeral_key = new_key()
    encrypted_ephemeral_key = asymm_box.encrypt(ephemeral_key)
    box = nacl.secret.SecretBox(ephemeral_key)
    encrypted_signed_blob = box.encrypt(signing_key.sign(blob))

    return b"".join((
        ASYMM_HEADER_STRUCT.pack(len(encrypted_ephemeral_key)),
        encrypted_ephemeral_key,
        encrypted_signed_blob,
    ))


def asymm_decrypt(blob: bytes, reader_key: bytes) -> bytes:
    encrypted_ephemeral_key_size, = ASYMM_HEADER_STRUCT.unpack_from(blob)
    encrypted_ephemeral_key = blob[ASYMM_HEADER_STRUCT.size:ASYMM_HEADER_STRUCT.size + encrypted_ephemeral_key_size]
    encrypted_signed_blob = blob[ASYMM_HEADER_STRUCT.size + encrypted_ephemeral_key_size:]
    reader_private_key_bytes, verify_key_bytes = ASYMM_READER_KEYS_STRUCT.unpack(reader_key)
    reader_private_key = nacl.public.PrivateKey(reader_private_key_bytes)
    verify_key = nacl.signing.VerifyKey(verify_key_bytes)
    asymm_box = nacl.public.SealedBox(reader_private_key)
    ephemeral_key = asymm_box.decrypt(encrypted_ephemeral_key)
    box = nacl.secret.SecretBox(ephemeral_key)

    return tp.cast(bytes, verify_key.verify(box.decrypt(encrypted_signed_blob)))


def compressed_avro_dump(
    data: tp.Any,
    *,
    schema_name: str,
    schema_version: int,
) -> bytes:
    with io.BytesIO() as f:
        fastavro.schemaless_writer(f, schema(schema_name, schema_version), data)
        blob = f.getvalue()
    return lzma.compress(blob, format=lzma.FORMAT_RAW, filters=[dict(id=lzma.FILTER_LZMA2, preset=5)])


def compressed_avro_load(
    blob: bytes,
    *,
    schema_name: str,
    schema_version: int,
) -> tp.Any:
    blob = lzma.decompress(blob, format=lzma.FORMAT_RAW, filters=[dict(id=lzma.FILTER_LZMA2)])

    with io.BytesIO(blob) as f:
        data = fastavro.schemaless_reader(f, schema(schema_name, schema_version))

    return data


class FileMetadataFlag(enum.IntFlag):
    SYMLINK = 1
    SYMLINK_ABS = 2


@dataclass
class FileMetadata:
    mtime_ns: int
    flags: int


@dataclass
class FilesCollectionItem:
    metadata: FileMetadata
    body_id: int


class FilesCollection:
    bodies: tp.List[bytes]
    _bodies_dict: tp.Dict[bytes, int]
    files: tp.Dict[str, FilesCollectionItem]

    def __init__(self) -> None:
        self.bodies = []
        self.files = {}
        self._bodies_dict = {}

    def add_body(self, body: bytes) -> int:
        result = self._bodies_dict.get(body)

        if result is None:
            result = len(self.bodies)
            self.bodies.append(body)
            self._bodies_dict[body] = result

        return result


def collect_files(src: pathlib.Path) -> FilesCollection:
    result = FilesCollection()

    for i in src.rglob("*"):
        is_symlink = i.is_symlink()
        if is_symlink or i.is_file():
            flags = 0

            if is_symlink:
                flags |= FileMetadataFlag.SYMLINK
                target = os.readlink(i)
                target_path = pathlib.Path(target)

                if target_path.is_absolute():
                    flags |= FileMetadataFlag.SYMLINK_ABS
                    resolved_target = target_path.resolve()
                    try:
                        target = str(resolved_target.relative_to(src))
                    except ValueError:
                        raise Error(
                            f"\"{i}\" absolute symlink points to \"{target}\" which is outside \"{src}\"",
                        )

                body = target.encode()
                mtime_ns = i.lstat().st_mtime_ns
            else:
                body = i.read_bytes()
                mtime_ns = i.stat().st_mtime_ns

            result.files[str(i.relative_to(src))] = FilesCollectionItem(
                metadata=FileMetadata(
                    mtime_ns=mtime_ns,
                    flags=flags,
                ),
                body_id=result.add_body(body),
            )
        elif i.is_dir():
            continue
        else:
            raise Error(f"don't know how to deal with \"{i}\"")

    return result


@dataclass
class FilesPartitionsItem:
    metadata: FileMetadata
    partition_id: int
    body_id: int

    def to_data(self) -> tp.Any:
        return {
            "mtime_ns": self.metadata.mtime_ns,
            "flags": self.metadata.flags,
            "partition_id": self.partition_id,
            "body_id": self.body_id,
        }

    @staticmethod
    def from_data(data: tp.Any, version: int) -> "FilesPartitionsItem":
        if version == 1:
            return FilesPartitionsItem(
                FileMetadata(
                    mtime_ns=data["mtime_ns"],
                    flags=data["flags"],
                ),
                partition_id=data["partition_id"],
                body_id=data["body_id"],
            )

        assert 0


@dataclass
class FilesPartitions:
    partitions: tp.List[tp.List[bytes]]
    used_partitions: tp.Dict[str, tp.Set[int]]
    files: tp.Dict[str, tp.Dict[str, FilesPartitionsItem]]


def strip_prefix(s: str, prefixes: tp.Iterable[str]) -> tp.Tuple[int, str]:
    for i, prefix in enumerate(prefixes):
        if s.startswith(prefix):
            return i, s[len(prefix):]

    return -1, s


def partition_files(collection: FilesCollection) -> FilesPartitions:
    keys_by_body_id: tp.Dict[int, tp.Set[str]] = {}

    @dataclass
    class File:
        key: str
        name: str
        f: FilesCollectionItem
        body_id: int

    files: tp.List[File] = []

    for fname, f in sorted(collection.files.items()):
        prefix, tail = strip_prefix(fname, ["master/", "tenants/"])

        key = None
        body_id = f.body_id

        if prefix == 0:
            key = ""
            fname = tail
        elif prefix == 1:
            fields = tail.split("/", 1)
            if len(fields) == 2:
                key, fname = fields

        if key is None:
            raise Error(f"don't know how to deal with \"{fname}\"")

        if f.metadata.flags & FileMetadataFlag.SYMLINK:
            def validate_symlink(body_id: int) -> int:
                target = collection.bodies[body_id].decode()
                target_parts = target.split("/")

                if key:
                    required_prefix = ["tenants", key]
                else:
                    required_prefix = ["master"]

                required_prefix_str = "/".join(required_prefix)

                if f.metadata.flags & FileMetadataFlag.SYMLINK_ABS:
                    if target_parts[:len(required_prefix)] != required_prefix:
                        raise Error(
                            f"\"{fname}\" absolute symlink points to \"{target}\" "
                            f"which is outside \"{required_prefix_str}\"",
                        )
                    body_id = collection.add_body("/".join(target_parts[len(required_prefix):]).encode())
                else:
                    cur_dir = fname.split("/")
                    for part in target_parts:
                        if part == ".":
                            pass
                        elif part == "..":
                            if cur_dir:
                                cur_dir.pop()
                            else:
                                raise Error(
                                    f"\"{fname}\" symlink points to \"{target}\" "
                                    f"which is outside \"{required_prefix_str}\"",
                                )
                        else:
                            cur_dir.append(part)

                return body_id

            body_id = validate_symlink(body_id)

        keys_by_body_id.setdefault(body_id, set()).add(key)
        files.append(
            File(
                key=key,
                name=fname,
                f=f,
                body_id=body_id,
            ),
        )

    keyset_to_partition: tp.Dict[tp.FrozenSet[str], int] = {}

    result = FilesPartitions(
        partitions=[],
        used_partitions={},
        files={},
    )

    body_id_to_partition_body_id: tp.Dict[int, tp.Tuple[int, int]] = {}

    for body_id, keyset in sorted(keys_by_body_id.items()):
        partition_id = keyset_to_partition.setdefault(frozenset(keyset), len(keyset_to_partition))
        if partition_id == len(result.partitions):
            result.partitions.append([])

        body_id_to_partition_body_id[body_id] = partition_id, len(result.partitions[partition_id])
        result.partitions[partition_id].append(collection.bodies[body_id])

        for key in keyset:
            result.used_partitions.setdefault(key, set()).add(partition_id)

    for i in files:
        partition_id, body_id = body_id_to_partition_body_id[i.body_id]
        result.files.setdefault(i.key, {})[i.name] = FilesPartitionsItem(
            metadata=i.f.metadata,
            partition_id=partition_id,
            body_id=body_id,
        )

    return result


def writeout(
    partitions: tp.Union[tp.List[tp.List[bytes]], tp.Mapping[int, tp.List[bytes]]],
    files: tp.Dict[str, tp.Dict[str, FilesPartitionsItem]],
    dest: pathlib.Path,
) -> None:
    created_dirs: tp.Set[tp.Tuple[str, ...]] = set()

    for prefix, pfiles in files.items():
        for fname, f in pfiles.items():
            fname_components = (prefix + fname).split("/")

            dest_dir = dest.joinpath(*fname_components[:-1])

            if tuple(fname_components[:-1]) not in created_dirs:
                dest_dir.mkdir(parents=True, exist_ok=True)
                for i in range(1, len(fname_components)):
                    created_dirs.add(tuple(fname_components[:i]))

            dest_path = dest_dir / fname_components[-1]

            body = partitions[f.partition_id][f.body_id]

            if f.metadata.flags & FileMetadataFlag.SYMLINK:
                body_s = body.decode()
                if f.metadata.flags & FileMetadataFlag.SYMLINK_ABS:
                    dest_path.symlink_to(dest / (prefix + body_s))
                else:
                    dest_path.symlink_to(body_s)

                # TODO lutime
            else:
                dest_path.write_bytes(body)

                os.utime(dest_path, ns=(f.metadata.mtime_ns, f.metadata.mtime_ns))


@dataclass
class TenantKeys:
    tenant_name: str
    key_id: int
    writer_key: bytes
    reader_key: bytes

    def to_data(self) -> tp.Any:
        return {
            "tenant_name": self.tenant_name,
            "key_id": self.key_id,
            "writer_key": self.writer_key,
            "reader_key": self.reader_key,
        }

    @staticmethod
    def from_data(data: tp.Any, version: int) -> "TenantKeys":
        if version == 1:
            return TenantKeys(
                tenant_name=data["tenant_name"],
                key_id=data["key_id"],
                writer_key=data["writer_key"],
                reader_key=data["reader_key"],
            )

        assert 0


class CryptoBlob:
    version: int
    max_id: int
    xpartitions: tp.List[bytes]
    xmaster: bytes
    xtenants: tp.Dict[int, bytes]

    def __init__(self) -> None:
        self.version = -1
        self.max_id = 0
        self.xpartitions = []
        self.xmaster = b""
        self.xtenants = {}

    def load_from_blob(self, blob: bytes) -> None:
        with io.BytesIO(blob) as f:
            self.version = fastavro.schemaless_reader(f, schemas()["blob_header"])
            data = fastavro.schemaless_reader(f, schema("blob", self.version))

        if self.version == 1:
            self.max_id = data["max_id"]
            self.xmaster = data["master"]
            self.xpartitions = data["partitions"]
            self.xtenants = {int(k): v for k, v in data["tenants"].items()}
        else:
            assert 0

    def dump_to_blob(self) -> bytes:
        data = {
            "max_id": self.max_id,
            "master": self.xmaster,
            "partitions": self.xpartitions,
            "tenants": {str(k): v for k, v in self.xtenants.items()},
        }

        with io.BytesIO() as f:
            fastavro.schemaless_writer(f, schemas()["blob_header"], self.version)
            fastavro.schemaless_writer(f, schema("blob", self.version), data)
            return f.getvalue()

    def collect(
        self,
        src: pathlib.Path,
        *,
        master_key: bytes,
        existing_tenants_keys: tp.Iterable[TenantKeys],
    ) -> None:
        self.version = 1

        collection = collect_files(src)
        partitioned = partition_files(collection)
        partition_keys = [new_key() for i in partitioned.partitions]
        tenants_keys_by_names = {i.tenant_name: i for i in existing_tenants_keys}

        for i in partitioned.files:
            if not i:
                continue

            if i not in tenants_keys_by_names:
                self.max_id += 1
                writer_key, reader_key = asymm_new_keypair()
                tenants_keys_by_names[i] = TenantKeys(
                    tenant_name=i,
                    key_id=self.max_id,
                    writer_key=writer_key,
                    reader_key=reader_key,
                )

        self.xpartitions = [
            encrypt(
                compressed_avro_dump(partition, schema_name="partition", schema_version=self.version),
                key,
            )
            for key, partition in zip(partition_keys, partitioned.partitions)
        ]

        files_data = {
            k: {k2: v2.to_data() for k2, v2 in v.items()}
            for k, v in partitioned.files.items()
        }

        master_data = {
            "partition_keys": partition_keys,
            "files": files_data,
            "tenants_keys": [tenants_keys_by_names[i].to_data() for i in partitioned.files if i],
        }

        self.xmaster = encrypt(
            compressed_avro_dump(master_data, schema_name="master", schema_version=self.version),
            master_key,
        )

        self.xtenants = {
            tenants_keys_by_names[tenant_name].key_id: asymm_encrypt(
                compressed_avro_dump(
                    {
                        "partition_keys": [
                            partition_key if partition_i in partitioned.used_partitions[tenant_name] else b""
                            for partition_i, partition_key in enumerate(partition_keys)
                        ],
                        "files": tenant_files_data,
                    },
                    schema_name="tenant",
                    schema_version=self.version,
                ),
                tenants_keys_by_names[tenant_name].writer_key,
            )
            for tenant_name, tenant_files_data in files_data.items() if tenant_name
        }

    def unseal_master(self, master_key: bytes) -> tp.Any:
        return compressed_avro_load(
            decrypt(self.xmaster, master_key),
            schema_name="master",
            schema_version=self.version,
        )

    def writeout_master(
        self,
        master_data: tp.Any,
        dest: pathlib.Path,
    ) -> None:
        partitions = [
            compressed_avro_load(
                decrypt(partition_data, partition_key),
                schema_name="partition",
                schema_version=self.version,
            )
            for partition_key, partition_data in zip(master_data["partition_keys"], self.xpartitions)
        ]
        if self.version == 1:
            files = {
                (f"tenants/{k}/" if k else "master/"): {
                    k2: FilesPartitionsItem.from_data(v2, self.version)
                    for k2, v2 in v.items()
                }
                for k, v in master_data["files"].items()
            }

            writeout(partitions, files, dest)

    def get_tenants_keys(
        self,
        master_data: tp.Any,
    ) -> tp.List[TenantKeys]:
        if self.version == 1:
            return [TenantKeys.from_data(i, 1) for i in master_data["tenants_keys"]]
        else:
            assert 0

    def writeout_tenant(
        self,
        dest: pathlib.Path,
        *,
        key_id: int,
        tenant_key: bytes,
    ) -> None:
        tenant_data = compressed_avro_load(
            asymm_decrypt(self.xtenants[key_id], tenant_key),
            schema_name="tenant",
            schema_version=self.version,
        )
        partitions = [
            compressed_avro_load(
                decrypt(partition_data, partition_key),
                schema_name="partition",
                schema_version=self.version,
            ) if partition_key else []
            for partition_key, partition_data in zip(tenant_data["partition_keys"], self.xpartitions)
        ]
        if self.version == 1:
            files = {
                '': {
                    k: FilesPartitionsItem.from_data(v, self.version)
                    for k, v in tenant_data["files"].items()
                },
            }

            writeout(partitions, files, dest)
