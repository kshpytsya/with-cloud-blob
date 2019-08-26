import hashlib
import io
import threading
import time
import typing as tp

import botocore
import implements
import with_cloud_blob.backend_intf as intf

from . import _boto_helpers as bh


class _BucketKey:
    bucket: str
    key: str

    def __init__(self, loc: str) -> None:
        try:
            self.bucket, self.key = loc.split("/", 1)
        except ValueError:
            raise intf.BackendError(
                "s3 location must contain at least one slash separating bucket and key names",
            )


class _DynamoDbKeyValueStore:
    def __init__(
        self,
        boto_session: tp.Any,
        opts: intf.Options,
        ttl: int,
    ) -> None:
        self._table_name = opts.get("dynamodb_table") or "with-cloud-blob"
        self._resource = bh.boto_resource_dynamodb(boto_session, opts)
        self._table: tp.Any = None
        self._ttl = ttl

    def _ensure_table(self) -> tp.Any:
        if self._table:
            return

        try:
            self._resource.create_table(
                AttributeDefinitions=[
                    {
                        "AttributeName": "key",
                        "AttributeType": "S",
                    },
                ],
                KeySchema=[
                    {
                        "AttributeName": "key",
                        "KeyType": "HASH",
                    },
                ],
                BillingMode="PAY_PER_REQUEST",
                TableName=self._table_name,
            )

            self._wait_for_table_active()

            self._resource.meta.client.update_time_to_live(
                TableName=self._table_name,
                TimeToLiveSpecification={
                    "Enabled": True,
                    "AttributeName": "expiry_time",
                },
            )

            self._wait_for_table_active()
        except self._resource.meta.client.exceptions.ResourceInUseException:
            # table already exists
            pass

        self._table = self._resource.Table(self._table_name)

    def _wait_for_table_active(self) -> None:
        while True:
            response = self._resource.meta.client.describe_table(TableName=self._table_name)
            status = response.get("Table", {}).get("TableStatus", "UNKNOWN")
            if status == "ACTIVE":
                break
            else:
                time.sleep(1)

    def get(self, key: str) -> tp.Optional[str]:
        self._ensure_table()
        response = self._table.get_item(
            Key={"key": key},
            ConsistentRead=True,
        )
        item = response.get("Item")
        if item and item["expiry_time"] > time.time():
            return tp.cast(str, item["value"].value)
        else:
            return None

    def put(self, key: str, value: bytes) -> None:
        self._ensure_table()
        self._table.update_item(
            TableName=self._table_name,
            Key={"key": key},
            UpdateExpression="SET #value = :value, #expiry_time = :expiry_time",
            ExpressionAttributeNames={
                "#value": "value",
                "#expiry_time": "expiry_time",
            },
            ExpressionAttributeValues={
                ":value": value,
                ":expiry_time": int(time.time() + self._ttl),
            },
        )


def _digest(data: tp.Optional[bytes]) -> bytes:
    if data is None:
        return b"*"

    return hashlib.sha1(data).digest()


@implements.implements(intf.IStorageBackend)
class Backend:
    @staticmethod
    def modify(
        *,
        loc: str,
        modifier: intf.StorageModifier,
        opts: intf.Options,
    ) -> None:
        session = bh.boto_session(opts)
        s3 = bh.boto_resource_s3(session, opts)

        # delay_put is used simulate eventual consistency of S3 in tests
        delay_put = float(opts.get("_delay_put") or "0")
        max_lag = int(opts.get("max_lag") or "30")

        if max_lag:
            dynamo = _DynamoDbKeyValueStore(session, opts, ttl=max_lag)
            expected_digest = dynamo.get(loc)
            if expected_digest is None:
                attempts = 1
            else:
                attempts = max_lag
        else:
            attempts = 1

        bk = _BucketKey(loc)
        opts.fail_on_unused()

        bucket = s3.Bucket(bk.bucket)

        for attempt in range(attempts):
            with io.BytesIO() as f:
                try:
                    bucket.download_fileobj(bk.key, f)
                    data: tp.Optional[bytes] = f.getvalue()
                except botocore.exceptions.ClientError as e:
                    try:
                        s3.meta.client.head_bucket(Bucket=bk.bucket)
                    except botocore.exceptions.ClientError as e2:
                        raise intf.BackendError(f"accessing bucket: {e2}")

                    if e.response["Error"]["Code"] == "404":
                        data = None
                    else:
                        raise intf.BackendError(e)
                except botocore.exceptions.BotoCoreError as e:
                    raise intf.BackendError(e)

            if max_lag:
                got_digest = _digest(data)
                if got_digest == expected_digest:
                    break

            time.sleep(1)

        new_data = modifier(data)

        if new_data != data:
            if max_lag:
                new_digest = _digest(new_data)
                dynamo.put(loc, new_digest)

            def action(bucket: tp.Any) -> None:
                if new_data is None:
                    bucket.Object(bk.key).delete()
                else:
                    bucket.put_object(Key=bk.key, Body=new_data)

            def postponed() -> None:
                time.sleep(delay_put)
                session = bh.boto_session(opts)
                s3 = bh.boto_resource_s3(session, opts)
                bucket = s3.Bucket(bk.bucket)
                action(bucket)

            if delay_put:
                threading.Thread(target=postponed, daemon=True).start()
            else:
                action(bucket)

    @staticmethod
    def load(
        *,
        loc: str,
        opts: intf.Options,
    ) -> bytes:
        session = bh.boto_session(opts)
        s3 = bh.boto_resource_s3(session, opts)
        opts.fail_on_unused()
        bk = _BucketKey(loc)

        bucket = s3.Bucket(bk.bucket)
        with io.BytesIO() as f:
            try:
                bucket.download_fileobj(bk.key, f)
            except botocore.exceptions.ClientError as e:
                raise intf.BackendError(e)
            except botocore.exceptions.BotoCoreError as e:
                raise intf.BackendError(e)

            return f.getvalue()
