import datetime
import typing as tp

import implements
import python_dynamodb_lock.python_dynamodb_lock as dlock
import with_cloud_blob.backend_intf as intf

from . import _boto_helpers as bh


class _Lock:
    def __init__(
        self,
        *,
        lock_client: dlock.DynamoDBLockClient,
        key: str,
        timeout: float,
    ) -> None:
        self._lock_client = lock_client
        self._key = key
        self._timeout = timeout
        self._lock: tp.Optional[dlock.DynamoDBLock] = None

    def __enter__(self) -> None:
        try:
            self._lock = self._lock_client.acquire_lock(
                self._key,
                retry_timeout=datetime.timedelta(seconds=self._timeout),
            )
        except dlock.DynamoDBLockError as e:
            if e.code == dlock.DynamoDBLockError.ACQUIRE_TIMEOUT:
                raise intf.TimeoutError()
            else:
                raise

    def __exit__(self, *a: tp.Any) -> bool:
        self._lock_client.release_lock(self._lock)
        return False


@implements.implements(intf.ILockBackend)
class Backend:
    @staticmethod
    def make_lock(
        *,
        loc: str,
        opts: intf.Options,
        timeout: float,
    ) -> tp.ContextManager[None]:
        session = bh.boto_session(opts)
        dynamodb_resource = bh.boto_resource_dynamodb(session, opts)
        lock_client = dlock.DynamoDBLockClient(dynamodb_resource)
        opts.fail_on_unused()

        # TODO cleate table

        return _Lock(
            lock_client=lock_client,
            key=loc,
            timeout=timeout,
        )
