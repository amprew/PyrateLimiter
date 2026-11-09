"""C5 (#305): the Leaker prefers a bucket's declared ``is_async`` and only
falls back to the side-effecting ``leak(0)`` probe when it is ``None``.
"""
import asyncio

from pyrate_limiter import AbstractBucket, BucketAsyncWrapper, InMemoryBucket, Limiter, Rate
from pyrate_limiter.abstracts.bucket import Leaker

RATES = [Rate(3, 1000)]


def test_default_is_async_is_none_and_builtins_declare():
    assert AbstractBucket.is_async is None  # unknown by default -> probe
    assert InMemoryBucket(RATES).is_async is False
    assert BucketAsyncWrapper(InMemoryBucket(RATES)).is_async is True


class _ExplodingLeakSync(InMemoryBucket):
    is_async = False

    def leak(self, current_timestamp=None):  # pragma: no cover - must not run
        raise AssertionError("leak() must not be probed when is_async is declared")


class _ExplodingLeakAsync(InMemoryBucket):
    is_async = True

    def leak(self, current_timestamp=None):  # pragma: no cover - must not run
        raise AssertionError("leak() must not be probed when is_async is declared")


class _ProbedAsync(AbstractBucket):
    """is_async left as the inherited None -> Leaker must fall back to probing
    leak(0), which returns a coroutine and routes this to async."""

    def __init__(self):
        self.rates = RATES

    async def put(self, item):
        return True

    async def leak(self, current_timestamp=None):
        return 0

    async def flush(self):
        return None

    async def count(self):
        return 0

    async def peek(self, index):
        return None


def test_declared_sync_registers_without_probing():
    leaker = Leaker(10_000)
    bucket = _ExplodingLeakSync(RATES)
    leaker.register(bucket)  # would raise if leak() were probed
    assert id(bucket) in leaker.sync_buckets
    assert id(bucket) not in leaker.async_buckets


def test_declared_async_registers_without_probing():
    leaker = Leaker(10_000)
    bucket = _ExplodingLeakAsync(RATES)
    leaker.register(bucket)  # would raise if leak() were probed
    assert id(bucket) in leaker.async_buckets
    assert id(bucket) not in leaker.sync_buckets


def test_unknown_falls_back_to_probe():
    leaker = Leaker(10_000)
    bucket = _ProbedAsync()
    assert bucket.is_async is None
    leaker.register(bucket)  # probes leak(0) -> coroutine -> async
    assert id(bucket) in leaker.async_buckets


def test_async_bucket_limiter_can_be_created_outside_event_loop():
    limiter = Limiter(BucketAsyncWrapper(InMemoryBucket(RATES)))
    leaker = limiter.bucket_factory._leaker

    assert leaker is not None
    assert leaker.aio_leak_task is None

    async def acquire_and_close():
        assert await limiter.try_acquire_async("item")
        task = leaker.aio_leak_task
        assert task is not None

        limiter.close()
        await asyncio.gather(task, return_exceptions=True)
        assert task.cancelled()

    asyncio.run(acquire_and_close())
