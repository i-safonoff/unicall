import asyncio

import pytest

from unicall import RemoteFlightError, UnserializableResultError, distributed
from unicall.backends.redis import RedisBackend

pytestmark = pytest.mark.integration


async def test_two_coalescers_share_one_execution_via_redis(redis_client) -> None:
    backend = RedisBackend(redis_client)
    calls = 0

    async def slow(x: int) -> int:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.1)
        return x * 2

    # Two independent Coalescer instances, each with its own local registry
    # -- as if two separate worker processes had each decorated the same
    # function -- sharing one Redis. Without the distributed backend this
    # would be 2 executions (one local flight per instance); with it, 1.
    process_a = distributed(backend)(slow)
    process_b = distributed(backend)(slow)

    results = await asyncio.gather(
        *(process_a(21) for _ in range(5)),
        *(process_b(21) for _ in range(5)),
    )

    assert calls == 1
    assert results == [42] * 10


async def test_a_vanished_leader_is_recovered_after_its_lease_expires(redis_client) -> None:
    backend = RedisBackend(redis_client)
    calls = 0

    async def f() -> str:
        nonlocal calls
        calls += 1
        return "alive"

    coalescer = distributed(backend, lease=0.05, wait_timeout=0.3, poll_interval=0.01)(f)
    remote_key = coalescer._remote_key((), {})

    # A "crashed" leader: it acquired the lock and then, unlike every real
    # execution path in this library, never renews, never publishes, never
    # releases. Its lease is the only thing that ever removes it.
    acquired = await backend.try_acquire(remote_key, "dead-process-token", lease_ms=50)
    assert acquired

    result = await coalescer()

    assert result == "alive"
    assert calls == 1


async def test_lease_renewal_keeps_a_long_flight_exclusive(redis_client) -> None:
    backend = RedisBackend(redis_client)
    calls = 0

    async def slow() -> str:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.15)
        return "done"

    # lease shorter than the runtime, and wait_timeout shorter than the
    # runtime too: the follower *will* time out waiting and try to become
    # leader itself before the original leader finishes. If renewal did not
    # keep extending the lease, that retry would find the lock expired,
    # acquire it, and run slow() a second time.
    make = distributed(backend, lease=0.05, wait_timeout=0.08, poll_interval=0.01)
    process_a = make(slow)
    process_b = make(slow)

    results = await asyncio.gather(process_a(), process_b())

    assert calls == 1
    assert results == ["done", "done"]


async def test_safe_release_does_not_delete_a_lock_it_no_longer_owns(redis_client) -> None:
    backend = RedisBackend(redis_client)
    key = "unicall:test:safe-release"

    await backend.try_acquire(key, "token-a", lease_ms=50)
    await asyncio.sleep(0.1)  # token-a's lease expires
    assert await backend.try_acquire(key, "token-b", lease_ms=5_000)

    # token-a doesn't know it lost the lock and tries to release "its" lock.
    await backend.release(key, "token-a")

    assert await redis_client.get(f"{key}:lock") == b"token-b"


async def test_safe_renew_does_not_extend_a_lock_it_no_longer_owns(redis_client) -> None:
    backend = RedisBackend(redis_client)
    key = "unicall:test:safe-renew"

    await backend.try_acquire(key, "token-a", lease_ms=50)
    await asyncio.sleep(0.1)
    assert await backend.try_acquire(key, "token-b", lease_ms=5_000)

    renewed = await backend.renew(key, "token-a", lease_ms=5_000)

    assert renewed is False
    assert await redis_client.get(f"{key}:lock") == b"token-b"


async def test_leaders_error_propagates_as_remote_flight_error(redis_client) -> None:
    backend = RedisBackend(redis_client)

    async def boom() -> None:
        raise ValueError("bad input")

    make = distributed(backend, wait_timeout=1.0, poll_interval=0.01)
    process_a = make(boom)
    process_b = make(boom)

    results = await asyncio.gather(process_a(), process_b(), return_exceptions=True)

    # Exactly one of the two actually raced to leadership and ran boom();
    # it raises ValueError directly. The other loses the race, waits, and
    # sees the leader's failure as a RemoteFlightError -- never a
    # reconstructed ValueError, see docs/DECISIONS.md.
    result_types = sorted(type(r).__name__ for r in results)
    assert result_types == ["RemoteFlightError", "ValueError"]

    remote_error = next(r for r in results if isinstance(r, RemoteFlightError))
    assert remote_error.remote_type == "ValueError"
    assert remote_error.remote_message == "bad input"


async def test_an_instant_flight_does_not_run_twice(redis_client) -> None:
    backend = RedisBackend(redis_client)
    calls = 0

    async def instant(x: int) -> int:
        nonlocal calls
        calls += 1
        return x

    # No sleep anywhere in instant(): a leader can acquire, run, publish,
    # and release the lock before a second caller's own try_acquire has
    # even happened. If that second caller checked the lock instead of the
    # published result first, it would find the lock already gone and
    # wrongly become a leader itself.
    make = distributed(backend)
    process_a = make(instant)
    process_b = make(instant)

    results = await asyncio.gather(
        *(process_a(1) for _ in range(20)),
        *(process_b(1) for _ in range(20)),
    )

    assert calls == 1
    assert results == [1] * 40


async def test_unserializable_result_raises_a_clear_error(redis_client) -> None:
    backend = RedisBackend(redis_client)

    async def give_a_set() -> set[int]:
        return {1, 2, 3}

    coalescer = distributed(backend)(give_a_set)

    with pytest.raises(UnserializableResultError):
        await coalescer()
