import asyncio

from unicall import unicall


async def test_concurrent_calls_share_one_execution() -> None:
    calls = 0

    @unicall()
    async def slow(x: int) -> int:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.05)
        return x * 2

    results = await asyncio.gather(*(slow(7) for _ in range(20)))

    assert calls == 1
    assert results == [14] * 20


async def test_sequential_calls_each_get_their_own_flight() -> None:
    calls = 0

    @unicall()
    async def fast(x: int) -> int:
        nonlocal calls
        calls += 1
        return x

    assert await fast(1) == 1
    assert await fast(1) == 1
    assert calls == 2


async def test_exception_is_shared_by_every_waiter() -> None:
    class BoomError(Exception):
        pass

    @unicall()
    async def fail() -> None:
        await asyncio.sleep(0.02)
        raise BoomError("no")

    results = await asyncio.gather(*(fail() for _ in range(5)), return_exceptions=True)

    assert len(results) == 5
    assert all(isinstance(r, BoomError) for r in results)
