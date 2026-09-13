import asyncio

from unicall import unicall


async def test_different_keys_run_concurrently_not_serialized() -> None:
    @unicall()
    async def f(x: int) -> int:
        await asyncio.sleep(0.05)
        return x

    loop = asyncio.get_running_loop()
    start = loop.time()
    results = await asyncio.gather(f(1), f(2), f(3))
    elapsed = loop.time() - start

    # Serialized behind one shared lock this would take ~0.15s; run in
    # parallel it should stay close to the single 0.05s sleep.
    assert elapsed < 0.12
    assert results == [1, 2, 3]
