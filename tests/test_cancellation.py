import asyncio
from typing import Any

import pytest

from unicall import unicall


async def test_one_callers_timeout_does_not_kill_the_flight_for_others() -> None:
    calls = 0

    @unicall()
    async def slow() -> str:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.1)
        return "done"

    async def impatient() -> None:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(slow(), timeout=0.02)

    patient = asyncio.ensure_future(slow())
    await impatient()
    result = await patient

    assert result == "done"
    assert calls == 1


async def test_flight_exception_is_retrieved_even_if_every_caller_cancels() -> None:
    loop = asyncio.get_running_loop()
    unretrieved: list[dict[str, Any]] = []
    loop.set_exception_handler(lambda _loop, context: unretrieved.append(context))

    try:

        @unicall()
        async def fail_slow() -> None:
            await asyncio.sleep(0.05)
            raise RuntimeError("boom")

        waiters = [asyncio.ensure_future(fail_slow()) for _ in range(3)]
        await asyncio.sleep(0.01)
        for w in waiters:
            w.cancel()
        for w in waiters:
            with pytest.raises(asyncio.CancelledError):
                await w

        # Give the (shielded, still-running) underlying flight time to
        # finish and its done-callback time to run.
        await asyncio.sleep(0.1)
        await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(None)

    assert not any("never retrieved" in str(ctx.get("message", "")) for ctx in unretrieved)
