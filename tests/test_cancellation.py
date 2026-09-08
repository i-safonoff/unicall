import asyncio

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
