import asyncio

import pytest

from unicall import UnhashableArgumentsError, unicall


async def test_positional_and_keyword_calls_share_a_key() -> None:
    calls = 0

    @unicall()
    async def f(x: int) -> int:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.02)
        return x

    results = await asyncio.gather(f(5), f(x=5))

    assert calls == 1
    assert results == [5, 5]


async def test_unhashable_arguments_raise_a_clear_error() -> None:
    @unicall()
    async def f(items: list[int]) -> int:
        return sum(items)

    with pytest.raises(UnhashableArgumentsError):
        await f([1, 2, 3])


async def test_custom_key_function_overrides_the_default() -> None:
    calls = 0

    @unicall(key=lambda request: request["user_id"])
    async def f(request: dict[str, object]) -> object:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.02)
        return request["user_id"]

    results = await asyncio.gather(
        f({"user_id": 1, "trace": "a"}),
        f({"user_id": 1, "trace": "b"}),
    )

    assert calls == 1
    assert results == [1, 1]
