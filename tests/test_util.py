import asyncio

from unicall import stable_hash, unicall


def test_stable_hash_is_deterministic() -> None:
    assert stable_hash(1, "a", [1, 2, 3]) == stable_hash(1, "a", [1, 2, 3])


def test_stable_hash_distinguishes_different_inputs() -> None:
    assert stable_hash(1, "a") != stable_hash(1, "b")
    assert stable_hash([1, 2]) != stable_hash([2, 1])


async def test_stable_hash_as_a_key_recipe_for_large_arguments() -> None:
    calls = 0

    @unicall(key=lambda items: stable_hash(items))
    async def total(items: list[int]) -> int:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.02)
        return sum(items)

    results = list(await asyncio.gather(total([1, 2, 3]), total([1, 2, 3])))

    assert calls == 1
    assert results == [6, 6]
