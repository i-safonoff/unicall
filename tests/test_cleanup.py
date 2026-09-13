from unicall import unicall


async def test_registry_does_not_grow_for_keys_called_once() -> None:
    @unicall()
    async def f(x: int) -> int:
        return x

    for i in range(500):
        await f(i)

    assert f.in_flight() == 0
