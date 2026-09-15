import asyncio

import pytest

from unicall import unicall


async def test_stats_tracks_flights_and_coalescing() -> None:
    @unicall()
    async def f(x: int) -> int:
        await asyncio.sleep(0.02)
        return x

    await asyncio.gather(*(f(1) for _ in range(10)))

    stats = f.stats()
    assert stats.calls_total == 10
    assert stats.flights_started == 1
    assert stats.calls_coalesced == 9
    assert stats.coalesce_ratio == pytest.approx(0.9)


async def test_stats_tracks_sequential_flights_separately() -> None:
    @unicall()
    async def f(x: int) -> int:
        return x

    await f(1)
    await f(2)
    await f(3)

    stats = f.stats()
    assert stats.calls_total == 3
    assert stats.flights_started == 3
    assert stats.calls_coalesced == 0


async def test_stats_counts_errors() -> None:
    @unicall()
    async def fail() -> None:
        raise RuntimeError("boom")

    for _ in range(3):
        with pytest.raises(RuntimeError):
            await fail()

    assert fail.stats().errors == 3


async def test_external_metrics_hook_receives_every_event() -> None:
    events: list[str] = []

    class Recorder:
        def call_made(self) -> None:
            events.append("call_made")

        def flight_started(self) -> None:
            events.append("flight_started")

        def call_coalesced(self) -> None:
            events.append("call_coalesced")

        def flight_finished(self, *, ok: bool, duration: float) -> None:
            events.append(f"flight_finished:{ok}")

    @unicall(metrics=Recorder())
    async def f() -> int:
        await asyncio.sleep(0.02)
        return 1

    await asyncio.gather(f(), f())

    assert events.count("call_made") == 2
    assert events.count("flight_started") == 1
    assert events.count("call_coalesced") == 1
    assert events.count("flight_finished:True") == 1
