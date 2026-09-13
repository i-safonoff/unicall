"""How many times does a cold resource actually get hit by a burst of
concurrent callers, with and without unicall?

    python benchmarks/stampede.py
"""

from __future__ import annotations

import asyncio
import time

from unicall import unicall

CONCURRENT_CALLERS = 1000
LATENCY_SECONDS = 0.05


async def cold_lookup(hits: list[int]) -> int:
    hits.append(1)
    await asyncio.sleep(LATENCY_SECONDS)
    return 42


@unicall(key=lambda hits: "the-one-key")
async def coalesced_lookup(hits: list[int]) -> int:
    return await cold_lookup(hits)


async def run() -> None:
    plain_hits: list[int] = []
    start = time.perf_counter()
    await asyncio.gather(*(cold_lookup(plain_hits) for _ in range(CONCURRENT_CALLERS)))
    plain_elapsed = time.perf_counter() - start

    coalesced_hits: list[int] = []
    start = time.perf_counter()
    await asyncio.gather(*(coalesced_lookup(coalesced_hits) for _ in range(CONCURRENT_CALLERS)))
    coalesced_elapsed = time.perf_counter() - start

    latency_ms = LATENCY_SECONDS * 1000
    print(f"{CONCURRENT_CALLERS} concurrent callers, one cold key, {latency_ms:.0f}ms latency")
    print(f"  without unicall: {len(plain_hits)} executions, {plain_elapsed:.3f}s wall")
    print(f"  with unicall:    {len(coalesced_hits)} execution,  {coalesced_elapsed:.3f}s wall")


if __name__ == "__main__":
    asyncio.run(run())
