"""Measures the actual benefit of ttl=: two waves of callers, the second
arriving just after the first flight completes.

    python benchmarks/ttl_spike.py
"""

from __future__ import annotations

import asyncio

from unicall import unicall

LATENCY_SECONDS = 0.05
GAP_SECONDS = 0.005
WAVE_SIZE = 200


async def run() -> None:
    for ttl in (None, 0.02):
        calls = 0

        @unicall(key=lambda: "k", ttl=ttl)
        async def lookup() -> int:
            nonlocal calls
            calls += 1
            await asyncio.sleep(LATENCY_SECONDS)
            return 42

        await asyncio.gather(*(lookup() for _ in range(WAVE_SIZE)))
        await asyncio.sleep(GAP_SECONDS)
        await asyncio.gather(*(lookup() for _ in range(WAVE_SIZE)))

        label = "no ttl" if ttl is None else f"ttl={ttl}s"
        gap_ms = GAP_SECONDS * 1000
        print(f"{label}: {calls} executions for two waves of {WAVE_SIZE}, {gap_ms:.0f}ms apart")


if __name__ == "__main__":
    asyncio.run(run())
