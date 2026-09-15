"""How many times does a cold resource get hit by a burst of concurrent
callers spread across multiple processes, with only local coalescing vs.
with the distributed backend coordinating them? Starts its own throwaway
Redis via testcontainers -- needs unicall[redis] and Docker.

    python benchmarks/distributed_stampede.py
"""

from __future__ import annotations

import asyncio

from unicall import distributed, unicall
from unicall.backends.redis import RedisBackend

PROCESSES = 4
CALLERS_PER_PROCESS = 50
LATENCY_SECONDS = 0.05


async def cold_lookup(hits: list[int]) -> int:
    hits.append(1)
    await asyncio.sleep(LATENCY_SECONDS)
    return 42


async def run() -> None:
    from redis.asyncio import Redis
    from testcontainers.community.redis import RedisContainer

    with RedisContainer("redis:7-alpine") as container:
        client = Redis.from_url(
            f"redis://{container.get_container_host_ip()}:{container.get_exposed_port(6379)}/0"
        )
        try:
            # Each "process" is its own Coalescer instance in this same
            # event loop -- local coalescing happens within each one, but
            # nothing connects them, exactly like N real worker processes
            # with no distributed backend.
            local_hits: list[int] = []
            local_processes = [unicall(key=lambda hits: "k")(cold_lookup) for _ in range(PROCESSES)]
            await asyncio.gather(
                *(p(local_hits) for p in local_processes for _ in range(CALLERS_PER_PROCESS))
            )

            backend = RedisBackend(client)
            dist_hits: list[int] = []
            dist_processes = [
                distributed(backend, key=lambda hits: "k")(cold_lookup) for _ in range(PROCESSES)
            ]
            await asyncio.gather(
                *(p(dist_hits) for p in dist_processes for _ in range(CALLERS_PER_PROCESS))
            )
        finally:
            await client.aclose()

    total = PROCESSES * CALLERS_PER_PROCESS
    print(f"{PROCESSES} processes x {CALLERS_PER_PROCESS} concurrent callers ({total} total)")
    print("one cold key:")
    print(f"  local coalescing only: {len(local_hits)} executions ({PROCESSES} processes)")
    print(f"  + distributed backend: {len(dist_hits)} execution")


if __name__ == "__main__":
    asyncio.run(run())
