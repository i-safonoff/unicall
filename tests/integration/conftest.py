"""Real Redis, because the distributed backend's correctness depends on
exact Redis semantics -- SET NX PX, a Lua script's atomicity, a key's TTL --
that a fake would have to reimplement to fake, at which point the test would
only be checking that the fake agrees with itself.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator
from typing import TYPE_CHECKING

import pytest
import pytest_asyncio

if TYPE_CHECKING:
    from redis.asyncio import Redis

pytestmark = pytest.mark.integration

# Ryuk is testcontainers' reaper: a sidecar that bind-mounts the Docker
# socket and removes containers if the test process dies. On Docker Desktop
# for Mac it does not work -- the socket lives under ~/.docker/run, which the
# VM refuses to bind-mount. Disabled rather than worked around; the fixture
# below is a context manager, so a normal exit or a failed test still cleans
# up. What's lost is cleanup after a SIGKILL.
os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")


@pytest.fixture(scope="session")
def redis_url() -> Iterator[str]:
    from testcontainers.community.redis import RedisContainer

    with RedisContainer("redis:7-alpine") as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(6379)
        yield f"redis://{host}:{port}/0"


@pytest_asyncio.fixture
async def redis_client(redis_url: str) -> AsyncIterator[Redis]:
    from redis.asyncio import Redis

    client = Redis.from_url(redis_url)
    try:
        await client.flushdb()
        yield client
    finally:
        await client.aclose()
