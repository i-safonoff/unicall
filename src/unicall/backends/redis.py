"""A Backend for unicall.distributed() over Redis. Needs `unicall[redis]`."""

from __future__ import annotations

import json

from redis.asyncio import Redis

# Both scripts compare the lock's value against our token before touching it
# -- the classic Redlock safe-unlock pattern. Without the compare, a process
# whose lease already expired (a GC pause, a slow function) could delete or
# extend a lock a different process has since acquired, either releasing
# someone else's lock out from under them or renewing a lease that isn't
# ours to renew.
_RELEASE_SCRIPT = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end
"""

_RENEW_SCRIPT = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
    return 0
end
"""


class RedisBackend:
    """Distributed coordination over Redis for `unicall.distributed()`.

    Owns no connection lifecycle: pass in a client you created and close
    yourself. Two Redis keys per coalesced key -- `{key}:lock` (the lease)
    and `{key}:result` (the published outcome, `R`/`E` prefix for result vs
    error) -- rather than one, so a result can outlive the lock that
    produced it and a follower mid-poll never sees a half-written key.
    """

    def __init__(self, client: Redis) -> None:
        self._client = client
        self._release_script = client.register_script(_RELEASE_SCRIPT)
        self._renew_script = client.register_script(_RENEW_SCRIPT)

    async def try_acquire(self, key: str, token: str, lease_ms: int) -> bool:
        ok = await self._client.set(f"{key}:lock", token, nx=True, px=lease_ms)
        return bool(ok)

    async def renew(self, key: str, token: str, lease_ms: int) -> bool:
        result = await self._renew_script(keys=[f"{key}:lock"], args=[token, lease_ms])
        return bool(result)

    async def release(self, key: str, token: str) -> None:
        await self._release_script(keys=[f"{key}:lock"], args=[token])

    async def publish_result(self, key: str, payload: bytes, ttl_ms: int) -> None:
        await self._client.set(f"{key}:result", b"R" + payload, px=ttl_ms)

    async def publish_error(self, key: str, remote_type: str, message: str, ttl_ms: int) -> None:
        body = json.dumps({"type": remote_type, "message": message}).encode()
        await self._client.set(f"{key}:result", b"E" + body, px=ttl_ms)

    async def poll_result(self, key: str) -> tuple[bool, bytes | None, tuple[str, str] | None]:
        raw = await self._client.get(f"{key}:result")
        if raw is None:
            return False, None, None
        # A client configured with decode_responses=True hands back str
        # instead of bytes; normalize rather than require callers to know
        # this backend needs binary mode.
        data = raw.encode() if isinstance(raw, str) else raw
        marker, body = data[:1], data[1:]
        if marker == b"E":
            info = json.loads(body)
            return True, None, (info["type"], info["message"])
        return True, body, None
