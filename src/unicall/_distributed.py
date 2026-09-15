from __future__ import annotations

import asyncio
import contextlib
import json
import uuid
from collections.abc import Awaitable, Callable
from typing import Any, ParamSpec, Protocol, TypeVar

from ._core import Coalescer, KeyFunc
from ._metrics import Metrics

P = ParamSpec("P")
T = TypeVar("T")

_TIMED_OUT = object()


class RemoteFlightError(Exception):
    """Raised in a follower process when the leader's flight raised.

    Carries the leader's exception type name and message, not the original
    exception object. Reconstructing an arbitrary exception type from data
    that arrived over Redis would mean either guessing at constructor
    signatures or deserializing something that can run code -- this library
    does neither. See docs/DECISIONS.md.
    """

    def __init__(self, remote_type: str, message: str) -> None:
        super().__init__(f"{remote_type}: {message}")
        self.remote_type = remote_type
        self.remote_message = message


class UnserializableResultError(TypeError):
    """Raised when a flight's result can't cross the distributed boundary."""


class Serializer(Protocol):
    def dumps(self, value: Any) -> bytes: ...
    def loads(self, data: bytes) -> Any: ...


class JSONSerializer:
    """The only Serializer this library ships, deliberately. A serializer
    that can reconstruct arbitrary Python objects (pickle) means a
    compromised or merely misconfigured Redis can run code in every process
    reading from it. JSON can't do that; the cost is that only JSON-shaped
    results can cross the distributed boundary.
    """

    def dumps(self, value: Any) -> bytes:
        try:
            return json.dumps(value).encode()
        except TypeError as exc:
            raise UnserializableResultError(
                f"result {value!r} is not JSON-serializable; the distributed "
                "backend can only return JSON-shaped results -- pass "
                "serializer= for anything else"
            ) from exc

    def loads(self, data: bytes) -> Any:
        return json.loads(data)


class Backend(Protocol):
    """What a distributed coordination backend needs to provide. Keys and
    tokens are plain strings; results and errors are already-serialized
    bytes -- a Backend never needs to know the Serializer in use.
    """

    async def try_acquire(self, key: str, token: str, lease_ms: int) -> bool:
        """Try to become the leader for `key`. True if it succeeded."""
        ...

    async def renew(self, key: str, token: str, lease_ms: int) -> bool:
        """Extend the lease -- only if we're still the owner. True if it stuck."""
        ...

    async def release(self, key: str, token: str) -> None:
        """Give up leadership -- only if we're still the owner."""
        ...

    async def publish_result(self, key: str, payload: bytes, ttl_ms: int) -> None: ...

    async def publish_error(
        self, key: str, remote_type: str, message: str, ttl_ms: int
    ) -> None: ...

    async def poll_result(self, key: str) -> tuple[bool, bytes | None, tuple[str, str] | None]:
        """Non-blocking check: (found, result_payload, (remote_type, message))."""
        ...


class DistributedCoalescer(Coalescer[P, T]):
    """A Coalescer whose flights are also coordinated across processes
    through a Backend. In-process concurrency is still handled entirely by
    the base class -- shield, eviction, metrics, all of it; this only
    changes what happens the moment *this process* is about to actually run
    the function, by racing every other process for leadership of the key
    first.
    """

    def __init__(
        self,
        func: Callable[P, Awaitable[T]],
        *,
        backend: Backend,
        key: KeyFunc[P] | None = None,
        metrics: Metrics | None = None,
        lease: float = 5.0,
        wait_timeout: float = 10.0,
        poll_interval: float = 0.015,
        result_ttl: float = 5.0,
        serializer: Serializer | None = None,
    ) -> None:
        super().__init__(func, key=key, metrics=metrics)
        self._backend = backend
        self._lease_ms = int(lease * 1000)
        self._wait_timeout = wait_timeout
        self._poll_interval = poll_interval
        self._result_ttl_ms = int(result_ttl * 1000)
        self._serializer = serializer or JSONSerializer()

    def _remote_key(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
        local_key = self._make_key(args, kwargs)
        # The local key can be any Hashable; Redis keys are strings. repr()
        # is stable for the tuples _make_key produces and stays readable in
        # `redis-cli KEYS *`, which matters when debugging a stuck lock by
        # hand. Namespaced by qualname so two distributed functions that
        # happen to produce the same local key don't collide.
        return f"unicall:{self._func.__module__}.{self._func.__qualname__}:{local_key!r}"

    async def _execute(self, *args: P.args, **kwargs: P.kwargs) -> T:
        remote_key = self._remote_key(args, kwargs)
        while True:
            # Checked before every acquire attempt, not just while waiting
            # on someone else's flight: a fast (or instant, or failing)
            # function can publish its result and release its lock before a
            # second caller's own try_acquire ever runs. That caller would
            # find the lock gone and -- wrongly -- conclude it should become
            # leader too, running the function a second time. The result
            # outlives the lock (its own TTL, released or not), so checking
            # for it first closes that window.
            existing = await self._read_result(remote_key)
            if existing is not _TIMED_OUT:
                return existing  # type: ignore[no-any-return]
            token = uuid.uuid4().hex
            if await self._backend.try_acquire(remote_key, token, self._lease_ms):
                return await self._run_as_leader(remote_key, token, args, kwargs)
            outcome = await self._wait_for_remote(remote_key)
            if outcome is not _TIMED_OUT:
                return outcome  # type: ignore[no-any-return]
            # The leader vanished without ever publishing -- crashed, or
            # just slower than wait_timeout -- so nobody is running this key
            # anymore as far as we can tell. Loop back and race to become
            # leader ourselves instead of waiting forever behind a flight
            # that no longer exists.

    async def _run_as_leader(
        self,
        remote_key: str,
        token: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> T:
        lost_leadership = False

        async def renew_loop() -> None:
            nonlocal lost_leadership
            interval = max(self._lease_ms / 3000, 0.01)
            while True:
                await asyncio.sleep(interval)
                if not await self._backend.renew(remote_key, token, self._lease_ms):
                    lost_leadership = True
                    return

        renewer = asyncio.ensure_future(renew_loop())
        try:
            result = await self._func(*args, **kwargs)
        except Exception as exc:
            # A renewal we lost means some other process's lease has since
            # expired ours and it may already be running (or have run) the
            # same key. Publishing our own answer now could race a more
            # recent one -- so a process that has lost leadership publishes
            # nothing. Its own local callers still get the correct result:
            # that guarantee never depended on Redis in the first place.
            if not lost_leadership:
                await self._backend.publish_error(
                    remote_key, type(exc).__name__, str(exc), self._result_ttl_ms
                )
            raise
        else:
            if not lost_leadership:
                payload = self._serializer.dumps(result)
                await self._backend.publish_result(remote_key, payload, self._result_ttl_ms)
            return result
        finally:
            renewer.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await renewer
            if not lost_leadership:
                await self._backend.release(remote_key, token)

    async def _read_result(self, remote_key: str) -> Any:
        """Non-blocking: the decoded result, or _TIMED_OUT if none is
        published yet. Raises RemoteFlightError if the published outcome
        was an error.
        """
        found, payload, error_info = await self._backend.poll_result(remote_key)
        if not found:
            return _TIMED_OUT
        if error_info is not None:
            raise RemoteFlightError(*error_info)
        assert payload is not None
        return self._serializer.loads(payload)

    async def _wait_for_remote(self, remote_key: str) -> Any:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._wait_timeout
        while loop.time() < deadline:
            outcome = await self._read_result(remote_key)
            if outcome is not _TIMED_OUT:
                return outcome
            await asyncio.sleep(self._poll_interval)
        return _TIMED_OUT


def distributed(
    backend: Backend,
    *,
    key: KeyFunc[P] | None = None,
    metrics: Metrics | None = None,
    lease: float = 5.0,
    wait_timeout: float = 10.0,
    poll_interval: float = 0.015,
    result_ttl: float = 5.0,
    serializer: Serializer | None = None,
) -> Callable[[Callable[P, Awaitable[T]]], DistributedCoalescer[P, T]]:
    """Like `unicall()`, but flights are also coordinated across processes
    through `backend` (see `unicall.backends.redis.RedisBackend`).

    `lease` must comfortably exceed the function's expected worst-case
    runtime: a lease that expires mid-run lets another process become
    leader while this one is still working, which does not corrupt
    anything but does mean the same call can run twice. The lease is
    renewed automatically every `lease / 3` seconds while the function is
    still running.
    """

    def decorator(func: Callable[P, Awaitable[T]]) -> DistributedCoalescer[P, T]:
        return DistributedCoalescer(
            func,
            backend=backend,
            key=key,
            metrics=metrics,
            lease=lease,
            wait_timeout=wait_timeout,
            poll_interval=poll_interval,
            result_ttl=result_ttl,
            serializer=serializer,
        )

    return decorator
