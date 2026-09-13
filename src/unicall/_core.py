from __future__ import annotations

import asyncio
import functools
import inspect
from collections.abc import Awaitable, Callable, Hashable
from typing import Any, Generic, ParamSpec, TypeVar

P = ParamSpec("P")
T = TypeVar("T")

KeyFunc = Callable[P, Hashable]


class UnhashableArgumentsError(TypeError):
    """Raised when a call's arguments can't be turned into a dedup key."""


def _default_key(
    signature: inspect.Signature, args: tuple[Any, ...], kwargs: dict[str, Any]
) -> Hashable:
    # Bind through the function's own signature rather than hashing
    # (args, kwargs) as received: f(5) and f(x=5) are the same call, and
    # without this they landed under two different keys and ran twice.
    bound = signature.bind(*args, **kwargs)
    bound.apply_defaults()
    key = tuple(sorted(bound.arguments.items()))
    try:
        hash(key)
    except TypeError as exc:
        raise UnhashableArgumentsError(
            f"arguments {args!r}, {kwargs!r} are not hashable; "
            "pass key=... to derive a dedup key explicitly"
        ) from exc
    return key


class Coalescer(Generic[P, T]):
    """Wraps an async function so concurrent calls with the same key share one flight."""

    def __init__(
        self,
        func: Callable[P, Awaitable[T]],
        *,
        key: KeyFunc[P] | None = None,
    ) -> None:
        functools.update_wrapper(self, func)
        self._func = func
        self._key = key
        self._signature = inspect.signature(func)
        self._flights: dict[Hashable, asyncio.Task[T]] = {}

    def _make_key(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Hashable:
        if self._key is not None:
            return self._key(*args, **kwargs)
        return _default_key(self._signature, args, kwargs)

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        key = self._make_key(args, kwargs)
        task = self._flights.get(key)
        # No `await` between the lookup above and the insert below. In
        # asyncio, only an await point yields control to another coroutine,
        # so this check-and-set can't race even without a lock -- and a lock
        # would itself be the await that reopens the race it was meant to
        # close.
        if task is None:
            task = asyncio.ensure_future(self._func(*args, **kwargs))
            self._flights[key] = task
            task.add_done_callback(self._make_done_callback(key, task))
        # asyncio.shield matters here: awaiting `task` directly would let a
        # caller's own cancellation (e.g. from asyncio.wait_for) propagate
        # into `task` itself, cancelling the flight for every other waiter.
        # shield lets a caller walk away without taking the flight down.
        return await asyncio.shield(task)

    def _make_done_callback(
        self, key: Hashable, task: asyncio.Task[T]
    ) -> Callable[[asyncio.Task[T]], None]:
        def _on_done(finished: asyncio.Task[T]) -> None:
            # Eviction happens here, unconditionally, rather than lazily on
            # the next lookup for this key. A key called exactly once would
            # otherwise leave its finished task in the registry forever --
            # the registry would grow by one entry per distinct key ever
            # seen instead of holding only what's currently in flight.
            if self._flights.get(key) is finished:
                del self._flights[key]
            # A flight nobody stuck around to await still needs its
            # exception retrieved, or asyncio logs "exception was never
            # retrieved" once the task is garbage collected. shield()
            # detaches a cancelled caller without reading the result, so if
            # every caller cancels before the flight finishes, nothing else
            # ever calls .exception() on it.
            if not finished.cancelled():
                finished.exception()

        return _on_done

    def in_flight(self) -> int:
        """Number of keys currently being awaited (not cached results)."""
        return len(self._flights)


def unicall(
    *, key: KeyFunc[P] | None = None
) -> Callable[[Callable[P, Awaitable[T]]], Coalescer[P, T]]:
    """Decorate an async function so concurrent calls with equal arguments
    (or an equal `key(...)`) share a single execution.
    """

    def decorator(func: Callable[P, Awaitable[T]]) -> Coalescer[P, T]:
        return Coalescer(func, key=key)

    return decorator
