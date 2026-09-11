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
        if task is None or task.done():
            task = asyncio.ensure_future(self._func(*args, **kwargs))
            self._flights[key] = task
        # asyncio.shield matters here: awaiting `task` directly would let a
        # caller's own cancellation (e.g. from asyncio.wait_for) propagate
        # into `task` itself, cancelling the flight for every other waiter.
        # shield lets a caller walk away without taking the flight down.
        return await asyncio.shield(task)


def unicall(
    *, key: KeyFunc[P] | None = None
) -> Callable[[Callable[P, Awaitable[T]]], Coalescer[P, T]]:
    """Decorate an async function so concurrent calls with equal arguments
    (or an equal `key(...)`) share a single execution.
    """

    def decorator(func: Callable[P, Awaitable[T]]) -> Coalescer[P, T]:
        return Coalescer(func, key=key)

    return decorator
