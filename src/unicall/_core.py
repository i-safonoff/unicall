from __future__ import annotations

import asyncio
import functools
from collections.abc import Awaitable, Callable, Hashable
from typing import Any, Generic, ParamSpec, TypeVar

P = ParamSpec("P")
T = TypeVar("T")


def _default_key(args: tuple[Any, ...], kwargs: dict[str, Any]) -> Hashable:
    return (args, tuple(sorted(kwargs.items())))


class Coalescer(Generic[P, T]):
    """Wraps an async function so concurrent calls with the same key share one flight."""

    def __init__(self, func: Callable[P, Awaitable[T]]) -> None:
        functools.update_wrapper(self, func)
        self._func = func
        self._flights: dict[Hashable, asyncio.Task[T]] = {}

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> T:
        key = _default_key(args, kwargs)
        task = self._flights.get(key)
        if task is None or task.done():
            task = asyncio.ensure_future(self._func(*args, **kwargs))
            self._flights[key] = task
        # asyncio.shield matters here: awaiting `task` directly would let a
        # caller's own cancellation (e.g. from asyncio.wait_for) propagate
        # into `task` itself, cancelling the flight for every other waiter.
        # shield lets a caller walk away without taking the flight down.
        return await asyncio.shield(task)


def unicall() -> Callable[[Callable[P, Awaitable[T]]], Coalescer[P, T]]:
    """Decorate an async function so concurrent calls with equal arguments
    share a single execution.
    """

    def decorator(func: Callable[P, Awaitable[T]]) -> Coalescer[P, T]:
        return Coalescer(func)

    return decorator
