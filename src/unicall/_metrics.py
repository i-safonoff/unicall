from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


class Metrics(Protocol):
    """Hook for wiring unicall's events into an external metrics system
    (Prometheus, statsd, whatever). Every method is called in addition to,
    not instead of, the built-in counters `Coalescer.stats()` returns.
    """

    def call_made(self) -> None: ...
    def flight_started(self) -> None: ...
    def call_coalesced(self) -> None: ...
    def flight_finished(self, *, ok: bool, duration: float) -> None: ...


@dataclass
class Stats:
    """Snapshot of a Coalescer's built-in counters."""

    calls_total: int = 0
    flights_started: int = 0
    calls_coalesced: int = 0
    errors: int = 0

    @property
    def coalesce_ratio(self) -> float:
        """Fraction of calls that joined a flight instead of starting one."""
        return self.calls_coalesced / self.calls_total if self.calls_total else 0.0


class _CountingMetrics:
    """The always-on, dependency-free implementation behind `.stats()`."""

    def __init__(self) -> None:
        self.stats = Stats()

    def call_made(self) -> None:
        self.stats.calls_total += 1

    def flight_started(self) -> None:
        self.stats.flights_started += 1

    def call_coalesced(self) -> None:
        self.stats.calls_coalesced += 1

    def flight_finished(self, *, ok: bool, duration: float) -> None:
        del duration  # not kept: see docs/DECISIONS.md on why no latency histogram
        if not ok:
            self.stats.errors += 1
