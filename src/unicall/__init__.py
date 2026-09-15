"""Collapse concurrent async calls to the same thing into one flight."""

from ._core import Coalescer, UnhashableArgumentsError, unicall
from ._distributed import (
    Backend,
    DistributedCoalescer,
    JSONSerializer,
    RemoteFlightError,
    Serializer,
    UnserializableResultError,
    distributed,
)
from ._metrics import Metrics, Stats
from ._util import stable_hash

__all__ = [
    "Backend",
    "Coalescer",
    "DistributedCoalescer",
    "JSONSerializer",
    "Metrics",
    "RemoteFlightError",
    "Serializer",
    "Stats",
    "UnhashableArgumentsError",
    "UnserializableResultError",
    "distributed",
    "stable_hash",
    "unicall",
]
__version__ = "0.2.0"
