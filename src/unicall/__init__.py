"""Collapse concurrent async calls to the same thing into one flight."""

from ._core import Coalescer, UnhashableArgumentsError, unicall
from ._metrics import Metrics, Stats
from ._util import stable_hash

__all__ = [
    "Coalescer",
    "Metrics",
    "Stats",
    "UnhashableArgumentsError",
    "stable_hash",
    "unicall",
]
__version__ = "0.1.0"
