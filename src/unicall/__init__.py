"""Collapse concurrent async calls to the same thing into one flight."""

from ._core import Coalescer, UnhashableArgumentsError, unicall

__all__ = ["Coalescer", "UnhashableArgumentsError", "unicall"]
__version__ = "0.1.0"
