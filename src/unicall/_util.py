from __future__ import annotations

import hashlib
from typing import Any


def stable_hash(*parts: Any) -> str:
    """A deterministic hash of `parts`, for use as a `key=` when arguments
    are too large to hash directly -- or hashing them is itself the
    expensive part of the call.

    Uses each part's `repr()`, so this is only as good as that repr: a
    custom object whose default repr includes its `id()` defeats it, since
    two equal-looking calls would still hash differently.
    """
    digest = hashlib.blake2b(digest_size=16)
    for part in parts:
        digest.update(repr(part).encode())
        digest.update(b"\0")
    return digest.hexdigest()
