"""Time-ordered identifiers (UUIDv7) and stable content hashes."""
from __future__ import annotations
import hashlib
import json
import os
import time
import uuid


def uuid7() -> str:
    ms = int(time.time() * 1000)
    rand = int.from_bytes(os.urandom(10), "big")
    value = (ms << 80) | (0x7 << 76) | ((rand >> 64) & 0x0FFF) << 64 | (0b10 << 62) | (rand & ((1 << 62) - 1))
    return str(uuid.UUID(int=value))


def content_hash(obj) -> str:
    """Deterministic sha256 over a JSON-serialisable object (sorted keys)."""
    payload = json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(payload).hexdigest()


def text_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()
