from __future__ import annotations

import asyncio
import time
from collections import OrderedDict
from collections.abc import Iterable

from app.core.state_manager import PersistedEventKey


class EventDeduplicator:
    """In-memory TTL deduper for replayed websocket payloads.

    Phase 1 keeps this intentionally local to the process. Phase 5 can persist
    checkpoints, but the event fingerprinting contract should remain stable.
    """

    def __init__(
        self,
        ttl_seconds: float,
        max_items: int,
        initial_entries: Iterable[PersistedEventKey] | None = None,
    ) -> None:
        if ttl_seconds <= 0:
            raise ValueError("dedupe ttl must be positive")
        if max_items <= 0:
            raise ValueError("dedupe max_items must be positive")
        self._ttl_seconds = ttl_seconds
        self._max_items = max_items
        self._seen: OrderedDict[str, float] = OrderedDict()
        self._lock = asyncio.Lock()
        now = time.time()
        if initial_entries:
            for entry in initial_entries:
                if now - entry.inserted_at_epoch <= self._ttl_seconds:
                    self._seen[entry.key] = entry.inserted_at_epoch
            self._evict_overflow()

    async def check_and_mark(self, key: str) -> bool:
        """Return True when the key is new, False when it is a duplicate."""
        now = time.time()
        async with self._lock:
            self._evict_expired(now)
            if key in self._seen:
                self._seen.move_to_end(key)
                return False
            self._seen[key] = now
            self._evict_overflow()
            return True

    def _evict_expired(self, now: float) -> None:
        while self._seen:
            _, inserted_at = next(iter(self._seen.items()))
            if now - inserted_at <= self._ttl_seconds:
                break
            self._seen.popitem(last=False)

    def _evict_overflow(self) -> None:
        while len(self._seen) > self._max_items:
            self._seen.popitem(last=False)
