from __future__ import annotations

import asyncio
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional


@dataclass
class CacheEntry:
    value: Any
    expires_at: datetime


class TTLCache:
    def __init__(self, max_entries: int) -> None:
        self._max_entries = max_entries
        self._entries: OrderedDict[str, CacheEntry] = OrderedDict()
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[Any]:
        async with self._lock:
            entry = self._entries.get(key)
            if not entry:
                return None
            if entry.expires_at <= datetime.now(timezone.utc):
                self._entries.pop(key, None)
                return None
            self._entries.move_to_end(key)
            return entry.value

    async def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        async with self._lock:
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
            self._entries[key] = CacheEntry(value=value, expires_at=expires_at)
            self._entries.move_to_end(key)
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)

    async def delete(self, key: str) -> None:
        async with self._lock:
            self._entries.pop(key, None)
