from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from uuid import UUID

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool


class PostgresStore:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._pool: Optional[AsyncConnectionPool] = None
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        async with self._lock:
            if not self._dsn:
                return
            if self._pool is None:
                self._pool = AsyncConnectionPool(conninfo=self._dsn, open=False, kwargs={"row_factory": dict_row})
                await self._pool.open()

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()

    async def log_request(
        self,
        request_id: UUID,
        method: str,
        params: Optional[Dict[str, Any]],
        raw_request: Dict[str, Any],
        upstream_id: Optional[str],
        tool_name: Optional[str],
        client_id: Optional[str],
        cache_key: Optional[str],
    ) -> None:
        if not self._pool:
            return
        async with self._pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO mcp_requests (id, method, params, raw_request, upstream_id, tool_name, client_id, cache_key)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    request_id,
                    method,
                    Jsonb(params) if params is not None else None,
                    Jsonb(raw_request),
                    upstream_id,
                    tool_name,
                    client_id,
                    cache_key,
                ),
            )

    async def log_response(
        self,
        response_id: UUID,
        request_id: UUID,
        success: bool,
        latency_ms: int,
        cache_hit: bool,
        response: Dict[str, Any],
    ) -> None:
        if not self._pool:
            return
        async with self._pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO mcp_responses (id, request_id, success, latency_ms, cache_hit, response)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    response_id,
                    request_id,
                    success,
                    latency_ms,
                    cache_hit,
                    Jsonb(response),
                ),
            )

    async def log_denial(
        self,
        denial_id: UUID,
        request_id: UUID,
        upstream_id: Optional[str],
        tool_name: Optional[str],
        reason: str,
    ) -> None:
        if not self._pool:
            return
        async with self._pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO mcp_denials (id, request_id, upstream_id, tool_name, reason)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    denial_id,
                    request_id,
                    upstream_id,
                    tool_name,
                    reason,
                ),
            )

    async def cache_get(self, cache_key: str) -> Optional[Dict[str, Any]]:
        if not self._pool:
            return None
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """
                SELECT response, expires_at
                FROM mcp_cache
                WHERE cache_key = %s
                """,
                (cache_key,),
            )
            row = await cur.fetchone()
            if not row:
                return None
            if row["expires_at"] <= datetime.now(timezone.utc):
                await conn.execute("DELETE FROM mcp_cache WHERE cache_key = %s", (cache_key,))
                return None
            return row["response"]

    async def cache_set(self, cache_key: str, response: Dict[str, Any], ttl_seconds: int) -> None:
        if not self._pool:
            return
        async with self._pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO mcp_cache (cache_key, response, expires_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (cache_key)
                DO UPDATE SET response = EXCLUDED.response, expires_at = EXCLUDED.expires_at
                """,
                (
                    cache_key,
                    Jsonb(response),
                    datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds),
                ),
            )

    async def cleanup_expired_cache(self) -> int:
        if not self._pool:
            return 0
        async with self._pool.connection() as conn:
            cur = await conn.execute("DELETE FROM mcp_cache WHERE expires_at < now()")
            return max(0, cur.rowcount)
