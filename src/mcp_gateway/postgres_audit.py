from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from uuid import UUID

from psycopg.types.json import Jsonb


class PostgresAuditMixin:
    async def log_request(
        self,
        request_id: UUID,
        method: str,
        params: Optional[Dict[str, Any]],
        raw_request: Dict[str, Any],
        upstream_id: Optional[str],
        tool_name: Optional[str],
        client_id: Optional[str],
        auth_user_id: Optional[str],
        auth_api_key_id: Optional[str],
        auth_role: Optional[str],
        cache_key: Optional[str],
        auth_subject: Optional[str] = None,
        auth_scheme: Optional[str] = None,
        auth_group_names: Optional[list[str]] = None,
        authorized_upstream_id: Optional[str] = None,
    ) -> None:
        if not self._pool:
            return
        async with self._pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO mcp_requests (
                    id,
                    method,
                    params,
                    raw_request,
                    upstream_id,
                    tool_name,
                    client_id,
                    auth_user_id,
                    auth_api_key_id,
                    auth_role,
                    auth_subject,
                    auth_scheme,
                    auth_group_names,
                    authorized_upstream_id,
                    cache_key
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    request_id,
                    method,
                    Jsonb(params) if params is not None else None,
                    Jsonb(raw_request),
                    upstream_id,
                    tool_name,
                    client_id,
                    auth_user_id,
                    auth_api_key_id,
                    auth_role,
                    auth_subject,
                    auth_scheme,
                    Jsonb(auth_group_names or []),
                    authorized_upstream_id,
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

    async def consume_rate_limit(
        self,
        *,
        scope_key: str,
        limit: int,
        window_seconds: int = 60,
        now: Optional[datetime] = None,
    ) -> dict[str, int | bool]:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        effective_now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        window_started_at = effective_now - timedelta(
            seconds=effective_now.second % window_seconds,
            microseconds=effective_now.microsecond,
        )
        window_ends_at = window_started_at + timedelta(seconds=window_seconds)
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """
                INSERT INTO gateway_rate_limits (scope_key, window_started_at, request_count, expires_at)
                VALUES (%s, %s, 1, %s)
                ON CONFLICT (scope_key, window_started_at)
                DO UPDATE SET
                    request_count = gateway_rate_limits.request_count + 1,
                    expires_at = EXCLUDED.expires_at,
                    updated_at = now()
                RETURNING request_count
                """,
                (
                    scope_key,
                    window_started_at,
                    window_ends_at,
                ),
            )
            row = await cur.fetchone()
        assert row is not None
        request_count = int(row["request_count"])
        retry_after_seconds = max(1, int((window_ends_at - effective_now).total_seconds()))
        return {
            "allowed": request_count <= max(1, limit),
            "request_count": request_count,
            "retry_after_seconds": retry_after_seconds,
        }

    async def cleanup_expired_rate_limits(self) -> int:
        if not self._pool:
            return 0
        async with self._pool.connection() as conn:
            cur = await conn.execute("DELETE FROM gateway_rate_limits WHERE expires_at < now()")
            return max(0, cur.rowcount)
