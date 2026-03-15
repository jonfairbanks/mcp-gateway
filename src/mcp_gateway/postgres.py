from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from uuid import UUID, uuid4

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
        auth_user_id: Optional[str],
        auth_api_key_id: Optional[str],
        auth_role: Optional[str],
        cache_key: Optional[str],
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
                    cache_key
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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

    @staticmethod
    def _serialize_user_row(row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": str(row["id"]),
            "subject": row["subject"],
            "display_name": row["display_name"],
            "role": row["role"],
            "is_active": row["is_active"],
            "created_at": row["created_at"].isoformat() if row.get("created_at") is not None else None,
            "updated_at": row["updated_at"].isoformat() if row.get("updated_at") is not None else None,
        }

    @staticmethod
    def _serialize_api_key_row(row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "api_key_id": str(row["id"]),
            "user_id": str(row["user_id"]),
            "key_name": row["key_name"],
            "key_prefix": row["key_prefix"],
            "is_active": row["is_active"],
            "created_at": row["created_at"].isoformat() if row.get("created_at") is not None else None,
            "last_used_at": row["last_used_at"].isoformat() if row.get("last_used_at") is not None else None,
            "expires_at": row["expires_at"].isoformat() if row.get("expires_at") is not None else None,
            "revoked_at": row["revoked_at"].isoformat() if row.get("revoked_at") is not None else None,
        }

    @staticmethod
    def _serialize_usage_row(row: Dict[str, Any], group_by: str) -> Dict[str, Any]:
        payload = {
            "request_count": row["request_count"],
            "tool_call_count": row["tool_call_count"],
            "success_count": row["success_count"],
            "denial_count": row["denial_count"],
            "cache_hit_count": row["cache_hit_count"],
            "last_used_at": row["last_used_at"].isoformat() if row.get("last_used_at") is not None else None,
        }
        if group_by == "user":
            payload.update(
                {
                    "user_id": str(row["user_id"]),
                    "subject": row["subject"],
                    "display_name": row["display_name"],
                    "role": row["role"],
                }
            )
        else:
            payload.update(
                {
                    "api_key_id": str(row["api_key_id"]),
                    "user_id": str(row["user_id"]),
                    "subject": row["subject"],
                    "display_name": row["display_name"],
                    "role": row["role"],
                    "key_name": row["key_name"],
                    "key_prefix": row["key_prefix"],
                }
            )
        return payload

    async def find_api_key_identity(self, key_prefix: str) -> Optional[Dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """
                SELECT
                    k.id AS api_key_id,
                    k.key_hash,
                    u.id AS user_id,
                    u.subject,
                    u.display_name,
                    u.role
                FROM gateway_api_keys AS k
                JOIN gateway_users AS u
                  ON u.id = k.user_id
                WHERE k.key_prefix = %s
                  AND k.is_active = TRUE
                  AND k.revoked_at IS NULL
                  AND (k.expires_at IS NULL OR k.expires_at > now())
                  AND u.is_active = TRUE
                """,
                (key_prefix,),
            )
            return await cur.fetchone()

    async def touch_api_key_last_used(self, api_key_id: str) -> None:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        async with self._pool.connection() as conn:
            await conn.execute(
                "UPDATE gateway_api_keys SET last_used_at = now() WHERE id = %s",
                (api_key_id,),
            )

    async def get_user_by_id(self, user_id: str) -> Optional[Dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """
                SELECT id, subject, display_name, role, is_active, created_at, updated_at
                FROM gateway_users
                WHERE id = %s
                """,
                (user_id,),
            )
            row = await cur.fetchone()
            return self._serialize_user_row(row) if row else None

    async def get_user_by_subject(self, subject: str) -> Optional[Dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """
                SELECT id, subject, display_name, role, is_active, created_at, updated_at
                FROM gateway_users
                WHERE subject = %s
                """,
                (subject,),
            )
            row = await cur.fetchone()
            return self._serialize_user_row(row) if row else None

    async def create_user(self, *, subject: str, display_name: str, role: str) -> Optional[Dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """
                INSERT INTO gateway_users (id, subject, display_name, role)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (subject) DO NOTHING
                RETURNING id, subject, display_name, role, is_active, created_at, updated_at
                """,
                (uuid4(), subject, display_name, role),
            )
            row = await cur.fetchone()
            return self._serialize_user_row(row) if row else None

    async def list_users(self) -> list[Dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """
                SELECT id, subject, display_name, role, is_active, created_at, updated_at
                FROM gateway_users
                ORDER BY created_at ASC, subject ASC
                """
            )
            rows = await cur.fetchall()
            return [self._serialize_user_row(row) for row in rows]

    async def update_user(
        self,
        user_id: str,
        *,
        display_name: Optional[str] = None,
        role: Optional[str] = None,
        is_active: Optional[bool] = None,
    ) -> Optional[Dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """
                UPDATE gateway_users
                SET
                    display_name = COALESCE(%s, display_name),
                    role = COALESCE(%s, role),
                    is_active = COALESCE(%s, is_active),
                    updated_at = now()
                WHERE id = %s
                RETURNING id, subject, display_name, role, is_active, created_at, updated_at
                """,
                (display_name, role, is_active, user_id),
            )
            row = await cur.fetchone()
            return self._serialize_user_row(row) if row else None

    async def list_api_keys(self, *, user_id: str) -> list[Dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """
                SELECT id, user_id, key_name, key_prefix, is_active, created_at, last_used_at, expires_at, revoked_at
                FROM gateway_api_keys
                WHERE user_id = %s
                ORDER BY created_at DESC, key_name ASC
                """,
                (user_id,),
            )
            rows = await cur.fetchall()
            return [self._serialize_api_key_row(row) for row in rows]

    async def issue_api_key_for_user(
        self,
        *,
        user_id: str,
        api_key_id: UUID,
        key_name: str,
        key_prefix: str,
        key_hash: str,
        expires_at: Optional[datetime],
    ) -> Dict[str, Any]:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """
                INSERT INTO gateway_api_keys (id, user_id, key_name, key_prefix, key_hash, expires_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id, user_id, key_name, key_prefix, is_active, created_at, last_used_at, expires_at, revoked_at
                """,
                (api_key_id, user_id, key_name, key_prefix, key_hash, expires_at),
            )
            row = await cur.fetchone()
            assert row is not None
            return self._serialize_api_key_row(row)

    async def revoke_api_key(self, api_key_id: str, *, user_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        where_sql = "id = %s"
        params: list[Any] = [api_key_id]
        if user_id is not None:
            where_sql += " AND user_id = %s"
            params.append(user_id)
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                f"""
                UPDATE gateway_api_keys
                SET is_active = FALSE, revoked_at = COALESCE(revoked_at, now())
                WHERE {where_sql}
                RETURNING id, user_id, key_name, key_prefix, is_active, created_at, last_used_at, expires_at, revoked_at
                """,
                tuple(params),
            )
            row = await cur.fetchone()
            return self._serialize_api_key_row(row) if row else None

    async def usage_summary(
        self,
        *,
        group_by: str,
        from_timestamp: Optional[datetime] = None,
        to_timestamp: Optional[datetime] = None,
    ) -> list[Dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        if group_by not in {"user", "api_key"}:
            raise ValueError("group_by must be one of: user, api_key")

        if group_by == "user":
            select_identity = """
                summary.auth_user_id AS user_id,
                users.subject,
                users.display_name,
                summary.auth_role AS role
            """
            join_identity = "LEFT JOIN gateway_users AS users ON users.id = summary.auth_user_id"
            group_identity = "summary.auth_user_id, users.subject, users.display_name, summary.auth_role"
            order_identity = "request_count DESC, last_used_at DESC, users.subject ASC"
            null_filter = "summary.auth_user_id IS NOT NULL"
        else:
            select_identity = """
                summary.auth_api_key_id AS api_key_id,
                summary.auth_user_id AS user_id,
                users.subject,
                users.display_name,
                summary.auth_role AS role,
                keys.key_name,
                keys.key_prefix
            """
            join_identity = """
                LEFT JOIN gateway_users AS users ON users.id = summary.auth_user_id
                LEFT JOIN gateway_api_keys AS keys ON keys.id = summary.auth_api_key_id
            """
            group_identity = """
                summary.auth_api_key_id,
                summary.auth_user_id,
                users.subject,
                users.display_name,
                summary.auth_role,
                keys.key_name,
                keys.key_prefix
            """
            order_identity = "request_count DESC, last_used_at DESC, keys.key_name ASC"
            null_filter = "summary.auth_api_key_id IS NOT NULL"

        async with self._pool.connection() as conn:
            cur = await conn.execute(
                f"""
                WITH summary AS (
                    SELECT
                        req.id,
                        req.timestamp,
                        req.method,
                        req.auth_user_id,
                        req.auth_api_key_id,
                        req.auth_role,
                        EXISTS (
                            SELECT 1
                            FROM mcp_responses AS resp
                            WHERE resp.request_id = req.id
                              AND resp.success = TRUE
                        ) AS success_hit,
                        EXISTS (
                            SELECT 1
                            FROM mcp_responses AS resp
                            WHERE resp.request_id = req.id
                              AND resp.cache_hit = TRUE
                        ) AS cache_hit,
                        EXISTS (
                            SELECT 1
                            FROM mcp_denials AS denial
                            WHERE denial.request_id = req.id
                        ) AS denial_hit
                    FROM mcp_requests AS req
                    WHERE (%s::timestamptz IS NULL OR req.timestamp >= %s)
                      AND (%s::timestamptz IS NULL OR req.timestamp <= %s)
                )
                SELECT
                    {select_identity},
                    COUNT(*)::bigint AS request_count,
                    SUM(CASE WHEN summary.method = 'tools/call' THEN 1 ELSE 0 END)::bigint AS tool_call_count,
                    SUM(CASE WHEN summary.success_hit THEN 1 ELSE 0 END)::bigint AS success_count,
                    SUM(CASE WHEN summary.denial_hit THEN 1 ELSE 0 END)::bigint AS denial_count,
                    SUM(CASE WHEN summary.cache_hit THEN 1 ELSE 0 END)::bigint AS cache_hit_count,
                    MAX(summary.timestamp) AS last_used_at
                FROM summary
                {join_identity}
                WHERE {null_filter}
                GROUP BY {group_identity}
                ORDER BY {order_identity}
                """,
                (from_timestamp, from_timestamp, to_timestamp, to_timestamp),
            )
            rows = await cur.fetchall()
            return [self._serialize_usage_row(row, group_by) for row in rows]

    async def issue_api_key(
        self,
        *,
        user_id: UUID,
        subject: str,
        display_name: str,
        role: str,
        api_key_id: UUID,
        key_name: str,
        key_prefix: str,
        key_hash: str,
        expires_at: Optional[datetime],
    ) -> Dict[str, Any]:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        async with self._pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO gateway_users (id, subject, display_name, role)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (subject)
                DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    role = EXCLUDED.role,
                    updated_at = now()
                """,
                (user_id, subject, display_name, role),
            )
            cur = await conn.execute(
                """
                SELECT id, subject, display_name, role
                FROM gateway_users
                WHERE subject = %s
                """,
                (subject,),
            )
            user_row = await cur.fetchone()
            assert user_row is not None
            await conn.execute(
                """
                INSERT INTO gateway_api_keys (id, user_id, key_name, key_prefix, key_hash, expires_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (api_key_id, user_row["id"], key_name, key_prefix, key_hash, expires_at),
            )
            return {
                "user_id": str(user_row["id"]),
                "subject": user_row["subject"],
                "display_name": user_row["display_name"],
                "role": user_row["role"],
                "api_key_id": str(api_key_id),
                "key_name": key_name,
                "expires_at": expires_at.isoformat() if expires_at is not None else None,
            }
