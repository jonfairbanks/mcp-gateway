from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from uuid import UUID, uuid4

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from .errors import ConflictError, NotFoundError


def _sanitize_role(role: Any) -> Optional[str]:
    if not isinstance(role, str):
        return None
    normalized = role.strip().lower()
    return "admin" if normalized == "admin" else None


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

    def is_available(self) -> bool:
        return self._pool is not None

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

    async def _ensure_policy_state(self, conn) -> None:
        await conn.execute(
            """
            INSERT INTO gateway_policy_state (singleton_key, policy_revision)
            VALUES ('default', 0)
            ON CONFLICT (singleton_key) DO NOTHING
            """
        )

    async def get_policy_revision(self) -> int:
        if not self._pool:
            return 0
        async with self._pool.connection() as conn:
            await self._ensure_policy_state(conn)
            cur = await conn.execute(
                """
                SELECT policy_revision
                FROM gateway_policy_state
                WHERE singleton_key = 'default'
                """
            )
            row = await cur.fetchone()
            return int(row["policy_revision"]) if row else 0

    async def _bump_policy_revision(self, conn) -> None:
        await self._ensure_policy_state(conn)
        await conn.execute(
            """
            UPDATE gateway_policy_state
            SET policy_revision = policy_revision + 1, updated_at = now()
            WHERE singleton_key = 'default'
            """
        )

    async def _group_exists(self, conn, group_id: str) -> bool:
        cur = await conn.execute("SELECT 1 FROM gateway_groups WHERE id = %s", (group_id,))
        return await cur.fetchone() is not None

    async def _user_exists(self, conn, user_id: str) -> bool:
        cur = await conn.execute("SELECT 1 FROM gateway_users WHERE id = %s", (user_id,))
        return await cur.fetchone() is not None

    @staticmethod
    def _serialize_user_row(row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": str(row["id"]),
            "subject": row["subject"],
            "display_name": row.get("display_name"),
            "role": _sanitize_role(row.get("role")),
            "issuer": row.get("issuer"),
            "email": row.get("email"),
            "auth_source": row.get("auth_source"),
            "is_active": row["is_active"],
            "last_seen_at": row["last_seen_at"].isoformat() if row.get("last_seen_at") is not None else None,
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
    def _serialize_group_row(row: Dict[str, Any], *, is_reserved: bool = False) -> Dict[str, Any]:
        return {
            "id": str(row["id"]),
            "name": row["name"],
            "description": row.get("description"),
            "is_reserved": is_reserved,
            "created_at": row["created_at"].isoformat() if row.get("created_at") is not None else None,
            "updated_at": row["updated_at"].isoformat() if row.get("updated_at") is not None else None,
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
        if group_by == "subject":
            payload.update(
                {
                    "subject": row["subject"],
                    "display_name": row.get("display_name"),
                    "email": row.get("email"),
                    "auth_scheme": row.get("auth_scheme"),
                }
            )
        elif group_by == "integration":
            payload.update(
                {
                    "upstream_id": row["authorized_upstream_id"],
                }
            )
        else:
            payload.update(
                {
                    "api_key_id": str(row["api_key_id"]),
                    "user_id": str(row["user_id"]) if row.get("user_id") is not None else None,
                    "subject": row.get("subject"),
                    "display_name": row.get("display_name"),
                    "role": row.get("role"),
                    "key_name": row.get("key_name"),
                    "key_prefix": row.get("key_prefix"),
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
                SELECT id, subject, display_name, role, issuer, email, auth_source, is_active, last_seen_at, created_at, updated_at
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
                SELECT id, subject, display_name, role, issuer, email, auth_source, is_active, last_seen_at, created_at, updated_at
                FROM gateway_users
                WHERE subject = %s
                """,
                (subject,),
            )
            row = await cur.fetchone()
            return self._serialize_user_row(row) if row else None

    async def create_user(self, *, subject: str, display_name: str, role: Optional[str]) -> Optional[Dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """
                INSERT INTO gateway_users (id, subject, display_name, role, auth_source)
                VALUES (%s, %s, %s, %s, 'legacy_api_key')
                ON CONFLICT (subject) DO NOTHING
                RETURNING id, subject, display_name, role, issuer, email, auth_source, is_active, last_seen_at, created_at, updated_at
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
                SELECT id, subject, display_name, role, issuer, email, auth_source, is_active, last_seen_at, created_at, updated_at
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
        role_provided: bool = False,
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
                    role = CASE WHEN %s THEN %s ELSE role END,
                    is_active = COALESCE(%s, is_active),
                    updated_at = now()
                WHERE id = %s
                RETURNING id, subject, display_name, role, issuer, email, auth_source, is_active, last_seen_at, created_at, updated_at
                """,
                (display_name, role_provided, role, is_active, user_id),
            )
            row = await cur.fetchone()
            return self._serialize_user_row(row) if row else None

    async def put_identity(
        self,
        *,
        subject: str,
        display_name: str,
        email: Optional[str],
        is_active: bool,
    ) -> Dict[str, Any]:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """
                INSERT INTO gateway_users (id, subject, display_name, email, auth_source, is_active)
                VALUES (%s, %s, %s, %s, 'manual', %s)
                ON CONFLICT (subject)
                DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    email = EXCLUDED.email,
                    is_active = EXCLUDED.is_active,
                    updated_at = now()
                RETURNING id, subject, display_name, role, issuer, email, auth_source, is_active, last_seen_at, created_at, updated_at
                """,
                (uuid4(), subject, display_name, email, is_active),
            )
            row = await cur.fetchone()
            assert row is not None
            return self._serialize_user_row(row)

    async def patch_identity(
        self,
        subject: str,
        *,
        display_name: Optional[str] = None,
        email: Optional[str] = None,
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
                    email = COALESCE(%s, email),
                    is_active = COALESCE(%s, is_active),
                    updated_at = now()
                WHERE subject = %s
                RETURNING id, subject, display_name, role, issuer, email, auth_source, is_active, last_seen_at, created_at, updated_at
                """,
                (display_name, email, is_active, subject),
            )
            row = await cur.fetchone()
            return self._serialize_user_row(row) if row else None

    async def list_identities(self) -> list[Dict[str, Any]]:
        return await self.list_users()

    async def list_group_names_for_subject(self, subject: str) -> list[str]:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """
                SELECT g.name
                FROM gateway_groups AS g
                JOIN gateway_group_memberships AS m
                  ON m.group_id = g.id
                JOIN gateway_users AS u
                  ON u.id = m.user_id
                WHERE u.subject = %s
                ORDER BY g.name ASC
                """,
                (subject,),
            )
            rows = await cur.fetchall()
            return [str(row["name"]) for row in rows]

    async def list_groups(self) -> list[Dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """
                SELECT id, name, description, created_at, updated_at
                FROM gateway_groups
                ORDER BY name ASC
                """
            )
            rows = await cur.fetchall()
            return [self._serialize_group_row(row) for row in rows]

    async def create_group(self, *, name: str, description: Optional[str]) -> Optional[Dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """
                INSERT INTO gateway_groups (id, name, description)
                VALUES (%s, %s, %s)
                ON CONFLICT (name) DO NOTHING
                RETURNING id, name, description, created_at, updated_at
                """,
                (uuid4(), name, description),
            )
            row = await cur.fetchone()
            if row is None:
                return None
            await self._bump_policy_revision(conn)
            return self._serialize_group_row(row)

    async def update_group(
        self,
        group_id: str,
        *,
        name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        async with self._pool.connection() as conn:
            if name is not None:
                cur = await conn.execute(
                    """
                    SELECT 1
                    FROM gateway_groups
                    WHERE name = %s AND id <> %s
                    """,
                    (name, group_id),
                )
                if await cur.fetchone() is not None:
                    raise ConflictError("A group with that name already exists.")
            cur = await conn.execute(
                """
                UPDATE gateway_groups
                SET
                    name = COALESCE(%s, name),
                    description = COALESCE(%s, description),
                    updated_at = now()
                WHERE id = %s
                RETURNING id, name, description, created_at, updated_at
                """,
                (name, description, group_id),
            )
            row = await cur.fetchone()
            if row is None:
                return None
            await self._bump_policy_revision(conn)
            return self._serialize_group_row(row)

    async def delete_group(self, group_id: str) -> bool:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        async with self._pool.connection() as conn:
            cur = await conn.execute("DELETE FROM gateway_groups WHERE id = %s", (group_id,))
            deleted = cur.rowcount > 0
            if deleted:
                await self._bump_policy_revision(conn)
            return deleted

    async def _ensure_identity_row(self, conn, subject: str) -> Dict[str, Any]:
        cur = await conn.execute(
            """
            INSERT INTO gateway_users (id, subject, display_name, auth_source)
            VALUES (%s, %s, %s, 'manual')
            ON CONFLICT (subject)
            DO UPDATE SET updated_at = now()
            RETURNING id, subject, display_name, role, issuer, email, auth_source, is_active, last_seen_at, created_at, updated_at
            """,
            (uuid4(), subject, subject),
        )
        row = await cur.fetchone()
        assert row is not None
        return row

    async def add_group_member(self, group_id: str, *, subject: str) -> Dict[str, Any]:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        async with self._pool.connection() as conn:
            if not await self._group_exists(conn, group_id):
                raise NotFoundError("Group not found.")
            user_row = await self._ensure_identity_row(conn, subject)
            await conn.execute(
                """
                INSERT INTO gateway_group_memberships (group_id, user_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                """,
                (group_id, user_row["id"]),
            )
            await self._bump_policy_revision(conn)
            return {"group_id": group_id, "subject": subject}

    async def remove_group_member(self, group_id: str, *, subject: str) -> bool:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """
                DELETE FROM gateway_group_memberships AS m
                USING gateway_users AS u
                WHERE m.group_id = %s
                  AND m.user_id = u.id
                  AND u.subject = %s
                """,
                (group_id, subject),
            )
            deleted = cur.rowcount > 0
            if deleted:
                await self._bump_policy_revision(conn)
            return deleted

    async def list_group_integration_grants(self, group_id: str) -> list[Dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """
                SELECT upstream_id, created_at
                FROM gateway_group_integration_grants
                WHERE group_id = %s
                ORDER BY upstream_id ASC
                """,
                (group_id,),
            )
            rows = await cur.fetchall()
            return [
                {
                    "upstream_id": row["upstream_id"],
                    "created_at": row["created_at"].isoformat() if row.get("created_at") is not None else None,
                }
                for row in rows
            ]

    async def add_group_integration_grant(self, group_id: str, *, upstream_id: str) -> Dict[str, Any]:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        async with self._pool.connection() as conn:
            if not await self._group_exists(conn, group_id):
                raise NotFoundError("Group not found.")
            cur = await conn.execute(
                """
                INSERT INTO gateway_group_integration_grants (group_id, upstream_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                RETURNING upstream_id, created_at
                """,
                (group_id, upstream_id),
            )
            row = await cur.fetchone()
            if row is None:
                cur = await conn.execute(
                    """
                    SELECT upstream_id, created_at
                    FROM gateway_group_integration_grants
                    WHERE group_id = %s AND upstream_id = %s
                    """,
                    (group_id, upstream_id),
                )
                row = await cur.fetchone()
            assert row is not None
            await self._bump_policy_revision(conn)
            return {
                "upstream_id": row["upstream_id"],
                "created_at": row["created_at"].isoformat() if row.get("created_at") is not None else None,
            }

    async def remove_group_integration_grant(self, group_id: str, *, upstream_id: str) -> bool:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """
                DELETE FROM gateway_group_integration_grants
                WHERE group_id = %s AND upstream_id = %s
                """,
                (group_id, upstream_id),
            )
            deleted = cur.rowcount > 0
            if deleted:
                await self._bump_policy_revision(conn)
            return deleted

    async def list_group_platform_grants(self, group_id: str) -> list[Dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """
                SELECT permission, created_at
                FROM gateway_group_platform_grants
                WHERE group_id = %s
                ORDER BY permission ASC
                """,
                (group_id,),
            )
            rows = await cur.fetchall()
            return [
                {
                    "permission": row["permission"],
                    "created_at": row["created_at"].isoformat() if row.get("created_at") is not None else None,
                }
                for row in rows
            ]

    async def add_group_platform_grant(self, group_id: str, *, permission: str) -> Dict[str, Any]:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        async with self._pool.connection() as conn:
            if not await self._group_exists(conn, group_id):
                raise NotFoundError("Group not found.")
            cur = await conn.execute(
                """
                INSERT INTO gateway_group_platform_grants (group_id, permission)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                RETURNING permission, created_at
                """,
                (group_id, permission),
            )
            row = await cur.fetchone()
            if row is None:
                cur = await conn.execute(
                    """
                    SELECT permission, created_at
                    FROM gateway_group_platform_grants
                    WHERE group_id = %s AND permission = %s
                    """,
                    (group_id, permission),
                )
                row = await cur.fetchone()
            assert row is not None
            await self._bump_policy_revision(conn)
            return {
                "permission": row["permission"],
                "created_at": row["created_at"].isoformat() if row.get("created_at") is not None else None,
            }

    async def remove_group_platform_grant(self, group_id: str, *, permission: str) -> bool:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """
                DELETE FROM gateway_group_platform_grants
                WHERE group_id = %s AND permission = %s
                """,
                (group_id, permission),
            )
            deleted = cur.rowcount > 0
            if deleted:
                await self._bump_policy_revision(conn)
            return deleted

    async def list_group_integration_policies(self) -> list[Dict[str, Any]]:
        if not self._pool:
            return []
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """
                SELECT g.name AS group_name, grants.upstream_id
                FROM gateway_group_integration_grants AS grants
                JOIN gateway_groups AS g ON g.id = grants.group_id
                ORDER BY g.name ASC, grants.upstream_id ASC
                """
            )
            return await cur.fetchall()

    async def list_group_platform_policies(self) -> list[Dict[str, Any]]:
        if not self._pool:
            return []
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """
                SELECT g.name AS group_name, grants.permission
                FROM gateway_group_platform_grants AS grants
                JOIN gateway_groups AS g ON g.id = grants.group_id
                ORDER BY g.name ASC, grants.permission ASC
                """
            )
            return await cur.fetchall()

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
            if not await self._user_exists(conn, user_id):
                raise NotFoundError("User not found.")
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
        normalized_group_by = "subject" if group_by == "user" else group_by
        if normalized_group_by not in {"subject", "integration", "api_key"}:
            raise ValueError("group_by must be one of: subject, integration, api_key")

        if normalized_group_by == "subject":
            select_identity = """
                summary.auth_subject AS subject,
                users.display_name,
                users.email,
                summary.auth_scheme
            """
            join_identity = "LEFT JOIN gateway_users AS users ON users.subject = summary.auth_subject"
            group_identity = "summary.auth_subject, users.display_name, users.email, summary.auth_scheme"
            order_identity = "request_count DESC, last_used_at DESC, summary.auth_subject ASC"
            null_filter = "summary.auth_subject IS NOT NULL"
        elif normalized_group_by == "integration":
            select_identity = """
                summary.authorized_upstream_id
            """
            join_identity = ""
            group_identity = "summary.authorized_upstream_id"
            order_identity = "request_count DESC, last_used_at DESC, summary.authorized_upstream_id ASC"
            null_filter = "summary.authorized_upstream_id IS NOT NULL"
        else:
            select_identity = """
                summary.auth_api_key_id AS api_key_id,
                summary.auth_user_id AS user_id,
                summary.auth_subject AS subject,
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
                summary.auth_subject,
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
                        req.auth_subject,
                        req.auth_scheme,
                        req.authorized_upstream_id,
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
            return [self._serialize_usage_row(row, normalized_group_by) for row in rows]

    async def issue_api_key(
        self,
        *,
        user_id: UUID,
        subject: str,
        display_name: str,
        role: Optional[str],
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
                INSERT INTO gateway_users (id, subject, display_name, role, auth_source)
                VALUES (%s, %s, %s, %s, 'legacy_api_key')
                ON CONFLICT (subject)
                DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    role = EXCLUDED.role,
                    auth_source = 'legacy_api_key',
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
                "role": _sanitize_role(user_row.get("role")),
                "api_key_id": str(api_key_id),
                "key_name": key_name,
                "expires_at": expires_at.isoformat() if expires_at is not None else None,
            }
