from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID, uuid4

from .errors import NotFoundError
from .postgres_serialization import sanitize_role, serialize_api_key_row, serialize_user_row


class PostgresIdentityMixin:
    async def _user_exists(self, conn, user_id: str) -> bool:
        cur = await conn.execute("SELECT 1 FROM gateway_users WHERE id = %s", (user_id,))
        return await cur.fetchone() is not None

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
            return serialize_user_row(row) if row else None

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
            return serialize_user_row(row) if row else None

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
            return serialize_user_row(row) if row else None

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
            return [serialize_user_row(row) for row in rows]

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
            return serialize_user_row(row) if row else None

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
            return serialize_user_row(row)

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
            return serialize_user_row(row) if row else None

    async def list_identities(self) -> list[Dict[str, Any]]:
        return await self.list_users()

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
            return [serialize_api_key_row(row) for row in rows]

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
            return serialize_api_key_row(row)

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
            return serialize_api_key_row(row) if row else None

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
                "role": sanitize_role(user_row.get("role")),
                "api_key_id": str(api_key_id),
                "key_name": key_name,
                "expires_at": expires_at.isoformat() if expires_at is not None else None,
            }
