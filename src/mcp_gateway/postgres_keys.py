from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from .postgres_serialization import serialize_api_key_row


class PostgresKeyMixin:
    async def find_api_key(self, key_prefix: str) -> Optional[Dict[str, Any]]:
        """Return the minimum material needed to verify a currently usable key."""
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """
                SELECT id, key_hash, key_name
                FROM gateway_access_keys
                WHERE key_prefix = %s
                  AND is_active = TRUE
                  AND revoked_at IS NULL
                  AND (expires_at IS NULL OR expires_at > now())
                """,
                (key_prefix,),
            )
            return await cur.fetchone()

    async def touch_api_key_last_used(self, api_key_id: UUID | str) -> None:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        async with self._pool.connection() as conn:
            await conn.execute(
                "UPDATE gateway_access_keys SET last_used_at = now() WHERE id = %s",
                (api_key_id,),
            )

    async def issue_api_key(
        self,
        *,
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
                INSERT INTO gateway_access_keys (id, key_name, key_prefix, key_hash, expires_at)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id, key_name, key_prefix, is_active, created_at, last_used_at, expires_at, revoked_at
                """,
                (api_key_id, key_name, key_prefix, key_hash, expires_at),
            )
            row = await cur.fetchone()
            assert row is not None
            return serialize_api_key_row(row)

    async def list_api_keys(self) -> list[Dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """
                SELECT id, key_name, key_prefix, is_active, created_at, last_used_at, expires_at, revoked_at
                FROM gateway_access_keys
                ORDER BY created_at DESC, key_name ASC
                """
            )
            rows = await cur.fetchall()
            return [serialize_api_key_row(row) for row in rows]

    async def revoke_api_key(self, api_key_id: UUID | str) -> Optional[Dict[str, Any]]:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """
                UPDATE gateway_access_keys
                SET is_active = FALSE, revoked_at = COALESCE(revoked_at, now())
                WHERE id = %s
                RETURNING id, key_name, key_prefix, is_active, created_at, last_used_at, expires_at, revoked_at
                """,
                (api_key_id,),
            )
            row = await cur.fetchone()
            return serialize_api_key_row(row) if row else None
