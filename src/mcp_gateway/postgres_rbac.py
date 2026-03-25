from __future__ import annotations

from typing import Any, Dict, Optional
from uuid import uuid4

from .errors import ConflictError, NotFoundError
from .postgres_serialization import serialize_group_row


class PostgresRBACMixin:
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
            return [serialize_group_row(row) for row in rows]

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
            return serialize_group_row(row)

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
            return serialize_group_row(row)

    async def delete_group(self, group_id: str) -> bool:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        async with self._pool.connection() as conn:
            cur = await conn.execute("DELETE FROM gateway_groups WHERE id = %s", (group_id,))
            deleted = cur.rowcount > 0
            if deleted:
                await self._bump_policy_revision(conn)
            return deleted

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
