from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from psycopg import AsyncConnection

from mcp_gateway.postgres import PostgresStore

SCHEMA_SQL = (Path(__file__).parents[1] / "schema.sql").read_text(encoding="utf-8")
MIGRATION_SQL = (Path(__file__).parents[1] / "migrations" / "001_owner_api_keys.sql").read_text(encoding="utf-8")


async def _execute(scoped_dsn: str, statement: str, params: tuple[object, ...] = ()) -> None:
    async with await AsyncConnection.connect(scoped_dsn, autocommit=True) as conn:
        await conn.execute(statement, params)


async def _fetchone(scoped_dsn: str, statement: str, params: tuple[object, ...] = ()) -> tuple[object, ...]:
    async with await AsyncConnection.connect(scoped_dsn, autocommit=True) as conn:
        cur = await conn.execute(statement, params)
        row = await cur.fetchone()
        assert row is not None
        return row


def test_fresh_schema_has_only_owner_key_storage_and_migration_is_a_noop(isolated_database_dsn) -> None:
    async def run(scoped_dsn: str) -> None:
        await _execute(scoped_dsn, SCHEMA_SQL)
        old_tables = await _fetchone(
            scoped_dsn,
            """
            SELECT
                to_regclass('gateway_users'),
                to_regclass('gateway_api_keys'),
                to_regclass('gateway_groups'),
                to_regclass('gateway_group_memberships'),
                to_regclass('gateway_policy_state')
            """,
        )
        assert old_tables == (None, None, None, None, None)
        assert await _fetchone(scoped_dsn, "SELECT to_regclass('gateway_access_keys')") == ("gateway_access_keys",)

        request_columns = await _fetchone(
            scoped_dsn,
            """
            SELECT array_agg(column_name ORDER BY column_name)
            FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = 'mcp_requests'
            """,
        )
        assert not {
            "auth_user_id",
            "auth_role",
            "auth_group_names",
            "authorized_upstream_id",
        }.intersection(request_columns[0])

        await _execute(scoped_dsn, MIGRATION_SQL)
        assert await _fetchone(scoped_dsn, "SELECT count(*) FROM gateway_access_keys") == (0,)

    asyncio.run(run(isolated_database_dsn))


def test_owner_keys_support_lookup_usage_metadata_and_revocation(isolated_database_dsn) -> None:
    async def run(scoped_dsn: str) -> None:
        await _execute(scoped_dsn, SCHEMA_SQL)
        store = PostgresStore(scoped_dsn)
        await store.start()
        try:
            api_key_id = uuid4()
            issued = await store.issue_api_key(
                api_key_id=api_key_id,
                key_name="laptop",
                key_prefix="owner-key-01",
                key_hash="secret-hash",
                expires_at=None,
            )
            assert issued["api_key_id"] == str(api_key_id)
            assert "id" not in issued
            assert "key_hash" not in issued

            found = await store.find_api_key("owner-key-01")
            assert found is not None
            assert set(found) == {"id", "key_hash", "key_name"}
            assert found["id"] == api_key_id
            assert found["key_hash"] == "secret-hash"

            await store.touch_api_key_last_used(api_key_id)
            listed = await store.list_api_keys()
            assert len(listed) == 1
            assert listed[0]["last_used_at"] is not None
            assert "key_hash" not in listed[0]

            revoked = await store.revoke_api_key(api_key_id)
            assert revoked is not None
            assert revoked["is_active"] is False
            assert revoked["revoked_at"] is not None
            assert "key_hash" not in revoked
            assert await store.find_api_key("owner-key-01") is None
        finally:
            await store.close()

    asyncio.run(run(isolated_database_dsn))


def test_migration_imports_only_active_admin_keys_without_touching_legacy_rows(isolated_database_dsn) -> None:
    async def run(scoped_dsn: str) -> None:
        await _execute(
            scoped_dsn,
            """
            CREATE TABLE gateway_users (
                id UUID PRIMARY KEY,
                role TEXT,
                is_active BOOLEAN NOT NULL
            );
            CREATE TABLE gateway_api_keys (
                id UUID PRIMARY KEY,
                user_id UUID NOT NULL REFERENCES gateway_users(id),
                key_name TEXT NOT NULL,
                key_prefix TEXT NOT NULL UNIQUE,
                key_hash TEXT NOT NULL,
                is_active BOOLEAN NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                last_used_at TIMESTAMPTZ,
                expires_at TIMESTAMPTZ,
                revoked_at TIMESTAMPTZ
            )
            """,
        )
        active_admin_id, inactive_admin_id, standard_user_id, disabled_admin_id = (uuid4() for _ in range(4))
        async with await AsyncConnection.connect(scoped_dsn, autocommit=True) as conn:
            async with conn.cursor() as cur:
                await cur.executemany(
                    "INSERT INTO gateway_users (id, role, is_active) VALUES (%s, %s, %s)",
                    [
                        (active_admin_id, " Admin ", True),
                        (inactive_admin_id, "admin", True),
                        (standard_user_id, "member", True),
                        (disabled_admin_id, "admin", False),
                    ],
                )

                now = datetime.now(timezone.utc)
                eligible_key_id = uuid4()
                legacy_keys = [
                    (eligible_key_id, active_admin_id, "eligible", "eligible-prefix", "eligible-hash", True, None, None),
                    (uuid4(), active_admin_id, "inactive", "inactive-prefix", "inactive-hash", False, None, None),
                    (uuid4(), inactive_admin_id, "expired", "expired-prefix", "expired-hash", True, now - timedelta(minutes=1), None),
                    (uuid4(), inactive_admin_id, "revoked", "revoked-prefix", "revoked-hash", True, None, now),
                    (uuid4(), standard_user_id, "member", "member-prefix", "member-hash", True, None, None),
                    (uuid4(), disabled_admin_id, "disabled", "disabled-prefix", "disabled-hash", True, None, None),
                ]
                await cur.executemany(
                    """
                    INSERT INTO gateway_api_keys (
                        id, user_id, key_name, key_prefix, key_hash, is_active, expires_at, revoked_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    legacy_keys,
                )

        before = await _fetchone(
            scoped_dsn,
            "SELECT array_agg(to_jsonb(gateway_api_keys)::text ORDER BY id) FROM gateway_api_keys",
        )
        await _execute(scoped_dsn, MIGRATION_SQL)
        assert await _fetchone(scoped_dsn, "SELECT count(*) FROM gateway_access_keys") == (1,)
        imported = await _fetchone(
            scoped_dsn,
            "SELECT id, key_name, key_prefix, key_hash, is_active, revoked_at FROM gateway_access_keys",
        )
        assert imported == (eligible_key_id, "eligible", "eligible-prefix", "eligible-hash", True, None)
        assert (
            await _fetchone(
                scoped_dsn,
                "SELECT array_agg(to_jsonb(gateway_api_keys)::text ORDER BY id) FROM gateway_api_keys",
            )
            == before
        )

        await _execute(
            scoped_dsn,
            "UPDATE gateway_access_keys SET is_active = FALSE, revoked_at = now() WHERE id = %s",
            (eligible_key_id,),
        )
        await _execute(scoped_dsn, MIGRATION_SQL)
        assert await _fetchone(scoped_dsn, "SELECT is_active, revoked_at IS NOT NULL FROM gateway_access_keys") == (False, True)

    asyncio.run(run(isolated_database_dsn))


@pytest.mark.parametrize("state", ["expired", "inactive", "revoked"])
def test_unusable_owner_keys_cannot_authenticate(state: str, isolated_database_dsn) -> None:
    async def run(scoped_dsn: str) -> None:
        await _execute(scoped_dsn, SCHEMA_SQL)
        store = PostgresStore(scoped_dsn)
        await store.start()
        try:
            api_key_id = uuid4()
            await store.issue_api_key(
                api_key_id=api_key_id, key_name=state, key_prefix="test-prefix", key_hash="test-hash", expires_at=None
            )
            assert await store.find_api_key("test-prefix") is not None
            updates = {
                "expired": "expires_at = now() - interval '1 minute'",
                "inactive": "is_active = FALSE",
                "revoked": "revoked_at = now()",
            }
            await _execute(scoped_dsn, f"UPDATE gateway_access_keys SET {updates[state]} WHERE id = %s", (api_key_id,))
            assert await store.find_api_key("test-prefix") is None
        finally:
            await store.close()
    asyncio.run(run(isolated_database_dsn))
