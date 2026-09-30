import asyncio
from pathlib import Path

from psycopg import connect

from mcp_gateway.postgres import PostgresStore
from tests.database_support import isolated_database
from tests.test_integration_app import _prepare_database


def test_app_setup_cannot_modify_tables_in_the_supplied_database_schema(isolated_database_dsn):
    schema_sql = (Path(__file__).parents[1] / "schema.sql").read_text()
    with connect(isolated_database_dsn, autocommit=True) as conn:
        conn.execute(schema_sql)
        conn.execute("INSERT INTO mcp_cache VALUES ('sentinel', '{}', now() + interval '1 day')")
        parent_schema = conn.execute("SELECT current_schema()").fetchone()[0]
        with isolated_database(isolated_database_dsn) as child_dsn:
            async def prepare():
                store = PostgresStore(child_dsn)
                await store.start()
                try:
                    await _prepare_database(store)
                    assert await store.cache_get("sentinel") is None
                    await store.cache_set("child-only", {"result": {}}, 60)
                finally:
                    await store.close()
            asyncio.run(prepare())
            with connect(child_dsn) as child:
                assert child.execute("SELECT current_schema()").fetchone()[0] != parent_schema
                assert child.execute("SELECT count(*) FROM mcp_cache").fetchone()[0] == 1
        assert conn.execute("SELECT cache_key FROM mcp_cache").fetchall() == [("sentinel",)]
