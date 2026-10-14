from pathlib import Path
from uuid import uuid4

from psycopg import connect, sql

SCHEMA_SQL = Path(__file__).resolve().parents[1] / "schema.sql"


def test_audit_schema_is_repeatable_and_indexes_cascade_children(isolated_database_dsn) -> None:
    with connect(isolated_database_dsn) as conn:
        schema = SCHEMA_SQL.read_text(encoding="utf-8")
        conn.execute(schema)
        conn.execute(schema)

        indexes = conn.execute(
            """
            SELECT table_class.relname, index_class.relname,
                   pg_get_indexdef(index_class.oid, 1, true)
            FROM pg_index AS index_info
            JOIN pg_class AS table_class ON table_class.oid = index_info.indrelid
            JOIN pg_class AS index_class ON index_class.oid = index_info.indexrelid
            JOIN pg_namespace AS namespace ON namespace.oid = table_class.relnamespace
            WHERE namespace.nspname = current_schema()
              AND index_class.relname IN ('idx_mcp_responses_request_id', 'idx_mcp_denials_request_id')
              AND index_info.indisvalid AND index_info.indisready
              AND index_info.indnkeyatts = 1
              AND index_info.indpred IS NULL
            """
        ).fetchall()
        assert set(indexes) == {
            ("mcp_responses", "idx_mcp_responses_request_id", "request_id"),
            ("mcp_denials", "idx_mcp_denials_request_id", "request_id"),
        }

        deleted_request_id, retained_request_id = uuid4(), uuid4()
        for request_id in (deleted_request_id, retained_request_id):
            conn.execute("INSERT INTO mcp_requests (id, method) VALUES (%s, %s)", (request_id, "tools/call"))
            for _ in range(2):
                conn.execute(
                    """
                    INSERT INTO mcp_responses (id, request_id, success, latency_ms, cache_hit)
                    VALUES (%s, %s, true, 1, false)
                    """,
                    (uuid4(), request_id),
                )
                conn.execute(
                    "INSERT INTO mcp_denials (id, request_id, reason) VALUES (%s, %s, %s)",
                    (uuid4(), request_id, "denied"),
                )

        conn.execute("DELETE FROM mcp_requests WHERE id = %s", (deleted_request_id,))
        assert conn.execute("SELECT id FROM mcp_requests").fetchall() == [(retained_request_id,)]
        for table in ("mcp_responses", "mcp_denials"):
            remaining = conn.execute(
                sql.SQL("SELECT request_id FROM {}").format(sql.Identifier(table))
            ).fetchall()
            assert remaining == [(retained_request_id,), (retained_request_id,)]
