from contextlib import contextmanager
from uuid import uuid4

from psycopg import connect, sql
from psycopg.conninfo import make_conninfo


@contextmanager
def isolated_database(dsn: str):
    """Use only a fresh schema, even when the supplied DSN targets a live database."""
    schema_name = f"test_gateway_{uuid4().hex}"
    with connect(dsn, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))
        try:
            yield make_conninfo(dsn, options=f"-c search_path={schema_name}")
        finally:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema_name)))
