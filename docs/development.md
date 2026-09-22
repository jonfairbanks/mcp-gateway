# Development and Testing

## Local Development

Use the editable dev install so pytest has runtime dependencies such as `psycopg` as well as test tools:

```bash
pip install -e ".[dev]"
pytest
```

## Integration Tests

To run the Postgres-backed integration tests:

```bash
docker compose up -d postgres
export DATABASE_URL='postgresql://postgres:postgres@localhost:5432/mcp_gateway'
pytest tests/test_integration_app.py
```

`MCP_GATEWAY_TEST_DATABASE_URL` takes precedence over the `DATABASE_URL` fallback. Each database test creates a fresh `test_gateway_<uuid>` schema and uses it as the only search path. Setup never truncates existing tables. Cleanup drops only that test's schema. The database role needs schema creation permission; use a disposable test database to avoid test load on a live server.

## Useful References

- Database schema: [`schema.sql`](../schema.sql)
