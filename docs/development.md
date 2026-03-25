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

`MCP_GATEWAY_TEST_DATABASE_URL` is still accepted, but `DATABASE_URL` now works as the default fallback for integration tests.

## Useful References

- Database schema: [`schema.sql`](../schema.sql)
