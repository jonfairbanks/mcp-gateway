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
export MCP_GATEWAY_TEST_DATABASE_URL='postgresql://postgres:postgres@localhost:5432/mcp_gateway'
pytest tests/test_integration_app.py
```

## Useful References

- Database schema: [`schema.sql`](/Users/jonfairbanks/Documents/GitHub/mcp-gateway/schema.sql)
- Postman collection: [`docs/postman/mcp-gateway.postman_collection.json`](/Users/jonfairbanks/Documents/GitHub/mcp-gateway/docs/postman/mcp-gateway.postman_collection.json)
