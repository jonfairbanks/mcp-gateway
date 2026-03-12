# MCP Gateway

`mcp-gateway` is an HTTP MCP proxy that gives clients one endpoint while routing to many upstream MCP servers.

It is designed for shared deployments with policy controls, caching, and observability.

![MCP Gateway Architecture](docs/mcp-gateway-architecture.png)

## Key Features

- Multi-upstream routing (`stdio` and HTTP upstreams).
- Tool discovery aggregation (`tools/list`, `resources/list`, `resources/templates/list`, `prompts/list`).
- Per-upstream deny policies with explicit policy-denied errors.
- Response caching for successful `tools/call`.
- Structured logs + Postgres request/response/denial/cache tables.
- Startup warmup and per-upstream health counters.

## Quick Start

1. Copy and edit [`config.example.yaml`](config.example.yaml).
2. Initialize Postgres schema:

```bash
psql "$DATABASE_URL" -f schema.sql
```

3. Install and run:

```bash
pip install .
DATABASE_URL='postgresql://postgres:postgres@localhost:5432/mcp_gateway' \
  mcp-gateway serve --config /path/to/config.yaml
```

For `stdio` upstreams, prefer `command` + `args`:

```yaml
- id: "notion"
  transport: "stdio"
  command: "npx"
  args: ["-y", "@notionhq/notion-mcp-server"]
  env:
    NOTION_TOKEN: "ntn_***"
```

## Client Setup

Point your MCP client to `/mcp` and include bearer auth.

#### Codex example:

```toml
[mcp_servers.mcp-gateway]
url = "http://localhost:8080/mcp"
http_headers = { "Authorization" = "Bearer change-me" }
```

#### Claude example:

```json
{
  "mcpServers": {
    "mcp-gateway": {
      "url": "http://localhost:8080/mcp",
      "headers": {
        "Authorization": "Bearer change-me"
      }
    }
  }
}
```

## Endpoints

- `POST /mcp` JSON-RPC MCP endpoint.
- `GET /tools` upstream tool catalog (`tools`, `exposed_tools`, `deny_tools`).
- `GET /metrics` Prometheus/OpenMetrics metrics.
- `GET /healthz` liveness + warmup/breaker status.
- `GET /readyz` readiness (`503` until at least one upstream initializes).
- `GET /sse` and `POST /message` for streamable MCP sessions.

## Docker

```bash
docker compose up --build
```

Default local endpoints:

- Gateway: `http://localhost:8080`
- Postgres: `postgresql://postgres:postgres@localhost:5432/mcp_gateway`

## Docs

- Configuration reference: [`docs/configuration.md`](docs/configuration.md)
- Database schema: [`schema.sql`](schema.sql)

## Troubleshooting

- If tools are missing, check `upstream_warmup` and `tools/list` logs.
- If a tool call is blocked, look for JSON-RPC `-32001` with `error.data.category = policy_denied`.
- If auth fails, verify `Authorization: Bearer <api_key>` matches `gateway.api_key`.

## License

MIT (see [`pyproject.toml`](pyproject.toml)).
