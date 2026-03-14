# MCP Gateway

`mcp-gateway` is an HTTP MCP proxy that gives clients one endpoint while routing to many upstream MCP servers.

It is designed for shared deployments with policy controls, caching, and observability.

<img src="docs/mcp-gateway-architecture.png" alt="MCP Gateway Architecture" width="75%">

<img src="docs/mcp-gateway-runtime-flow.png" alt="MCP Gateway Runtime Flow" width="75%">

Discovery requests such as `initialize` and `tools/list` fan out across upstream MCPs and the gateway merges the results.
`tools/call` requests route to one selected upstream, then the gateway applies caching, logging, and policy checks before returning the response.

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
export MCP_GATEWAY_API_KEY='change-me'
export NOTION_TOKEN='ntn_***'
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
    NOTION_TOKEN: "${NOTION_TOKEN}"
```

`config.yaml` supports explicit env refs for string values:

- `${NAME}` requires the environment variable to be set.
- `${NAME:-default}` uses `default` when the variable is unset or empty.

## Client Setup

Point your MCP client to `/mcp` and include bearer auth.
By default the gateway refuses to start without `gateway.api_key`; set `gateway.allow_unauthenticated: true` only when you intentionally want an open deployment.

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

Apache-2.0 (see [`LICENSE`](LICENSE)).

Contributions submitted to this repository are accepted under the repository's Apache-2.0 license unless explicitly stated otherwise.
