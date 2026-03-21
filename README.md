# MCP Gateway

`mcp-gateway` is an MCP aggregation proxy for teams that want one MCP endpoint in front of many upstream MCP servers.

It is intended for operators and platform engineers who need to:

- expose one stable MCP endpoint to clients
- route requests to multiple upstream MCP servers
- enforce access control and per-upstream deny rules
- share cache and audit state across replicas
- observe tool usage and upstream health

The gateway speaks MCP over `POST /mcp` and currently supports MCP protocol version `2025-11-25` only.

<img src="docs/mcp-gateway-architecture.svg" alt="MCP Gateway Architecture" width="75%">

<img src="docs/mcp-gateway-runtime-flow.svg" alt="MCP Gateway Runtime Flow" width="75%">

## What It Does

For MCP clients:

- presents one MCP endpoint at `POST /mcp`
- fans out discovery requests such as `initialize`, `tools/list`, `resources/list`, and `prompts/list`
- routes `tools/call` to one upstream based on tool ownership

For operators:

- supports `stdio` and remote `streamable_http` upstreams
- stores audit logs, shared cache entries, and shared rate-limit state in Postgres
- exposes Prometheus metrics at `GET /metrics`
- supports single shared bearer auth or Postgres-backed API keys with RBAC

## Prerequisites

Before deploying, make sure you have:

- Python 3.11+ available for the gateway process
- a reachable Postgres database
- credentials or tokens for each upstream MCP you want to expose
- runtime dependencies for any `stdio` upstreams such as `npx`, `uvx`, or vendor CLIs

## Quick Start

1. Copy [`config.example.yaml`](config.example.yaml) to your deployment config path.
2. Set secrets and tokens with environment variables instead of committing them into the config file.
3. Apply the schema:

```bash
psql "$DATABASE_URL" -f schema.sql
```

4. Install and start the gateway:

```bash
pip install .
export MCP_GATEWAY_API_KEY='change-me'
export DATABASE_URL='postgresql://postgres:postgres@localhost:5432/mcp_gateway'
mcp-gateway serve --config /path/to/config.yaml
```

5. Verify the service:

```bash
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
curl -H 'Authorization: Bearer change-me' http://localhost:8080/tools
```

## Minimal Config Example

```yaml
gateway:
  auth_mode: "single_shared"
  api_key: "${MCP_GATEWAY_API_KEY}"

logging:
  stdout_json: true

cache:
  enabled: true
  default_ttl_minutes: 60

upstreams:
  - id: "context7"
    name: "Context7 MCP"
    transport: "stdio"
    command: "npx"
    args:
      - "-y"
      - "@upstash/context7-mcp"

  - id: "github"
    name: "GitHub MCP"
    transport: "streamable_http"
    endpoint: "https://api.githubcopilot.com/mcp/"
    bearer_token_env_var: "GITHUB_PAT_TOKEN"
    timeout_ms: 30000
```

`config.yaml` supports explicit env interpolation:

- `${NAME}` requires the environment variable to be set
- `${NAME:-default}` uses `default` when the variable is unset or empty

This example only shows the smallest useful setup. The gateway has additional optional settings and sensible defaults; see [docs/configuration.md](docs/configuration.md) for the full configuration surface.

## Guide Map

- Deployment guide: [docs/deployment-guide.md](docs/deployment-guide.md)
- Client configuration: [docs/client-configuration.md](docs/client-configuration.md)
- Operations guide: [docs/operations.md](docs/operations.md)
- Configuration reference: [docs/configuration.md](docs/configuration.md)
- RBAC onboarding: [docs/rbac-onboarding.md](docs/rbac-onboarding.md)
- Development and testing: [docs/development.md](docs/development.md)
- Database schema: [schema.sql](schema.sql)
- Postman collection: [docs/postman/mcp-gateway.postman_collection.json](docs/postman/mcp-gateway.postman_collection.json)
