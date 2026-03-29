# Deployment Guide

This guide is for operators who want to deploy `mcp-gateway` in front of one or more upstream MCP servers.

## Deployment Model

The intended production shape is:

1. one or more gateway replicas
2. one shared Postgres database
3. zero or more local `stdio` upstreams per replica
4. zero or more remote `streamable_http` upstreams
5. MCP clients pointed at the gateway instead of individual upstreams

Postgres is the shared state backend for:

- request and response audit rows
- denials
- shared cache entries
- Postgres-backed API keys and RBAC state
- shared rate limiting

## Prerequisites

Before deploying, make sure you have:

- Python 3.11+ available for the gateway process
- a reachable Postgres database
- credentials or tokens for each upstream MCP you want to expose
- runtime dependencies for any `stdio` upstreams such as `npx`, `uvx`, or vendor CLIs

## Quick Start

1. Start from [`config.example.yaml`](../config.example.yaml).
2. For deployment-specific use, copy it to `config.yaml` and replace the getting-started defaults.
3. Apply the schema:

```bash
psql "$DATABASE_URL" -f schema.sql
```

4. Install and start the gateway:

```bash
pip install .
cp config.example.yaml config.yaml
export MCP_GATEWAY_API_KEY='change-me'
export DATABASE_URL='postgresql://postgres:postgres@localhost:5432/mcp_gateway'
mcp-gateway serve --config ./config.yaml
```

If a `.env` file is present in the working directory, `mcp-gateway` loads it automatically at startup.

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
  default_ttl_minutes: 60

upstreams:
  - id: "context7"
    transport: "stdio"
    command: "npx"
    args:
      - "-y"
      - "@upstash/context7-mcp"
    env: {}

  - id: "github"
    transport: "streamable_http"
    endpoint: "https://api.githubcopilot.com/mcp/"
    bearer_token_env_var: "GITHUB_PAT_TOKEN"
```

`config.yaml` supports explicit env interpolation:

- `${NAME}` requires the environment variable to be set
- `${NAME:-default}` uses `default` when the variable is unset or empty

This example only shows the smallest useful setup. The checked-in example config enables `context7` by default and keeps broad admin HTTP off by default. See [`docs/configuration.md`](./configuration.md) for the full configuration surface and defaults.

## Authentication Modes

### `single_shared`

Use this when one bearer token is enough for the deployment.

Characteristics:

- easiest mode to deploy
- all authenticated callers are effectively full-access gateway users
- good for a single operator or a trusted internal client

### `postgres_api_keys`

Use this when multiple users need separate API keys and access control.

Characteristics:

- users authenticate with Postgres-backed API keys
- `admin` retains full platform access
- standard users authenticate successfully but require RBAC grants for tool execution and delegated operator workflows
- supports break-glass access through `gateway.bootstrap_admin_api_key`

To seed the first admin key:

```bash
DATABASE_URL='postgresql://postgres:postgres@localhost:5432/mcp_gateway' \
  mcp-gateway create-api-key \
  --config /path/to/config.yaml \
  --subject alice \
  --display-name "Alice" \
  --role admin \
  --key-name default
```

For standard users, omit `--role` and grant access through groups and integration or platform grants.

## Endpoints

### Core endpoints

- `POST /mcp` MCP transport endpoint
- `GET /tools` lightweight tool catalog
- `GET /metrics` Prometheus/OpenMetrics scrape endpoint
- `GET /healthz` liveness endpoint
- `GET /readyz` readiness endpoint

### Management endpoints

Always available self-service endpoints:

- `GET /v1/me`
- `GET /v1/me/api-keys`
- `POST /v1/me/api-keys`
- `DELETE /v1/me/api-keys/{key_id}`

Operator workflows use the CLI rather than an HTTP admin control plane:

- `mcp-gateway validate-config --config ./config.yaml`
- `mcp-gateway warmup-check --config ./config.yaml`
- `mcp-gateway list-integrations --config ./config.yaml`
- `mcp-gateway create-user --config ./config.yaml --subject alice --display-name Alice --issue-api-key`
- `mcp-gateway create-group --config ./config.yaml --name sales --description "Sales team"`
- `mcp-gateway add-group-member --config ./config.yaml --group-id <group-id> --subject alice`
- `mcp-gateway grant-integration --config ./config.yaml --group-id <group-id> --upstream-id jira`
- `mcp-gateway grant-platform --config ./config.yaml --group-id <group-id> --permission admin.usage.read`

## Upstream Configuration Guidance

### `stdio` upstreams

Use `stdio` when the MCP server is packaged as a local process or CLI.

Recommended pattern:

```yaml
- id: "context7"
  transport: "stdio"
  command: "npx"
  args:
    - "-y"
    - "@upstash/context7-mcp"
  env: {}
```

Notes:

- prefer `command` plus `args` instead of shell wrappers
- keep secrets in `env`, not inline in arguments
- make sure the runtime dependency exists on every replica

### `streamable_http` upstreams

Use `streamable_http` when the upstream is already exposed over MCP HTTP.

Recommended pattern:

```yaml
- id: "github"
  transport: "streamable_http"
  endpoint: "https://api.githubcopilot.com/mcp/"
  bearer_token_env_var: "GITHUB_PAT_TOKEN"
```

Notes:

- the gateway expects MCP Streamable HTTP semantics
- if you need custom static headers, use `http_headers`
- if the upstream requires serialized requests, set `http_serialize_requests: true`

## OpenTelemetry Tracing

Tracing is optional and uses standard OTEL environment variables. A common setup looks like:

```bash
export OTEL_TRACES_EXPORTER=otlp
export OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
export OTEL_SERVICE_NAME=mcp-gateway
```

When enabled, the gateway emits spans for inbound HTTP requests, MCP request handling, and outbound upstream calls.

## Docker

```bash
docker compose up --build
```

Default local endpoints:

- Gateway: `http://localhost:8080`
- Postgres: `postgresql://postgres:postgres@localhost:5432/mcp_gateway`
