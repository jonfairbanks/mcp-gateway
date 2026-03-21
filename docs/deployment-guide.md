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

1. Copy [`config.example.yaml`](/Users/jonfairbanks/Documents/GitHub/mcp-gateway/config.example.yaml) to your deployment config path.
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
  listen_host: "0.0.0.0"
  listen_port: 8080
  auth_mode: "single_shared"
  api_key: "${MCP_GATEWAY_API_KEY}"
  bootstrap_admin_api_key: "${MCP_GATEWAY_BOOTSTRAP_ADMIN_API_KEY:-}"
  allow_unauthenticated: false
  public_tools_catalog: false
  trusted_proxies: ["127.0.0.1", "::1"]
  request_max_bytes: 2097152
  rate_limit_per_minute: 120

logging:
  stdout_json: true
  extra_redact_fields: []

cache:
  enabled: true
  max_entries: 10000
  default_ttl_minutes: 60
  client_scoped_tools: []

upstreams:
  - id: "context7"
    name: "context7"
    transport: "stdio"
    command: "npx"
    args: ["-y", "@upstash/context7-mcp"]
    env: {}

  - id: "github"
    name: "github"
    transport: "streamable_http"
    endpoint: "https://api.githubcopilot.com/mcp/"
    bearer_token_env_var: "GITHUB_PAT_TOKEN"
    timeout_ms: 30000
```

`config.yaml` supports explicit env interpolation:

- `${NAME}` requires the environment variable to be set
- `${NAME:-default}` uses `default` when the variable is unset or empty

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
- standard users authenticate successfully but require RBAC grants for tool execution and delegated admin APIs
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

Available when `gateway.auth_mode` is `postgres_api_keys`:

- `GET /v1/me`
- `GET /v1/me/api-keys`
- `POST /v1/me/api-keys`
- `DELETE /v1/me/api-keys/{key_id}`
- `GET /v1/admin/identities`
- `PUT /v1/admin/identities/{subject}`
- `PATCH /v1/admin/identities/{subject}`
- `GET /v1/admin/users`
- `POST /v1/admin/users`
- `PATCH /v1/admin/users/{user_id}`
- `GET /v1/admin/integrations`
- `GET /v1/admin/groups`
- `POST /v1/admin/groups`
- `PATCH /v1/admin/groups/{group_id}`
- `DELETE /v1/admin/groups/{group_id}`
- `POST /v1/admin/groups/{group_id}/members`
- `DELETE /v1/admin/groups/{group_id}/members/{subject}`
- `GET /v1/admin/groups/{group_id}/integration-grants`
- `POST /v1/admin/groups/{group_id}/integration-grants`
- `DELETE /v1/admin/groups/{group_id}/integration-grants/{upstream_id}`
- `GET /v1/admin/groups/{group_id}/platform-grants`
- `POST /v1/admin/groups/{group_id}/platform-grants`
- `DELETE /v1/admin/groups/{group_id}/platform-grants/{permission}`
- `GET /v1/admin/usage`

## Upstream Configuration Guidance

### `stdio` upstreams

Use `stdio` when the MCP server is packaged as a local process or CLI.

Recommended pattern:

```yaml
- id: "context7"
  transport: "stdio"
  command: "npx"
  args: ["-y", "@upstash/context7-mcp"]
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

## Docker

```bash
docker compose up --build
```

Default local endpoints:

- Gateway: `http://localhost:8080`
- Postgres: `postgresql://postgres:postgres@localhost:5432/mcp_gateway`
