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
- Postgres-backed API keys
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
export MCP_GATEWAY_API_KEY="$(openssl rand -hex 32)"
export DATABASE_URL='postgresql://postgres:postgres@localhost:5432/mcp_gateway'
mcp-gateway serve --config ./config.yaml
```

The image includes Context7 installed from `upstreams/package-lock.json`; startup runs the local `context7-mcp` binary without downloading npm packages. For a source checkout, install it once with `npm ci --prefix upstreams --ignore-scripts` and add `$PWD/upstreams/node_modules/.bin` to `PATH` before starting the gateway. Use Node.js 24 LTS.

If a `.env` file is present in the working directory, `mcp-gateway` loads it automatically at startup.

5. Verify the service:

```bash
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
curl -H "Authorization: Bearer ${MCP_GATEWAY_API_KEY}" http://localhost:8080/tools
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
    command: "context7-mcp"
    env: {}

  - id: "github"
    transport: "streamable_http"
    endpoint: "https://api.githubcopilot.com/mcp/"
    bearer_token_env_var: "GITHUB_PAT_TOKEN"
```

`config.yaml` supports explicit env interpolation:

- `${NAME}` requires the environment variable to be set
- `${NAME:-default}` uses `default` when the variable is unset or empty

This example only shows the smallest useful setup. The checked-in example config enables `context7` by default. See [`docs/configuration.md`](./configuration.md) for the full configuration surface and defaults.

## Authentication Modes

### `single_shared`

Use this when one bearer token is enough for the deployment.

Characteristics:

- easiest mode to deploy
- all authenticated callers have full gateway access
- good for a single operator or a trusted internal client

### `postgres_api_keys`

Use this when callers need separate, independently revocable API keys.

Characteristics:

- callers authenticate with Postgres-backed API keys from `gateway_access_keys`
- every authenticated key has the same gateway access
- per-upstream `deny_tools` remains the only configured tool restriction
- supports break-glass access through `gateway.bootstrap_api_key`
- `gateway.bootstrap_admin_api_key` remains accepted for existing configuration

Create an owner key through the operator CLI:

```bash
DATABASE_URL='postgresql://postgres:postgres@localhost:5432/mcp_gateway' \
  mcp-gateway create-api-key \
  --config /path/to/config.yaml \
  --key-name primary \
  --expires-days 90
```

Store the displayed secret once. The gateway only stores its hash.

## Endpoints

### Core endpoints

- `POST /mcp` MCP transport endpoint
- `GET /tools` lightweight tool catalog
- `GET /metrics` Prometheus/OpenMetrics scrape endpoint
- `GET /healthz` liveness endpoint
- `GET /readyz` readiness endpoint

### Authentication Endpoint

Authenticated identity endpoint:

- `GET /v1/me`

API key lifecycle uses the CLI rather than HTTP endpoints:

- `mcp-gateway validate-config --config ./config.yaml`
- `mcp-gateway warmup-check --config ./config.yaml`
- `mcp-gateway list-integrations --config ./config.yaml`
- `mcp-gateway create-api-key --config ./config.yaml --key-name NAME --expires-days N`
- `mcp-gateway list-api-keys --config ./config.yaml`
- `mcp-gateway revoke-api-key --config ./config.yaml --key-id UUID`

## Auth Migration Rollout

Use the deployment pipeline to roll out the owner-key migration. Do not run the migration as an automatic local startup step.

1. Take a restorable Postgres backup before the pipeline runs the migration.
2. Have the CI deployment stage apply `schema.sql`, then `migrations/001_owner_api_keys.sql`, with `ON_ERROR_STOP` enabled.
3. Deploy the release with `auth_mode: postgres_api_keys` and issue any required owner keys through the CLI.
4. Verify `GET /v1/me` and a normal MCP tool call with an issued key before retiring legacy access.

The migration creates and uses `gateway_access_keys`. It imports only legacy keys that are active, unexpired, unrevoked, and attached to active `admin` users. It preserves those keys' IDs and hashes. Restricted non-admin keys are skipped so the migration cannot expand their tool access. Issue replacement owner keys explicitly for callers that used restricted keys.

Legacy user, group, grant, and identity tables remain untouched for rollback. Revocation state is independent on each side: revoking a `gateway_access_keys` key does not revoke its legacy `gateway_api_keys` row, and revoking a legacy row does not revoke its owner-key row. If a rollback must disable a credential, revoke it in both stores before returning traffic to the previous release.

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
