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
- PyCasbin-backed RBAC for integration and platform permissions in `postgres_api_keys` mode.
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
Set `gateway.public_tools_catalog: true` only if you want `GET /tools` to be browsable without auth; execution endpoints remain protected.
For multi-user auth, set `gateway.auth_mode: "postgres_api_keys"`, keep `DATABASE_URL` set, and optionally configure `gateway.bootstrap_admin_api_key` for break-glass admin access.
The gateway only supports MCP protocol version `2025-11-25`; older MCP protocol versions are rejected.
In `postgres_api_keys` mode, `admin` retains full platform access. Non-admin API-key users authenticate successfully, but tool execution and admin access come from PyCasbin group memberships plus integration or platform grants.

To seed the first database-backed admin key:

```bash
DATABASE_URL='postgresql://postgres:postgres@localhost:5432/mcp_gateway' \
  mcp-gateway create-api-key \
  --config /path/to/config.yaml \
  --subject alice \
  --display-name "Alice" \
  --role admin \
  --key-name default
```

For standard non-admin users, omit `--role` and assign access through groups and grants.

Once you have an admin key, the gateway also exposes JSON management APIs:

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

A ready-to-import Postman collection for the HTTP API lives at [`docs/postman/mcp-gateway.postman_collection.json`](docs/postman/mcp-gateway.postman_collection.json).
Set the collection variables `base_url` and `admin_token` first; `user_token`, `group_id`, `user_id`, and `key_id` can be filled in automatically by the create requests.

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

- `POST /mcp` main JSON-RPC MCP endpoint for `initialize`, discovery requests, and `tools/call`.
- `GET /tools` lightweight HTTP tool catalog showing exposed and denied tools across upstreams; optionally public when `gateway.public_tools_catalog: true`.
- `GET /metrics` Prometheus/OpenMetrics scrape endpoint for gateway and upstream counters.
- `GET /healthz` liveness endpoint with gateway status and warmup/circuit-breaker snapshot.
- `GET /readyz` readiness endpoint; returns `503` until at least one upstream is initialized and usable.
- `GET /v1/me` returns the authenticated principal, role, auth scheme, and linked user metadata.
- `GET /v1/me/api-keys` lists the caller’s API keys and metadata without returning plaintext secrets.
- `POST /v1/me/api-keys` creates a new API key for the authenticated user and returns the plaintext key once.
- `DELETE /v1/me/api-keys/{key_id}` revokes one of the authenticated user’s API keys.
- `GET /v1/admin/identities` lists local identities keyed by subject; admin only.
- `PUT /v1/admin/identities/{subject}` creates or replaces a local identity record; admin only.
- `PATCH /v1/admin/identities/{subject}` updates identity metadata or active status; admin only.
- `GET /v1/admin/users` lists all managed users with role metadata and active status; admin only.
- `POST /v1/admin/users` creates a managed user and can optionally issue an initial API key; admin only.
- `PATCH /v1/admin/users/{user_id}` updates a user’s display name, admin status, or active status; admin only.
- `GET /v1/admin/integrations` lists configured upstream integration ids that can be granted through PyCasbin; admin only.
- `GET /v1/admin/groups` lists RBAC groups; admin only.
- `POST /v1/admin/groups` creates an RBAC group; admin only.
- `PATCH /v1/admin/groups/{group_id}` updates an RBAC group; admin only.
- `DELETE /v1/admin/groups/{group_id}` deletes an RBAC group; admin only.
- `POST /v1/admin/groups/{group_id}/members` adds a subject to an RBAC group; admin only.
- `DELETE /v1/admin/groups/{group_id}/members/{subject}` removes a subject from an RBAC group; admin only.
- `GET /v1/admin/groups/{group_id}/integration-grants` lists integration grants for a group; admin only.
- `POST /v1/admin/groups/{group_id}/integration-grants` grants one upstream integration to a group; admin only.
- `DELETE /v1/admin/groups/{group_id}/integration-grants/{upstream_id}` removes one integration grant from a group; admin only.
- `GET /v1/admin/groups/{group_id}/platform-grants` lists platform permissions for a group; admin only.
- `POST /v1/admin/groups/{group_id}/platform-grants` grants one platform permission to a group; admin only.
- `DELETE /v1/admin/groups/{group_id}/platform-grants/{permission}` removes one platform permission from a group; admin only.
- `GET /v1/admin/usage` returns aggregated request and key usage statistics grouped by subject, integration, or API key; admin only.

## Docker

```bash
docker compose up --build
```

Default local endpoints:

- Gateway: `http://localhost:8080`
- Postgres: `postgresql://postgres:postgres@localhost:5432/mcp_gateway`

## Testing

Use the editable dev install so pytest has both runtime dependencies such as `psycopg` and the test tools:

```bash
pip install -e ".[dev]"
pytest
```

The Postgres integration test boots the aiohttp app, one HTTP fixture upstream, and one stdio fixture upstream against a real database. Start Postgres first, then point the test suite at it with a dedicated DSN:

```bash
docker compose up -d postgres
export MCP_GATEWAY_TEST_DATABASE_URL='postgresql://postgres:postgres@localhost:5432/mcp_gateway'
pytest tests/test_integration_app.py
```

## Metrics

Prometheus metrics keep `upstream_id` so MCP call volume is easy to query by integration over time.

Useful counters:

- `mcp_gateway_tool_calls_total{upstream_id,success,cache_hit}`
- `mcp_gateway_upstream_calls_total{upstream_id,method,success}`
- `mcp_gateway_denials_total{upstream_id}`

Example query:

```promql
sum by (upstream_id) (
  increase(mcp_gateway_tool_calls_total[1h])
)
```

## Docs

- RBAC onboarding: [`docs/rbac-onboarding.md`](docs/rbac-onboarding.md)
- Configuration reference: [`docs/configuration.md`](docs/configuration.md)
- Database schema: [`schema.sql`](schema.sql)

## Troubleshooting

- If tools are missing, check `upstream_warmup` and `tools/list` logs.
- If a tool call is blocked, look for JSON-RPC `-32001` with `error.data.category = policy_denied`.
- If auth fails, verify `Authorization: Bearer <api_key>` matches `gateway.api_key`.
- If key management endpoints return `400 Unavailable`, verify `gateway.auth_mode` is `postgres_api_keys`.

## License

Apache-2.0 (see [`LICENSE`](LICENSE)).

Contributions submitted to this repository are accepted under the repository's Apache-2.0 license unless explicitly stated otherwise.
