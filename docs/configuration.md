# Configuration Reference

This file documents `config.yaml` for `mcp-gateway`.

## Top-level

```yaml
gateway:
logging:
cache:
upstreams:
```

String values support explicit env interpolation:

- `${NAME}` requires the environment variable to be set
- `${NAME:-default}` uses `default` when the variable is unset or empty

## `gateway`

- `listen_host` default `0.0.0.0`
- `listen_port` default `8080`
- `auth_mode` default `single_shared`; supported values are `single_shared` and `postgres_api_keys`
- `api_key` bearer token used in `single_shared` mode
- `bootstrap_admin_api_key` optional break-glass admin token for `postgres_api_keys` mode
- `allow_unauthenticated` default `false`; when `true`, MCP execution routes may be open, but the `/v1/me` and `/v1/admin/*` management APIs still require a valid bearer token
- `public_tools_catalog` default `false`; when `true`, `GET /tools` skips auth but still uses rate limiting
- `trusted_proxies` default `["127.0.0.1", "::1"]`
  - `X-Forwarded-For` and `X-Client-Id` headers are only trusted when `request.remote` is in this list.
- `request_max_bytes` default `2097152` (2 MB)
- `rate_limit_per_minute` default `120`
- `circuit_breaker_fail_threshold` default `20`
- `circuit_breaker_open_seconds` default `30`

## `logging`

- `stdout_json` default `true`
- `extra_redact_fields` default `[]`; additional case-insensitive payload keys redacted before request/response persistence

## `cache`

- `enabled` default `true`
- `max_entries` default `1000` (in-memory cache)
- `default_ttl_minutes` default `60`
- `client_scoped_tools` default `[]`

`client_scoped_tools` means cache keys for listed tools include `client_id` so users do not share cached results.

## Management APIs

When `gateway.auth_mode` is `postgres_api_keys`, the gateway exposes:

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

Role behavior:

- `admin`: full MCP access plus user management, RBAC management, usage reporting, and API key management
- standard users: no built-in integration grants; self-service API key management remains available, but tool execution and delegated admin access come from PyCasbin group memberships plus integration or platform grants

## `upstreams[]`

Required:

- `id`
- `transport` (`stdio` or `streamable_http`)

Common:

- `name` default `id`
- `timeout_ms` default `10000`
- `max_in_flight` default `20`
- `deny_tools` default `[]`
- `cache_ttl_minutes` optional per-upstream override
- `tool_routes` optional routing hints by prefix
- `circuit_breaker_fail_threshold` optional override
- `circuit_breaker_open_seconds` optional override

### `stdio` upstream fields

- `command` string or string list
- `args` optional list, appended to `command`
- `env` optional map
- `cwd` optional working directory
- `stdio_read_limit_bytes` default `104857600` (100 MB)

### `streamable_http` upstream fields

- `endpoint` JSON-RPC HTTP endpoint
- `http_headers` optional static headers
- `bearer_token_env_var` optional env var name used if `Authorization` is not provided in `http_headers`
- `http_serialize_requests` default `false` (concurrent HTTP calls enabled). Set `true` to force one-at-a-time requests for that upstream.

## Example

```yaml
gateway:
  listen_host: "0.0.0.0"
  listen_port: 8080
  auth_mode: "single_shared"
  api_key: "${MCP_GATEWAY_API_KEY:-change-me}"
  bootstrap_admin_api_key: "${MCP_GATEWAY_BOOTSTRAP_ADMIN_API_KEY:-}"
  allow_unauthenticated: false
  public_tools_catalog: false

logging:
  stdout_json: true
  extra_redact_fields: []

cache:
  enabled: true
  default_ttl_minutes: 60
  max_entries: 10000
  client_scoped_tools: []

upstreams:
  - id: "context7"
    transport: "stdio"
    command: "npx"
    args:
      - "-y"
      - "@upstash/context7-mcp"
    deny_tools: []

  - id: "chrome-devtools"
    transport: "stdio"
    command: "npx"
    args:
      - "-y"
      - "chrome-devtools-mcp@latest"
      - "--slim"
      - "--headless"
      - "--no-usage-statistics"
    deny_tools: []

  - id: "notion-remote"
    name: "notion-remote"
    transport: "streamable_http"
    endpoint: "https://mcp.notion.com/mcp"
    http_serialize_requests: false
    http_headers:
      Notion-Version: "2022-06-28"
    bearer_token_env_var: "NOTION_TOKEN"
    timeout_ms: 30000
    deny_tools:
      - "API-post-page"
      - "API-patch-page"
    tool_routes:
      - "API-"
```
