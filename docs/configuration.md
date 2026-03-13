# Configuration Reference

This file documents `config.yaml` for `mcp-gateway`.

## Top-level

```yaml
gateway:
logging:
cache:
upstreams:
```

## `gateway`

- `listen_host` default `0.0.0.0`
- `listen_port` default `8080`
- `api_key` bearer token required by gateway HTTP endpoints
- `allow_unauthenticated` default `false`; when `true`, startup allows an empty `api_key` and logs a warning
- `trusted_proxies` default `["127.0.0.1", "::1"]`
  - `X-Forwarded-For` and `X-Client-Id` headers are only trusted when `request.remote` is in this list.
- `request_max_bytes` default `2097152` (2 MB)
- `rate_limit_per_minute` default `120`
- `circuit_breaker_fail_threshold` default `20`
- `circuit_breaker_open_seconds` default `30`

## `logging`

- `stdout_json` default `true`

## `cache`

- `enabled` default `true`
- `max_entries` default `1000` (in-memory cache)
- `default_ttl_minutes` default `60`
- `client_scoped_tools` default `[]`

`client_scoped_tools` means cache keys for listed tools include `client_id` so users do not share cached results.

## `upstreams[]`

Required:

- `id`
- `transport` (`stdio` or `http_sse`)

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

### `http_sse` upstream fields

- `endpoint` JSON-RPC HTTP endpoint
- `http_headers` optional static headers
- `bearer_token_env_var` optional env var name used if `Authorization` is not provided in `http_headers`
- `http_serialize_requests` default `false` (concurrent HTTP calls enabled). Set `true` to force one-at-a-time requests for that upstream.

## Example

```yaml
gateway:
  listen_host: "0.0.0.0"
  listen_port: 8080
  api_key: "change-me"

logging:
  stdout_json: true

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

  - id: "notion-sse"
    name: "notion-sse"
    transport: "http_sse"
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
