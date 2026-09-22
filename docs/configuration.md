# Configuration Reference

This document describes the deployment-time configuration surface for `mcp-gateway`.

Use it together with [`config.example.yaml`](../config.example.yaml) when building a production config.

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

Operational guidance:

- use env refs for secrets, tokens, and API keys
- keep `env` and `http_headers` as YAML mappings, not lists
- prefer explicit `name` values only when they help operators; otherwise `id` is enough

## `gateway`

- `listen_host` default `0.0.0.0`
- `listen_port` default `8080`
- `auth_mode` default `single_shared`; supported values are `single_shared` and `postgres_api_keys`
- `api_key` bearer token used in `single_shared` mode
- `bootstrap_api_key` optional break-glass shared token for `postgres_api_keys` mode. `bootstrap_admin_api_key` remains an accepted alias for existing configuration.
- `allow_unauthenticated` default `false`; when `true`, MCP execution routes may be open, but `GET /v1/me` still requires a valid bearer token
- `allowed_origins` defaults to `[]`. MCP requests with an `Origin` header must match an exact HTTP(S) origin in this list, such as `https://client.example.com`. Native clients without `Origin` remain supported. Wildcards and `null` are rejected.
- `public_tools_catalog` default `false`; when `true`, `GET /tools` skips auth but still uses rate limiting
- `public_metrics` default `false`; when `true`, `GET /metrics` skips auth
- `tracing_enabled` default `false`; when `true`, OTEL exporter environment variables may activate tracing/export
- `readiness_mode` default `any`; supported values are `any`, `required`, and `threshold`
- `required_ready_upstreams` default `[]`; required when `readiness_mode` is `required`
- `readiness_min_healthy_upstreams` optional minimum healthy upstream count for `readiness_mode: threshold`
- `readiness_min_healthy_percent` optional minimum healthy upstream percentage for `readiness_mode: threshold`
- `trusted_proxies` default `["127.0.0.1", "::1"]`
  - `X-Forwarded-For` and `X-Client-Id` headers are only trusted when `request.remote` is in this list.
- `request_max_bytes` default `2097152` (2 MB)
- `rate_limit_per_minute` default `120`
- `circuit_breaker_fail_threshold` default `20`
- `circuit_breaker_open_seconds` default `30`

Deployment notes:

- `auth_mode: single_shared` is the simplest deployment path
- `auth_mode: postgres_api_keys` is the right mode when callers need individually revocable keys
- `allow_unauthenticated: true` should be treated as a public exposure setting
- operator workflows such as validation, warmup checks, and API key lifecycle are CLI-driven

## `logging`

- `stdout_json` default `true`
- `extra_redact_fields` default `[]`; additional case-insensitive payload keys redacted before request/response persistence
- `store_request_bodies` default `false`
- `store_response_bodies` default `false`
- `body_capture_upstreams` default `[]`; request/response bodies for listed upstream IDs may be persisted even when global body capture is off
- `body_capture_tools` default `[]`; request/response bodies for listed tool names may be persisted even when global body capture is off

## `cache`

- `enabled` default `true`
- `max_entries` default `1000` (in-memory cache)
- `default_ttl_minutes` default `60`
- `allowed_tools` default `[]`; only listed tools are cacheable
- `globally_shareable_tools` default `[]`; listed tools may share cache entries across callers

Cache behavior:

- no tool calls are cached unless they appear in `allowed_tools`
- cached tools are scoped by API key ID in `postgres_api_keys` mode
- `single_shared` and bootstrap authentication use one shared gateway principal for cache scoping
- only tools in `globally_shareable_tools` may share cache entries across different callers

The in-memory cache is only a local optimization. Shared cache correctness comes from Postgres.

## HTTP APIs

The gateway exposes one authenticated identity endpoint:

- `GET /v1/me`

There is no HTTP API key-management surface. Operator workflows move through the CLI:

- `mcp-gateway validate-config`
- `mcp-gateway warmup-check`
- `mcp-gateway list-integrations`
- `mcp-gateway create-api-key --key-name NAME --expires-days N`
- `mcp-gateway list-api-keys`
- `mcp-gateway revoke-api-key --key-id UUID`

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

Operator guidance:

- choose stable `id` values because they identify upstreams in logs, metrics, and routing
- set `tool_routes` when you want routing to stay predictable across similarly named integrations
- use per-upstream breaker and timeout overrides for slower or less reliable vendors

### `stdio` upstream fields

- `command` string or string list
- `args` optional list, appended to `command`
- `env` optional map of environment variables. Stdio processes inherit only `PATH`, `HOME`, `LANG`, `LC_ALL`, `LC_CTYPE`, `TZ`, `TMPDIR`, `TMP`, `TEMP`, `SYSTEMROOT`, and `WINDIR`. Pass each upstream credential explicitly, for example `SERVICE_TOKEN: "${SERVICE_TOKEN}"`. Other parent variables, including proxy and runtime-loader settings, are not inherited. This is environment filtering, not an OS sandbox; child processes still share the gateway user and filesystem.
- `cwd` optional working directory
- `stdio_read_limit_bytes` default `104857600` (100 MB)

Use `stdio` when the upstream MCP is installed locally on each gateway replica.

### `streamable_http` upstream fields

- `endpoint` JSON-RPC HTTP endpoint
- `http_headers` optional static headers
- `bearer_token_env_var` optional env var name used if `Authorization` is not provided in `http_headers`
- `http_response_max_bytes` defaults to 8388608 (8 MiB), measured after HTTP decompression. Applies to JSON, SSE, error, and notification responses, including SSE framing. SSE lines and events share this byte limit; there are no smaller per-line or per-event byte limits. SSE additionally permits at most 16384 lines and 1024 parsed data events. Exceeding a limit fails the upstream request.
- `http_serialize_requests` default `false` (concurrent HTTP calls enabled). Set `true` to force one-at-a-time requests for that upstream.
- The gateway currently supports MCP protocol versions `2025-03-26` and `2025-11-25`. Unsupported versions are rejected.

Use `streamable_http` when the upstream MCP is already exposed over HTTP.

If both `http_headers.Authorization` and `bearer_token_env_var` are set, the explicit `Authorization` header wins.

## Example

```yaml
gateway:
  listen_host: "0.0.0.0"
  listen_port: 8080
  auth_mode: "single_shared"
  api_key: "${MCP_GATEWAY_API_KEY}"
  bootstrap_api_key: "${MCP_GATEWAY_BOOTSTRAP_ADMIN_API_KEY:-}"
  allow_unauthenticated: false
  public_tools_catalog: false
  tracing_enabled: false
  readiness_mode: "any"

logging:
  stdout_json: true
  extra_redact_fields: []

cache:
  enabled: true
  default_ttl_minutes: 60
  max_entries: 10000
  allowed_tools: []
  globally_shareable_tools: []

upstreams:
  - id: "context7"
    transport: "stdio"
    command: "context7-mcp"
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

  - id: "github"
    name: "github"
    transport: "streamable_http"
    endpoint: "https://api.githubcopilot.com/mcp/"
    http_serialize_requests: false
    bearer_token_env_var: "GITHUB_PAT_TOKEN"
    timeout_ms: 30000
    deny_tools:
      - "create_or_update_file"
      - "delete_file"
    tool_routes:
      - "github."
```

## Validation Rules

Common validation failures:

- `upstreams[].transport` must be `stdio` or `streamable_http`
- `upstreams[].env` must be a YAML mapping
- `upstreams[].http_headers` must be a YAML mapping
- `bearer_token_env_var` must be a valid environment variable name
- `command` must be a string or list of strings
- `args` must be a list

## Security Behavior

Boolean settings accept YAML booleans or the strings `true` and `false` (case insensitive), including values from environment interpolation. Other values are rejected.

Denied tools remain in the internal routing registry so calls return a policy denial, but their schemas are omitted from fresh and cached `tools/list` responses. The optional `/tools` operator catalog still reports configured tool names and deny rules; leave `public_tools_catalog` disabled to keep that metadata private.

Stdio requests are never replayed after a write or response failure because their outcome may be unknown. After losing an initialized session, a later independent request restarts the child and completes the MCP initialization handshake before sending the new request. If initialization fails, the new request is not sent. Gateway readiness retains its startup-policy behavior so a recoverable timeout does not prevent traffic from triggering recovery. Check the upstream's state before manually retrying a mutation.

Cache normalization ignores only the protocol-level `params._meta.progressToken`. Nested tool arguments are preserved. The `v2` cache namespace prevents reuse of older entries; existing entries expire normally.
