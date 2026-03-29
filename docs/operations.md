# Operations Guide

## Health and Readiness

- `GET /healthz` confirms the process is alive
- `GET /readyz` returns `503` until the configured readiness policy is satisfied
- both endpoints return minimal public payloads only

Readiness policies:

- `any` marks the gateway ready when at least one upstream has passed both `initialize` and `tools/list`
- `required` marks the gateway ready only when every upstream listed in `gateway.required_ready_upstreams` is healthy
- `threshold` marks the gateway ready when the configured healthy-count and/or healthy-percent threshold is met

## Metrics

Prometheus metrics keep `upstream_id` so MCP usage is easy to query by integration.

By default, `GET /metrics` requires bearer authentication. Set `gateway.public_metrics: true` only when that scrape endpoint is intentionally exposed.

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

## Tracing

Tracing is off by default. The gateway only emits OpenTelemetry spans when `gateway.tracing_enabled: true` and standard OTEL exporter environment variables are present.

Common setup:

- `OTEL_TRACES_EXPORTER=otlp`
- `OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318`
- optional: `OTEL_SERVICE_NAME=mcp-gateway`

When enabled, the gateway emits spans for:

- incoming HTTP requests
- MCP method handling inside the gateway
- outbound upstream calls
- Postgres repository operations such as audit writes, cache access, auth lookups, and rate limiting

For `streamable_http` upstreams, the current trace context is propagated on outbound requests with standard trace headers.

## Logs

Structured logs include upstream and tool-level detail. If an upstream fails warmup or a tool call is denied, the logs are usually the fastest source of detail.

## Security Notes

- do not store credentials directly in `config.yaml`
- use env refs for secrets and tokens
- `gateway.allow_unauthenticated: true` should be treated as an intentional public exposure decision
- `gateway.public_tools_catalog: true` only makes the catalog public; it does not make `tools/call` public
- `gateway.public_metrics: true` should be treated as an explicit observability exposure decision
- the gateway currently supports MCP protocol versions `2025-03-26` and `2025-11-25`

## Troubleshooting

- If tools are missing, check `upstream_warmup` and `tools/list` logs.
- If `readyz` stays unhealthy, check the configured readiness policy and the warmup status of the upstreams that policy depends on.
- If a tool call is blocked, look for JSON-RPC `-32001` with `error.data.category = policy_denied`.
- If auth fails, verify the bearer token and `gateway.auth_mode`.
- If a `stdio` upstream fails on startup, verify the CLI exists on the host and the `env` block is a YAML mapping, not a list.
