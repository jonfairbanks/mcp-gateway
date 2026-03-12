# MCP Gateway

`mcp-gateway` is an HTTP-hosted MCP proxy that lets clients connect to one MCP server while you route to many upstream MCP servers.

This project is built for multi-client deployments (for example, Codex/Claude users pointing at one shared gateway) with policy controls, observability, and resilience features.

## What It Does

- Exposes one HTTP MCP endpoint for clients.
- Connects to multiple upstream MCP servers (`stdio` and HTTP JSON-RPC endpoints).
- Aggregates discovery calls (`tools/list`, `resources/list`, `resources/templates/list`, `prompts/list`).
- Routes `tools/call` to the correct upstream using a learned tool registry.
- Enforces per-upstream deny lists for tools.
- Caches successful `tools/call` responses with TTL.
- Logs requests/responses/denials to Postgres and structured stdout.
- Applies per-upstream health counters, concurrency limits, and circuit breakers.
- Runs startup warmup to initialize upstreams and pre-discover tools.

## Architecture

Client -> MCP Gateway (HTTP) -> Upstream MCP servers

Gateway behavior:

- `initialize`: fan-out to all upstreams and merge capabilities.
- `notifications/initialized`: fan-out fire-and-forget.
- `tools/list`: fan-out and merge tools, then build `tool_name -> upstream_id` registry.
- `tools/call`: denylist check -> cache lookup -> upstream call -> cache write.

## Install

```bash
pip install .
```

Run:

```bash
gateway serve-http --config /path/to/config.yaml
```

## Quick Start

1. Create config from [`config.example.yaml`](config.example.yaml).
2. Initialize database schema:

```bash
psql "$DATABASE_URL" -f schema.sql
```

3. Start gateway:

```bash
DATABASE_URL='postgresql://postgres:postgres@localhost:5432/mcp_gateway' \
  gateway serve-http --config /path/to/config.yaml
```

Most settings are optional. `config.example.yaml` intentionally keeps only required fields.

Default values for common settings:

- `gateway.listen_host`: `0.0.0.0`
- `gateway.listen_port`: `8080`
- `gateway.request_max_bytes`: `1048576` (1 MB)
- `gateway.rate_limit_per_minute`: `120`
- `cache.default_ttl_minutes`: `60`
- `upstreams[].timeout_ms`: `10000`
- `upstreams[].stdio_read_limit_bytes`: `104857600` (100 MB)

## Docker Deployment

Use the included Docker assets:

- [`Dockerfile`](Dockerfile)
- [`docker-compose.yml`](docker-compose.yml)

Start gateway + Postgres:

```bash
docker compose up --build
```

Default endpoints:

- Gateway: `http://localhost:8080`
- Postgres: `postgresql://postgres:postgres@localhost:5432/mcp_gateway`

## Client Configuration

Point clients at the gateway URL and include bearer auth.

Codex example:

```toml
[mcp_servers.mcp-gateway]
url = "http://localhost:8080/mcp"
http_headers = { "Authorization" = "Bearer change-me" }
```

Notes:

- Client-side tool names may appear as `mcp__mcp-gateway__<tool>`.
- Upstream/original tool names remain unchanged inside gateway routing and policies.

## Configuration Reference

Top-level config:

- `gateway.listen_host`: bind host.
- `gateway.listen_port`: bind port.
- `gateway.public_base_url`: public URL for docs/ops usage.
- `gateway.api_key`: required bearer token for client requests.
- `gateway.trusted_proxies`: source IPs allowed to supply `X-Forwarded-For`.
- `gateway.request_max_bytes`: max HTTP body size.
- `gateway.rate_limit_per_minute`: request limit per client id/IP per minute.
- `gateway.circuit_breaker_fail_threshold`: global breaker threshold.
- `gateway.circuit_breaker_open_seconds`: global breaker open duration.

Cache:

- `cache.enabled`: enable/disable cache.
- `cache.max_entries`: in-memory cache size.
- `cache.default_ttl_minutes`: default tool cache TTL.
- `cache.client_scoped_tools`: tools whose cache key includes `client_id`.

Per upstream:

- `id`, `name`
- `transport`: `stdio` or `http_sse`.
- `command` + optional `env`, `cwd` for `stdio` upstreams.
- `endpoint` for `http_sse` upstreams (JSON-RPC POST endpoint).
- `http_headers`: optional static headers sent to HTTP upstream requests.
- `timeout_ms`
- `stdio_read_limit_bytes`: max bytes for one upstream stdout line (default `104857600` / 100 MB).
- `max_in_flight`: concurrency cap per upstream.
- `deny_tools`: exact tool names to block.
- `cache_ttl_minutes`: optional override.
- `tool_routes`: optional prefix routing hints.
- `circuit_breaker_fail_threshold`, `circuit_breaker_open_seconds`: per-upstream overrides.

## HTTP Endpoints

- `POST /mcp`: JSON-RPC request/response.
- `GET /sse`: open SSE stream session.
- `POST /message?session_id=...`: send JSON-RPC and stream result via SSE.
- `GET /healthz`: liveness plus warmup/breaker status.
- `GET /readyz`: readiness (`503` until at least one upstream initializes successfully).

## Observability

Structured logs include:

- `mcp_request`, `mcp_response`, `mcp_denied`
- `upstream_health` with per-method success/fail counters
- `upstream_warmup` with discovered tools per upstream
- `upstream_stderr` tagged with `upstream_id`

Tool observability fields:

- `tool_name`: original MCP tool name
- `tool_alias`: synthetic `<upstream_id>.<tool_name>` alias for log clarity

Database tables in [`schema.sql`](schema.sql):

- `mcp_requests`
- `mcp_responses`
- `mcp_denials`
- `mcp_cache`

## Policy and Safety

- Denylists are enforced before upstream invocation.
- Cache only applies to `tools/call` successes.
- Notifications are handled as non-blocking upstream notifications.
- Circuit breakers protect the gateway from repeated upstream failures.

## Known MCP Behavior

Some MCP servers expose only tools and no resources/templates.

Expected outcome in that case:

- `tools/list` returns tools.
- `resources/list` may return empty.
- `resources/templates/list` may return empty or `-32601` upstream; gateway treats optional method absence as non-fatal for aggregate discovery.

## Troubleshooting

If tools do not appear in client:

1. Confirm client points to gateway URL (`/mcp`) and sends `Authorization: Bearer <api_key>`.
2. Check warmup logs for each upstream (`event=upstream_warmup`).
3. Check discovery logs (`method=tools/list`) and ensure aggregate success.
4. Review `upstream_stderr` logs for auth/permission errors.

If a specific upstream tool set is missing:

1. Verify upstream can initialize and answer `tools/list` directly.
2. Confirm denylist is not filtering those tools.
3. Confirm upstream auth context is valid (for example, CLI login/token state for CLI-based MCP servers).

## Development Notes

- Runtime mode is HTTP-only (`gateway serve-http`).
- Upstream transports remain mixed (`stdio` and HTTP endpoint upstreams are both supported).
- Python version: 3.11+

## License

MIT (see project metadata in [`pyproject.toml`](pyproject.toml)).
