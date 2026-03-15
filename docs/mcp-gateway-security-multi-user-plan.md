# MCP Gateway Security + Multi-User Upgrade

## Summary

Implement this as one coherent upgrade with backward compatibility preserved.

- Add explicit env-variable interpolation to `config.yaml` so secrets like `gateway.api_key` and upstream env values can come from process env instead of clear text.
- Keep `/mcp`, `/sse`, and `/message` authenticated, but make `/tools` optionally public via config because it is discovery-only and still worth keeping opt-in due to information disclosure.
- Adopt `uv` for developer workflow only, not as a runtime or packaging rewrite. Keep `setuptools` and the current Docker install path unchanged in this pass.
- Introduce a Postgres-backed multi-user auth mode with hashed API keys, roles, usage attribution, and self-service key management APIs, while retaining the current single shared key mode as the default.

## Implementation Sequence

1. Config/env secret support
2. Optional public `/tools`
3. Auth/request-context refactor
4. Postgres-backed users, API keys, and RBAC
5. Self-service/admin APIs and usage reporting
6. `uv` developer workflow adoption

## Key Changes

### Config and Secrets

- Add recursive env interpolation in the config loader for all string values using explicit syntax only:
  - `${NAME}`: required env var
  - `${NAME:-default}`: optional env var with fallback
- Fail startup with a clear `ValueError` when a required env var is missing.
- Update config schema with:
  - `gateway.public_tools_catalog: bool = false`
  - `gateway.auth_mode: "single_shared" | "postgres_api_keys" = "single_shared"`
  - `gateway.bootstrap_admin_api_key: str = ""`
- Keep existing `gateway.api_key` behavior unchanged for `single_shared` mode.
- Update `config.example.yaml`, `README.md`, and `docs/configuration.md` to use env refs for secrets:
  - `gateway.api_key: "${MCP_GATEWAY_API_KEY}"`
  - `NOTION_TOKEN: "${NOTION_TOKEN}"`
- Add targeted comments in config-loading code explaining why expansion is explicit and why missing envs fail fast.

### `/tools` Visibility and HTTP Auth Refactor

- Refactor HTTP auth from "boolean authorize" to "authenticate request and return principal/context".
- Make `/tools` skip auth only when `gateway.public_tools_catalog` is `true`; keep rate limiting enabled either way.
- Keep `/healthz`, `/readyz`, and `/metrics` behavior unchanged.
- Add clarifying comments around:
  - trusted-proxy handling for `X-Client-Id`
  - why `/tools` can be public while execution routes stay private
  - the stdio read loop that ignores progress/notification frames until the matching response id arrives

### Multi-User Auth, API Keys, and RBAC

- Add a new auth service layer and request context object passed from HTTP handlers into gateway execution:
  - `AuthenticatedPrincipal { user_id, api_key_id, subject, role, is_bootstrap_admin }`
  - `RequestContext { principal, client_id }`
- Use Postgres as the source of truth in `postgres_api_keys` mode.
- Add new tables plus an additive upgrade SQL script and refresh `schema.sql` for fresh installs:
  - `gateway_users`
  - `gateway_api_keys`
- Extend `mcp_requests` with nullable auth attribution columns:
  - `auth_user_id`
  - `auth_api_key_id`
  - `auth_role`
- Store API keys as:
  - generated plaintext shown once to the caller
  - stored `key_prefix` for lookup
  - stored `key_hash` using SHA-256
  - verification via prefix lookup + constant-time hash comparison
- Support one role per user for v1:
  - `admin`: full MCP access, user management, usage reporting, key management for any user
  - `member`: full MCP access, self-service key management
  - `viewer`: discovery-only access (`initialize`, discovery RPCs, `/tools`); deny `tools/call`, `/sse`, and `/message`
- Keep `gateway.bootstrap_admin_api_key` as a break-glass/admin bootstrap token only in `postgres_api_keys` mode. It authenticates as admin but is not a DB-managed key and should be removable after real admin keys exist.
- Change rate limiting and cache scoping to use authenticated identity first:
  - rate limit key: `api_key_id` if authenticated, otherwise current client-id logic
  - `client_scoped_tools` cache scope: `user_id` if authenticated, otherwise current client-id logic

### Self-Service and Admin APIs

Add JSON admin endpoints on the existing aiohttp server.

- `GET /v1/me`
  - returns current principal metadata and role
- `GET /v1/me/api-keys`
  - returns the caller's active/revoked key metadata, never the raw secret
- `POST /v1/me/api-keys`
  - request: `{ "label": string, "expires_at": string|null }`
  - response: key metadata plus one-time `api_key`
- `DELETE /v1/me/api-keys/{key_id}`
  - revoke one of the caller's own keys
- `GET /v1/admin/users`
  - admin-only user list with role and status
- `POST /v1/admin/users`
  - request: `{ "subject": string, "display_name": string, "role": "admin"|"member"|"viewer", "issue_api_key": bool, "key_label": string|null, "expires_at": string|null }`
  - response: created user and optional one-time `api_key`
- `PATCH /v1/admin/users/{user_id}`
  - request supports `display_name`, `role`, and `is_active`
- `GET /v1/admin/usage`
  - admin-only aggregated usage from `mcp_requests`/`mcp_responses`
  - query params: `from`, `to`, `group_by=user|api_key`
  - response rows include request count, tool-call count, success count, denial count, cache-hit count, and last-used timestamp
- Do not add a browser UI in this pass; self-service is API-first.

### `uv` Adoption

- Keep the build backend as `setuptools`.
- Add `uv.lock`.
- Update docs to standardize local/dev commands on:
  - `uv sync --extra dev`
  - `uv run pytest`
  - `uv run ruff check`
  - `uv run mcp-gateway serve --config ...`
- Do not switch the Dockerfile to `uv` in this pass; runtime packaging stays as-is to keep deployment changes small.

## Test Plan

- Config tests:
  - env interpolation works for required and defaulted vars
  - missing required env fails with a clear error
  - nested upstream `env` and `http_headers` values interpolate correctly
- HTTP/auth tests:
  - `/tools` requires auth by default
  - `/tools` becomes public when `public_tools_catalog` is enabled
  - `/mcp`, `/sse`, and `/message` still require auth when `/tools` is public
  - bootstrap admin key authenticates only in `postgres_api_keys` mode
- Multi-user tests:
  - valid key authenticates and attaches `user_id` / `api_key_id`
  - revoked key, expired key, and inactive user are rejected
  - `viewer` can initialize and list tools but cannot call tools
  - `member` cannot use admin endpoints
  - `admin` can create users and issue keys
  - self-service create/list/revoke works and only returns plaintext key on creation
- Usage/accounting tests:
  - request rows persist `auth_user_id`, `auth_api_key_id`, and `auth_role`
  - usage endpoint aggregates correctly by user and by key
  - client-scoped cache now isolates by authenticated user
- Backward-compat tests:
  - existing `gateway.api_key` mode still behaves exactly as today
  - current README/config examples still work after replacing literals with env refs

## Assumptions and Defaults

- Default rollout mode stays `single_shared`; multi-user is opt-in.
- `/tools` stays private by default; public exposure is a deliberate config choice.
- Env expansion is explicit only; plain strings are never auto-expanded.
- `uv` is a developer-experience improvement, not a runtime dependency change.
- RBAC is intentionally small for v1: `admin`, `member`, `viewer`.
- Self-service means authenticated API endpoints, not a web console.
