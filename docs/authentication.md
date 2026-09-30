# Authentication

`mcp-gateway` authenticates bearer tokens. It has two modes:

- `single_shared` accepts the configured `gateway.api_key`.
- `postgres_api_keys` accepts operator-issued keys in `gateway_access_keys`.

Every authenticated key has the same gateway access. There are no users, roles, groups, grants, or Casbin policy. Configure `upstreams[].deny_tools` to block specific tools for every authenticated caller.

## Shared Authentication

Use `single_shared` when one trusted bearer token is enough:

```yaml
gateway:
  auth_mode: "single_shared"
  api_key: "${MCP_GATEWAY_API_KEY}"
```

The shared token maps to one gateway principal. This keeps rate-limit and cache behavior consistent across callers that use it.

## Operator-Managed API Keys

Use `postgres_api_keys` when callers need separate keys that an operator can independently inspect or revoke:

```yaml
gateway:
  auth_mode: "postgres_api_keys"
  bootstrap_api_key: "${MCP_GATEWAY_BOOTSTRAP_ADMIN_API_KEY:-}"
```

`gateway.bootstrap_api_key` is an optional break-glass shared token. Existing `gateway.bootstrap_admin_api_key` configuration remains accepted as an alias, and the example keeps `MCP_GATEWAY_BOOTSTRAP_ADMIN_API_KEY` for environment compatibility. The bootstrap token has the shared gateway principal for cache scoping.

Create, inspect, and revoke keys only through the operator CLI:

```bash
mcp-gateway create-api-key --config ./config.yaml --key-name NAME --expires-days N
mcp-gateway list-api-keys --config ./config.yaml
mcp-gateway revoke-api-key --config ./config.yaml --key-id UUID
```

`create-api-key` prints the secret once. Store it in the caller's secret manager. The gateway stores only the key ID, prefix, hash, timestamps, and revocation state.

`GET /v1/me` remains available to return the authenticated key principal. The gateway does not expose `/v1/me/api-keys` endpoints. API key lifecycle is an operator CLI workflow.

## Cache Scoping

In `postgres_api_keys` mode, the cache uses the API key ID as the caller scope. Keys therefore do not share cached tool results by default. `single_shared` and bootstrap authentication use the shared gateway principal. Tools listed in `cache.globally_shareable_tools` remain the explicit exception and can share cached results across callers.

## Migrating From RBAC

The owner-key release removes authorization based on legacy users, roles, groups, and grants. Before rollout, take a restorable database backup. Have the CI deployment stage apply `schema.sql` and then `migrations/001_owner_api_keys.sql`; do not add migration execution to local gateway startup.

`migrations/001_owner_api_keys.sql` creates `gateway_access_keys` and imports only legacy `gateway_api_keys` rows that meet all of these conditions:

- the key is active, unexpired, and not revoked
- its linked `gateway_users` row is active
- its linked user has the legacy `admin` role

On a fresh database, the migration creates the key table and skips the import. Imported rows keep their existing IDs and hashes. Restricted non-admin legacy keys are intentionally skipped because granting them the new shared access model would expand their access. Issue replacement owner keys after rollout for any caller that previously used a restricted key.

The migration leaves legacy identity and group tables intact so a rollback can use the previous release. Owner-key and legacy-key revocations are independent. If a rollback must invalidate a key, revoke the corresponding row in both `gateway_access_keys` and `gateway_api_keys` before sending traffic back to the prior release.

## Verification

After the CI rollout, verify the principal and tool discovery with an issued key:

```bash
curl -H "Authorization: Bearer $MCP_GATEWAY_API_KEY" http://gateway.example/v1/me
curl -X POST http://gateway.example/mcp \
  -H "Authorization: Bearer $MCP_GATEWAY_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":"1","method":"tools/list","params":{}}'
```

`deny_tools` remains the expected reason for an authenticated caller to receive a policy denial for a tool.

## Related Files

- Configuration reference: [`docs/configuration.md`](configuration.md)
- Deployment guide: [`docs/deployment-guide.md`](deployment-guide.md)
- Database schema: [`schema.sql`](../schema.sql)
