# RBAC Onboarding

This guide is for operators who are deploying `mcp-gateway` in `postgres_api_keys` mode and need to grant different users access to different integrations.

## Model

Authentication is API-key based.
Authorization is handled by PyCasbin using groups, integration grants, and platform grants.

The high-level rules are:

- `admin` users keep full built-in MCP access and full CLI operator access.
- standard users authenticate successfully, but do not get built-in integration or admin access.
- Non-admin access comes from group memberships and grants stored in Postgres.
- Tool discovery remains unfiltered. A user can still see tools in `GET /tools` or `tools/list`.
- Authorization is enforced on `tools/call`.
- Per-upstream `deny_tools` still overrides an otherwise allowed integration grant.

In practice, the model is:

- user -> API key
- user subject -> one or more groups
- group -> one or more integration grants
- group -> zero or more platform grants

## Roles vs Groups

Only one built-in role remains:

- `admin`

Every other managed user is a standard user with `role = null`.
Groups are the main access-control unit for non-admin users. Examples:

- `sales` -> can use `jira`
- `developers` -> can use `jira`, `github`, `aws`, `context7`, `notion`
- `ops-observers` -> can read usage reports but cannot change groups

Reserved group names cannot be created or edited:

- `legacy_admin`

## Grant Types

### Integration grants

Integration grants are upstream-level permissions.
The grant target is the upstream `id` from `config.yaml`, not an individual tool name.

Examples:

- `jira`
- `github`
- `aws`
- `context7`
- `notion`

If a group has an integration grant for `jira`, members of that group can call tools routed to the `jira` upstream.

### Platform grants

Platform grants control delegated operator capabilities.

Supported platform permissions are:

- `admin.identities.read`
- `admin.identities.write`
- `admin.groups.read`
- `admin.groups.write`
- `admin.usage.read`

These are useful when you want a non-admin operator to manage some parts of the system without giving them full admin access.

## Bootstrap Flow

Use this order on a fresh deployment:

1. Apply the schema.
2. Run the gateway in `postgres_api_keys` mode.
3. Create or use an admin API key.
4. List configured integrations.
5. Create a group.
6. Create a managed user and issue their first API key.
7. Add that user's subject to the group.
8. Grant one or more integrations to the group.
9. Test with that user's API key.

This is the simplest mental model:

- auth answers "who is this caller?"
- RBAC answers "which integrations and delegated operator capabilities can they use?"

## Example: Sales Can Only Use Jira

Assume:

- your config path is `./config.yaml`
- the gateway is running in `postgres_api_keys` mode

List valid integration ids:

```bash
mcp-gateway list-integrations --config ./config.yaml
```

Create the `sales` group:

```bash
mcp-gateway create-group \
  --config ./config.yaml \
  --name sales \
  --description "Sales team"
```

Create a non-admin user and issue their first API key:

```bash
mcp-gateway create-user \
  --config ./config.yaml \
  --subject alice \
  --display-name Alice \
  --issue-api-key
```

Add the user to the group:

```bash
mcp-gateway add-group-member \
  --config ./config.yaml \
  --group-id "$GROUP_ID" \
  --subject alice
```

Grant Jira to the group:

```bash
mcp-gateway grant-integration \
  --config ./config.yaml \
  --group-id "$GROUP_ID" \
  --upstream-id jira
```

Now `alice` should:

- authenticate successfully with the issued API key
- see tools in discovery
- be allowed to call Jira tools
- be denied on tools routed to non-Jira integrations

## Example: Developers Can Use Multiple Integrations

You can model a broader engineering group by attaching several integration grants to the same group:

- `jira`
- `github`
- `aws`
- `context7`
- `notion`

The flow is the same:

1. create a `developers` group
2. add one or more subjects to that group
3. add one integration grant per upstream

This keeps the access model simple and avoids hard-coding permissions into roles.

## Optional: Delegated Admin Access

If you want a non-admin group to manage parts of the gateway, add platform grants to that group.

Examples:

- `admin.groups.read` lets a group inspect RBAC state
- `admin.groups.write` lets a group manage groups and grants through operator workflows
- `admin.usage.read` lets a group query usage summaries

Example command:

```bash
mcp-gateway grant-platform \
  --config ./config.yaml \
  --group-id "$GROUP_ID" \
  --permission admin.usage.read
```

## Testing

Useful checks during onboarding:

### Confirm the principal

```bash
curl -H "Authorization: Bearer $USER_TOKEN" \
  http://localhost:8080/v1/me
```

This should show the user's subject, role, auth scheme, and group names.

### Confirm discovery still works

```bash
curl -X POST http://localhost:8080/mcp \
  -H "Authorization: Bearer $USER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":"1","method":"tools/list","params":{}}'
```

### Confirm execution is enforced

```bash
curl -X POST http://localhost:8080/mcp \
  -H "Authorization: Bearer $USER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":"2","method":"tools/call","params":{"name":"some.tool","arguments":{}}}'
```

If the tool is not allowed, the gateway returns a JSON-RPC error with:

- code `-32001`
- `error.data.category = "policy_denied"`

## Common Pitfalls

- A standard user with no group grants can authenticate but still cannot call tools.
- Integration grants use upstream ids, not tool names.
- `GET /tools` and `tools/list` are not an authorization test; `tools/call` is.
- `deny_tools` in upstream config can still block specific tools even when the group has the upstream grant.
- If you have not created any groups yet, that is normal. No groups are seeded automatically.
- upstream ids in grants must match the configured `upstreams[].id` values exactly.

## Related Files

- Configuration reference: [`docs/configuration.md`](configuration.md)
- Database schema: [`schema.sql`](../schema.sql)
