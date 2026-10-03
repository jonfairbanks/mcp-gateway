# Client Configuration

Both `/mcp` and `/mcp/full` are always available with the same bearer auth. `/mcp` exposes compact search and call wrappers. `/mcp/full` exposes individual tools for direct calls and native client discovery.

## Choose an Endpoint

| Client | Recommended Endpoint | Reason |
| --- | --- | --- |
| Codex | `/mcp` | Compact discovery has been tested. Native deferred behavior varies by version and model; use `/mcp/full` when you confirm it fits your setup. |
| Claude Code | `/mcp/full` when native tool search is active | Native search can defer schemas while preserving direct tool calls, avoiding the gateway wrappers and search round trip. |
| GitHub Copilot in VS Code | `/mcp/full` with supported models and tool search enabled | The client can search deferred tools itself, avoiding the gateway wrappers and search round trip. |
| Other clients or uncertain native discovery support | `/mcp` | Keeps the advertised catalog small without relying on native schema deferral. |

Native tool search depends on the client's version, model, and settings. See [Claude Code's MCP tool search documentation](https://code.claude.com/docs/en/mcp#scale-with-mcp-tool-search) and [VS Code's token efficiency overview](https://code.visualstudio.com/blogs/2026/06/17/improving-token-efficiency-in-github-copilot). Use `/mcp` if native tool search is unavailable or disabled and you prefer a compact catalog.

A full schema catalog is not a measurement of model prompt context: clients may defer schemas. Compact discovery adds a gateway search request and does not guarantee faster execution or lower context use. See [Tool Discovery](tool-discovery.md) for limits and tradeoffs.

## Migrate Existing Clients

- Direct-call clients using `/mcp` must change their URL to `/mcp/full`.
- Compact clients using `/mcp/discovery` must change their URL to `/mcp`.
- `/mcp/discovery` is removed, with no alias. Reconnect the client after changing the URL.

## Codex

```toml
[mcp_servers.mcp-gateway]
url = "http://localhost:8080/mcp"
http_headers = { "Authorization" = "Bearer <your-api-key>" }
```

Reconnect the gateway or restart Codex after changing the URL to refresh its active tools.

## Claude Code

This example uses the full catalog for native tool search. Use `/mcp` for compact discovery instead.

```json
{
  "mcpServers": {
    "mcp-gateway": {
      "type": "http",
      "url": "http://localhost:8080/mcp/full",
      "headers": {
        "Authorization": "Bearer <your-api-key>"
      }
    }
  }
}
```

## Client Expectations

- the gateway exposes MCP over `POST /mcp` and `POST /mcp/full`
- the gateway currently supports MCP protocol versions `2025-03-26` and `2025-11-25`
- `/mcp` advertises search and call wrappers; `/mcp/full` advertises the full permitted tool catalog
- `tools/call` is routed to the upstream that owns the tool
