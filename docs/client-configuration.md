# Client Configuration

Both `/mcp` and `/mcp/discovery` are always available with the same bearer auth. Choose `/mcp` for the full permitted catalog or native deferred tool discovery. Choose `/mcp/discovery` for a compact initial catalog with search and call wrappers, including in Codex.

The examples below use `/mcp/discovery`; change the URL to `/mcp` if that better fits your client. Compact discovery adds a search request before calling a tool and does not guarantee faster execution or lower context use than native client discovery. See [Tool Discovery](tool-discovery.md) for limits and tradeoffs.

## Codex

```toml
[mcp_servers.mcp-gateway]
url = "http://localhost:8080/mcp/discovery"
http_headers = { "Authorization" = "Bearer <your-api-key>" }
```

Reconnect the gateway or restart Codex after changing the URL to refresh its active tools.

## Claude

```json
{
  "mcpServers": {
    "mcp-gateway": {
      "url": "http://localhost:8080/mcp/discovery",
      "headers": {
        "Authorization": "Bearer <your-api-key>"
      }
    }
  }
}
```

## Client Expectations

- the gateway exposes MCP over `POST /mcp` and `POST /mcp/discovery`
- the gateway currently supports MCP protocol versions `2025-03-26` and `2025-11-25`
- `/mcp` advertises the full permitted tool catalog; `/mcp/discovery` advertises search and call wrappers
- `tools/call` is routed to the upstream that owns the tool
