# Client Configuration

Use `/mcp/discovery` for a smaller initial tool catalog, including in Codex. First set `gateway.tool_discovery_enabled: true` and restart the gateway. The examples below use this compact endpoint with bearer auth.

Use `/mcp` instead for the full permitted catalog or native client tool discovery. See [Tool Discovery](tool-discovery.md) for search limits and tradeoffs.

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

- the gateway exposes MCP over `POST /mcp` and the optional `POST /mcp/discovery` endpoint
- the gateway currently supports MCP protocol versions `2025-03-26` and `2025-11-25`
- `/mcp` advertises the full permitted tool catalog; `/mcp/discovery` advertises search and call wrappers
- `tools/call` is routed to the upstream that owns the tool
