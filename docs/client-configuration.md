# Client Configuration

Point MCP clients at the gateway’s `POST /mcp` endpoint and include bearer auth.

## Codex

```toml
[mcp_servers.mcp-gateway]
url = "http://localhost:8080/mcp"
http_headers = { "Authorization" = "Bearer change-me" }
```

## Claude

```json
{
  "mcpServers": {
    "mcp-gateway": {
      "url": "http://localhost:8080/mcp",
      "headers": {
        "Authorization": "Bearer change-me"
      }
    }
  }
}
```

## Client Expectations

- the gateway exposes MCP over `POST /mcp`
- the gateway currently supports MCP protocol version `2025-11-25` only
- discovery requests are aggregated across upstreams
- `tools/call` is routed to the upstream that owns the tool
