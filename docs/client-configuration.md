# Client Configuration

Point MCP clients at the gateway’s `POST /mcp` endpoint and include bearer auth.

## Codex

```toml
[mcp_servers.mcp-gateway]
url = "http://localhost:8080/mcp"
http_headers = { "Authorization" = "Bearer <your-api-key>" }
```

## Claude

```json
{
  "mcpServers": {
    "mcp-gateway": {
      "url": "http://localhost:8080/mcp",
      "headers": {
        "Authorization": "Bearer <your-api-key>"
      }
    }
  }
}
```

## Client Expectations

- the gateway exposes MCP over `POST /mcp`
- the gateway currently supports MCP protocol versions `2025-03-26` and `2025-11-25`
- discovery requests are aggregated across upstreams
- `tools/call` is routed to the upstream that owns the tool

## Concurrent Calls

After initialization, send independent calls as separate concurrent `POST /mcp` requests. Wait for each dependent call to finish before sending the next one.

MCP `2025-11-25` requires [one JSON-RPC message per POST](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports#sending-messages-to-the-server). The gateway keeps sequential batch handling for legacy clients, so a batch does not speed up independent calls.

Upstream `max_in_flight` limits still apply. HTTP upstreams allow concurrent calls unless `http_serialize_requests` is enabled; each stdio upstream processes calls one at a time.
