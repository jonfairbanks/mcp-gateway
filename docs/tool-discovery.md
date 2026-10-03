# Tool Discovery

Both `/mcp` and `/mcp/discovery` are always available. Use `/mcp` for the full permitted catalog or native deferred tool discovery. Choose `/mcp/discovery` when you prefer a compact initial catalog, including in Codex.

Point the client's MCP connection at your chosen URL with the same bearer token, then reconnect the client to refresh its tools. The compact endpoint advertises two tools:

- `gateway_search_tools`: Search locally with `query`, optional `limit` (1–10, default 5), and optional `upstream` integration ID. Results include tool names, input schemas, and annotations.
- `gateway_call_tool`: Pass the selected `name` and an `arguments` object matching its input schema.

```json
{"name":"gateway_search_tools","arguments":{"query":"pull request review","upstream":"github","limit":5}}
{"name":"gateway_call_tool","arguments":{"name":"pull_request_read","arguments":{"method":"get_reviews","owner":"example","repo":"project","pullNumber":42}}}
```

Search is lexical. Retry with specific terms or an exact tool name when results miss. Queries are limited to 512 characters, and search text is capped at 64 KiB. Oversized schemas are omitted with a message; use `/mcp` for those tools. An extra search request adds a network round trip. The smaller initial catalog does not guarantee faster execution or lower context use than native client discovery.

Wrapped calls use the existing authentication, rate limits, deny policy, routing, cache scope, redaction, and audit path. The call wrapper conservatively advertises possible writes; consult each target's annotations before execution. Validation supports local schema references and never fetches external references. Tools requiring external references must use `/mcp`.

The index and routing table refresh together during registry warmup or discovery refresh. Warm searches make no upstream requests. Denied tools are excluded. The gateway reserves `gateway_search_tools` and `gateway_call_tool`; collisions fail registry publication. Browser clients require the same explicit `allowed_origins` setting as `/mcp`.
