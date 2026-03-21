from __future__ import annotations

import json
import os
import sys
from typing import Any

from mcp_gateway.protocol import CURRENT_PROTOCOL_VERSION

TOOL_NAME = os.getenv("FAKE_STDIO_TOOL_NAME", "stdio.echo")
EXIT_AFTER_RESPONSES = int(os.getenv("FAKE_STDIO_EXIT_AFTER_RESPONSES", "0"))
response_count = 0


def _write(payload: dict[str, Any]) -> None:
    global response_count
    sys.stdout.write(json.dumps(payload) + "\n")
    sys.stdout.flush()
    response_count += 1
    if EXIT_AFTER_RESPONSES and response_count >= EXIT_AFTER_RESPONSES:
        raise SystemExit(0)


for raw_line in sys.stdin:
    if not raw_line.strip():
        continue
    request = json.loads(raw_line)
    method = request.get("method")
    request_id = request.get("id")

    if method == "notifications/initialized":
        continue

    if method == "initialize":
        _write(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "protocolVersion": request.get("params", {}).get("protocolVersion", CURRENT_PROTOCOL_VERSION),
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "fake-stdio-upstream", "version": "0.1.0"},
                },
            }
        )
        continue

    if method == "tools/list":
        _write(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "tools": [
                        {
                            "name": TOOL_NAME,
                            "description": "Echoes the provided value.",
                            "inputSchema": {
                                "type": "object",
                                "properties": {"value": {"type": "string"}},
                                "required": ["value"],
                            },
                        }
                    ]
                },
            }
        )
        continue

    if method == "tools/call":
        params = request.get("params") or {}
        arguments = params.get("arguments") or {}
        _write(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": f"{TOOL_NAME}:{arguments.get('value', '')}",
                        }
                    ],
                    "isError": False,
                },
            }
        )
        continue

    _write(
        {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {
                "code": -32601,
                "message": f"Unknown method: {method}",
            },
        }
    )
