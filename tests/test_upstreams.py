from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

from mcp_gateway import upstreams

FIXTURE_STDIO_UPSTREAM = Path(__file__).resolve().parent / "fixtures" / "fake_stdio_upstream.py"


def test_http_upstream_start_is_safe_under_concurrent_first_use(monkeypatch) -> None:
    created_sessions: list[object] = []

    class FakeClientSession:
        def __init__(self, timeout) -> None:
            created_sessions.append(self)

        async def close(self) -> None:
            return None

    monkeypatch.setattr(upstreams.aiohttp, "ClientSession", FakeClientSession)

    client = upstreams.StreamableHTTPUpstream("https://example.com/mcp", 1000)

    async def run_test() -> None:
        await asyncio.gather(client.start(), client.start(), client.start())
        await client.close()

    asyncio.run(run_test())

    assert len(created_sessions) == 1


class FakeResponse:
    def __init__(self, status: int, body: str, headers: dict[str, str] | None = None) -> None:
        self.status = status
        self._body = body
        self.headers = headers or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def text(self) -> str:
        return self._body

    async def read(self) -> bytes:
        return self._body.encode("utf-8")


class FakeClientSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = list(responses)
        self.requests: list[dict[str, object]] = []

    def post(self, endpoint: str, json: dict, headers: dict[str, str]):
        self.requests.append({"endpoint": endpoint, "json": json, "headers": headers})
        return self.responses.pop(0)

    async def close(self) -> None:
        return None


def test_http_upstream_call_parses_plain_json_response() -> None:
    client = upstreams.StreamableHTTPUpstream("https://example.com/mcp", 1000)
    client._session = FakeClientSession(
        [
            FakeResponse(
                200,
                json.dumps({"jsonrpc": "2.0", "id": "req-1", "result": {"ok": True}}),
            )
        ]
    )

    response = asyncio.run(client.call({"jsonrpc": "2.0", "id": "req-1", "method": "initialize", "params": {}}))

    assert response.success is True
    assert response.payload["result"] == {"ok": True}


def test_http_upstream_call_parses_sse_response_and_ignores_notifications() -> None:
    client = upstreams.StreamableHTTPUpstream("https://example.com/mcp", 1000)
    client._session = FakeClientSession(
        [
            FakeResponse(
                200,
                "\n".join(
                    [
                        'event: message',
                        'data: {"jsonrpc":"2.0","method":"notifications/progress","params":{"progress":0.5}}',
                        "",
                        'event: message',
                        'data: {"jsonrpc":"2.0","id":"req-1","result":{"capabilities":{"tools":{}}}}',
                        "",
                    ]
                ),
                headers={"MCP-Session-ID": "session-123"},
            )
        ]
    )

    response = asyncio.run(
        client.call(
            {
                "jsonrpc": "2.0",
                "id": "req-1",
                "method": "initialize",
                "params": {"protocolVersion": "2024-11-05"},
            }
        )
    )

    assert response.success is True
    assert response.payload["id"] == "req-1"
    assert response.payload["result"] == {"capabilities": {"tools": {}}}
    assert client._session_id == "session-123"


def test_http_upstream_call_errors_when_sse_body_has_no_matching_response() -> None:
    client = upstreams.StreamableHTTPUpstream("https://example.com/mcp", 1000)
    client._session = FakeClientSession(
        [
            FakeResponse(
                200,
                "\n".join(
                    [
                        'event: message',
                        'data: {"jsonrpc":"2.0","method":"notifications/message","params":{"level":"info"}}',
                        "",
                    ]
                ),
            )
        ]
    )

    response = asyncio.run(client.call({"jsonrpc": "2.0", "id": "req-1", "method": "tools/list", "params": {}}))

    assert response.success is False
    assert response.payload["error"]["message"] == "HTTP upstream returned SSE body without matching JSON-RPC response (status 200)"


def test_stdio_upstream_restarts_after_child_exit() -> None:
    client = upstreams.StdioUpstream(
        [sys.executable, str(FIXTURE_STDIO_UPSTREAM)],
        {
            "FAKE_STDIO_TOOL_NAME": "stdio.echo",
            "FAKE_STDIO_EXIT_AFTER_RESPONSES": "1",
        },
        None,
        1000,
        1024 * 1024,
        "stdio-fixture",
    )

    async def run_test() -> tuple[upstreams.UpstreamResponse, upstreams.UpstreamResponse, int, int]:
        first = await client.call(
            {
                "jsonrpc": "2.0",
                "id": "req-1",
                "method": "tools/call",
                "params": {"name": "stdio.echo", "arguments": {"value": "first"}},
            }
        )
        assert client._process is not None
        first_pid = client._process.pid
        await asyncio.wait_for(client._process.wait(), timeout=2.0)

        second = await client.call(
            {
                "jsonrpc": "2.0",
                "id": "req-2",
                "method": "tools/call",
                "params": {"name": "stdio.echo", "arguments": {"value": "second"}},
            }
        )
        assert client._process is not None
        second_pid = client._process.pid
        await client.close()
        return first, second, first_pid, second_pid

    first, second, first_pid, second_pid = asyncio.run(run_test())

    assert first.success is True
    assert first.payload["result"]["content"][0]["text"] == "stdio.echo:first"
    assert second.success is True
    assert second.payload["result"]["content"][0]["text"] == "stdio.echo:second"
    assert second_pid != first_pid
