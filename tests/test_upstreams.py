from __future__ import annotations

import asyncio
import json

from mcp_gateway import upstreams


def test_http_upstream_start_is_safe_under_concurrent_first_use(monkeypatch) -> None:
    created_sessions: list[object] = []

    class FakeClientSession:
        def __init__(self, timeout) -> None:
            created_sessions.append(self)

        async def close(self) -> None:
            return None

    monkeypatch.setattr(upstreams.aiohttp, "ClientSession", FakeClientSession)

    client = upstreams.HTTPUpstream("https://example.com/mcp", 1000)

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
    client = upstreams.HTTPUpstream("https://example.com/mcp", 1000)
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
    client = upstreams.HTTPUpstream("https://example.com/mcp", 1000)
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
    client = upstreams.HTTPUpstream("https://example.com/mcp", 1000)
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
