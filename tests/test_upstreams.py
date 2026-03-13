from __future__ import annotations

import asyncio

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
