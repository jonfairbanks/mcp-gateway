from __future__ import annotations

import asyncio
from typing import Any

import pytest
from aiohttp.test_utils import TestClient, TestServer

from mcp_gateway.config import UpstreamConfig
from mcp_gateway.gateway import Gateway
from mcp_gateway.request_context import AuthenticatedPrincipal, RequestContext
from mcp_gateway.server_http import HttpServer
from mcp_gateway.telemetry import GatewayTelemetry
from mcp_gateway.upstreams import UpstreamResponse
from tests.gateway_test_support import RecordingLogger, RecordingStore, _config_with_upstreams, _upstream


class SharedStore(RecordingStore):
    def __init__(self) -> None:
        super().__init__()
        self.entries: dict[str, dict[str, Any]] = {}
        self.reads = 0

    def is_available(self) -> bool:
        return False

    async def cache_get(self, key: str) -> dict[str, Any] | None:
        self.reads += 1
        return self.entries.get(key)

    async def cache_set(self, key: str, response: dict[str, Any], ttl_seconds: int) -> None:
        self.entries[key] = response


def _make_gateway(store: SharedStore) -> tuple[Gateway, HttpServer, list[int | str]]:
    config = _config_with_upstreams([_upstream("fixture")])
    config.cache.allowed_tools = ["fixture.read"]
    config.gateway.rate_limit_per_minute = 10000
    logger = RecordingLogger()
    telemetry = GatewayTelemetry()
    gateway = Gateway(config, store, logger, telemetry)
    calls: list[int | str] = []

    async def call(upstream: UpstreamConfig, payload: dict[str, Any]) -> UpstreamResponse:
        calls.append(payload["id"])
        return UpstreamResponse(
            {
                "jsonrpc": "2.0",
                "id": payload["id"],
                "result": {
                    "content": [{"type": "text", "text": "cached value"}],
                    "structuredContent": {"id": "application-id"},
                },
                "_meta": {"example": "preserved"},
            },
            True,
        )

    gateway._call_upstream = call  # type: ignore[method-assign]
    return gateway, HttpServer(config, gateway, logger, telemetry), calls


def _request(request_id: int | str) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tools/call",
        "params": {"name": "fixture.read", "arguments": {"query": "same"}},
    }


@pytest.mark.parametrize("second_id", [0, 100, "second-request"])
def test_memory_cache_ids_through_authenticated_http(second_id: int | str) -> None:
    async def run() -> None:
        store = SharedStore()
        gateway, server, calls = _make_gateway(store)
        try:
            async with TestClient(TestServer(server.build_app())) as client:
                replies = []
                for request_id in [99, second_id, "third-request"]:
                    async with client.post(
                        "/mcp", json=_request(request_id), headers={"Authorization": "Bearer secret"}
                    ) as response:
                        assert response.status == 200
                        replies.append(await response.json())
                assert [row["id"] for row in replies] == [99, second_id, "third-request"]
                assert all(row["result"] == replies[0]["result"] for row in replies)
                assert all(row["_meta"] == {"example": "preserved"} for row in replies)
                assert replies[1]["result"]["structuredContent"]["id"] == "application-id"
                assert calls == [99]
                assert next(iter(store.entries.values()))["id"] == 99
                assert next(iter(gateway._memory_cache._entries.values())).value["id"] == 99
        finally:
            await gateway.close()

    asyncio.run(run())


def test_shared_store_cache_ids_across_gateway_instances() -> None:
    async def run() -> None:
        store = SharedStore()
        gateway_a, _, calls_a = _make_gateway(store)
        gateway_b, _, calls_b = _make_gateway(store)
        principal = AuthenticatedPrincipal(subject="gateway", auth_scheme="shared_bearer")
        context = RequestContext(client_id="lab", principal=principal)
        try:
            first = await gateway_a.handle(_request(99), context)
            second, third = await asyncio.gather(
                gateway_b.handle(_request("replica-b"), context),
                gateway_b.handle(_request(0), context),
            )
            assert [first.payload["id"], second.payload["id"], third.payload["id"]] == [99, "replica-b", 0]
            assert second.cache_hit and third.cache_hit
            assert calls_a == [99] and calls_b == []
            assert next(iter(store.entries.values()))["id"] == 99
            assert store.reads == 3
        finally:
            await gateway_a.close()
            await gateway_b.close()

    asyncio.run(run())
