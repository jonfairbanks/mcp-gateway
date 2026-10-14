from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from mcp_gateway.gateway import Gateway
from mcp_gateway.logging import Logger
from mcp_gateway.postgres import PostgresStore
from mcp_gateway.server_http import HttpServer
from mcp_gateway.telemetry import GatewayTelemetry
from tests.gateway_test_support import _config_with_upstreams

ALLOWED_ORIGIN = "https://client.example"


def _server() -> HttpServer:
    config = _config_with_upstreams([])
    config.gateway.allow_unauthenticated = True
    config.gateway.api_key = ""
    config.gateway.allowed_origins = [ALLOWED_ORIGIN]
    config.gateway.request_max_bytes = 128
    telemetry = GatewayTelemetry()
    gateway = Gateway(config, PostgresStore(""), Logger(False), telemetry)
    gateway.handle = AsyncMock()
    return HttpServer(config, gateway, Logger(False), telemetry)


@pytest.mark.parametrize("endpoint", ["/mcp", "/mcp/full"])
@pytest.mark.parametrize("origin", [ALLOWED_ORIGIN, None, "https://denied.example"])
def test_oversized_mcp_request_respects_origin_policy(endpoint, origin):
    async def run():
        server = _server()
        try:
            async with TestClient(TestServer(server.build_app())) as client:
                response = await client.post(
                    endpoint,
                    headers={"Origin": origin} if origin else {},
                    json={"jsonrpc": "2.0", "id": 1, "method": "ping", "padding": "x" * 256},
                )
                body = await response.text()
                if origin == "https://denied.example":
                    assert response.status == 403
                    assert body == "Origin not allowed"
                else:
                    assert response.status == 413
                    assert "Maximum request body size 128 exceeded" in body
                if origin == ALLOWED_ORIGIN:
                    assert response.headers["Access-Control-Allow-Origin"] == ALLOWED_ORIGIN
                    assert response.headers["Access-Control-Allow-Methods"] == "POST, OPTIONS"
                else:
                    assert "Access-Control-Allow-Origin" not in response.headers
                assert "Origin" in response.headers["Vary"]
                server._gateway.handle.assert_not_awaited()
        finally:
            await server._telemetry.close()

    asyncio.run(run())


@pytest.mark.parametrize("endpoint", ["/mcp", "/mcp/full", "/error"])
def test_http_exception_preserves_response_and_limits_cors_to_mcp(endpoint):
    async def raise_error(request):
        raise web.HTTPTooManyRequests(
            text="Please retry later.",
            content_type="application/problem+json",
            headers={"Retry-After": "17", "Vary": "Accept-Encoding"},
        )

    async def run():
        server = _server()
        server.mcp_post_handler = raise_error
        app = server.build_app()
        app.router.add_post("/error", raise_error)
        try:
            async with TestClient(TestServer(app)) as client:
                response = await client.post(endpoint, headers={"Origin": ALLOWED_ORIGIN})
                assert response.status == 429
                assert await response.text() == "Please retry later."
                assert response.headers["Retry-After"] == "17"
                assert response.content_type == "application/problem+json"
                vary = set(value.strip() for value in response.headers["Vary"].split(","))
                assert "Accept-Encoding" in vary
                if endpoint == "/error":
                    assert vary == {"Accept-Encoding"}
                    assert not any(header.lower().startswith("access-control-") for header in response.headers)
                else:
                    assert "Origin" in vary
                    assert response.headers["Access-Control-Allow-Origin"] == ALLOWED_ORIGIN
        finally:
            await server._telemetry.close()

    asyncio.run(run())


@pytest.mark.parametrize("endpoint", ["/mcp", "/mcp/full"])
def test_method_not_allowed_keeps_allow_header_and_cors(endpoint):
    async def run():
        server = _server()
        try:
            async with TestClient(TestServer(server.build_app())) as client:
                response = await client.put(endpoint, headers={"Origin": ALLOWED_ORIGIN})
                assert response.status == 405
                assert await response.text() == "Method Not Allowed"
                assert set(response.headers["Allow"].split(", ")) == {"DELETE", "GET", "HEAD", "OPTIONS", "POST"}
                assert response.headers["Access-Control-Allow-Origin"] == ALLOWED_ORIGIN
        finally:
            await server._telemetry.close()

    asyncio.run(run())
