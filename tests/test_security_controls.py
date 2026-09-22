from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from aiohttp.test_utils import TestClient, TestServer

from mcp_gateway.config import AppConfig, GatewayConfig, load_config
from mcp_gateway.gateway import Gateway
from mcp_gateway.jsonrpc import normalize_params
from mcp_gateway.logging import Logger
from mcp_gateway.postgres import PostgresStore
from mcp_gateway.server_http import HttpServer
from mcp_gateway.telemetry import GatewayTelemetry
from mcp_gateway.upstreams import StdioUpstream, StreamableHTTPUpstream, UpstreamResponse
from tests.gateway_test_support import _config_with_upstreams, _upstream
from tests.test_upstreams import FakeClientSession, FakeResponse


@pytest.mark.parametrize("failure", ["eof", "write", "drain", "reset"])
def test_stdio_ambiguous_failure_is_not_replayed(failure):
    async def run():
        client = StdioUpstream(["unused"], {}, None, 1000, 65536, "fixture")
        stdin = SimpleNamespace(write=Mock(), drain=AsyncMock())
        stdout = SimpleNamespace(readline=AsyncMock(return_value=b""))
        if failure == "write":
            stdin.write.side_effect = BrokenPipeError()
        if failure in {"drain", "reset"}:
            stdin.drain.side_effect = BrokenPipeError() if failure == "drain" else ConnectionResetError()
        client._process = SimpleNamespace(stdin=stdin, stdout=stdout)
        client.start = AsyncMock()
        client.close = AsyncMock()
        with pytest.raises(RuntimeError, match="outcome unknown"):
            await client.call({"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "fixture"}})
        assert stdin.write.call_count == 1
        client.start.assert_awaited_once()
        client.close.assert_awaited_once()
    asyncio.run(run())


@pytest.mark.parametrize("explicit", [{}, {"SERVICE_TOKEN": "assigned", "LANG": "C"}])
def test_stdio_only_inherits_runtime_environment(monkeypatch, explicit):
    monkeypatch.setenv("DATABASE_URL", "fixture-database")
    monkeypatch.setenv("MCP_GATEWAY_API_KEY", "fixture-key")
    monkeypatch.setenv("HTTP_VENDOR_TOKEN", "fixture-http-token")
    monkeypatch.setenv("NODE_OPTIONS", "fixture-options")
    monkeypatch.setenv("PATH", "/fixture/bin")
    monkeypatch.setenv("HOME", "/fixture/home")
    process = SimpleNamespace(returncode=0, stderr=None)
    spawn = AsyncMock(return_value=process)
    monkeypatch.setattr("mcp_gateway.upstreams.asyncio.create_subprocess_exec", spawn)

    async def run():
        client = StdioUpstream(["fixture"], explicit, None, 1000, 65536, "fixture")
        await client.start()
        await client.close()
    asyncio.run(run())
    env = spawn.call_args.kwargs["env"]
    assert env["PATH"] == "/fixture/bin"
    assert env["HOME"] == "/fixture/home"
    assert not {"DATABASE_URL", "MCP_GATEWAY_API_KEY", "HTTP_VENDOR_TOKEN", "NODE_OPTIONS"}.intersection(env)
    assert all(env[key] == value for key, value in explicit.items())


@pytest.mark.parametrize("mode", ["call", "accepted", "notify", "notify_error"])
def test_http_body_limit_applies_to_all_response_paths(mode):
    client = StreamableHTTPUpstream("https://example.test/mcp", 1000, response_max_bytes=8)
    status = {"call": 200, "accepted": 202, "notify": 200, "notify_error": 400}[mode]
    client._session = FakeClientSession([FakeResponse(status, "123456789")])
    operation = client.notify if mode.startswith("notify") else client.call
    with pytest.raises(RuntimeError, match="http_response_max_bytes"):
        asyncio.run(operation({"id": 1, "method": "ping"}))


def test_http_body_limit_accepts_exact_boundary():
    body = '{"id":1,"result":{}}'
    client = StreamableHTTPUpstream("https://example.test/mcp", 1000, response_max_bytes=len(body))
    client._session = FakeClientSession([FakeResponse(200, body)])
    assert asyncio.run(client.call({"id": 1, "method": "ping"})).success


@pytest.mark.parametrize("limit,body", [
    ("MAX_SSE_LINES", ": comment\n: comment\n: comment\n"),
    ("MAX_SSE_EVENTS", "data: {}\n\ndata: {}\n\ndata: {}\n\n"),
])
def test_sse_parser_enforces_independent_budgets(monkeypatch, limit, body):
    monkeypatch.setattr(StreamableHTTPUpstream, limit, 2)
    with pytest.raises(RuntimeError, match="limit exceeded"):
        list(StreamableHTTPUpstream._parse_sse_events(body))


def test_sse_parser_stops_at_match_and_preserves_multiline_data(monkeypatch):
    monkeypatch.setattr(StreamableHTTPUpstream, "MAX_SSE_EVENTS", 1)
    body = ': heartbeat\r\ndata: {"id": 7,\r\ndata: "result": {"ok": true}}\r\n\r\ndata: {}\r\n\r\n'
    assert StreamableHTTPUpstream._extract_sse_payload(body, 7) == {"id": 7, "result": {"ok": True}}
    assert StreamableHTTPUpstream._extract_sse_payload('data: {"result": {}}\n\n', None) == {"result": {}}


@pytest.mark.parametrize("arguments", [
    {"_meta": {"progressToken": "value"}},
    {"nested": [{"_meta": {"progressToken": "value"}}]},
])
def test_cache_preserves_nested_application_metadata(arguments):
    first = {"name": "fixture", "arguments": arguments, "_meta": {"progressToken": 1}}
    second = json.loads(json.dumps(first).replace('"value"', '"other"'))
    assert normalize_params(first) != normalize_params(second)
    assert first["_meta"] == {"progressToken": 1}
    assert normalize_params(first) == normalize_params({**first, "_meta": {"progressToken": 2}})


def test_cache_uses_fresh_namespace_for_scoped_and_shared_entries():
    config = _config_with_upstreams([_upstream()])
    gateway = Gateway(config, PostgresStore(""), Logger(False), GatewayTelemetry())
    for shared in [[], ["fixture"]]:
        config.cache.globally_shareable_tools = shared
        assert gateway._cache_key(config.upstreams[0], "tools/call", "fixture", {}, "owner").startswith("v2:")


@pytest.mark.parametrize("field", ["public_tools_catalog", "public_metrics", "allow_unauthenticated"])
@pytest.mark.parametrize("value,expected", [(True, True), (False, False), ("true", True), ("false", False), ("FALSE", False)])
def test_access_flags_parse_explicit_booleans(field, value, expected):
    assert getattr(GatewayConfig.from_dict({field: value}), field) is expected


@pytest.mark.parametrize("value", ["0", "no", "yes", "", 1, [], {}])
def test_access_flags_reject_ambiguous_values(value):
    with pytest.raises(ValueError, match="must be true or false"):
        GatewayConfig.from_dict({"public_tools_catalog": value})


def test_environment_expanded_false_stays_private(tmp_path, monkeypatch):
    monkeypatch.delenv("PUBLIC_TOOLS_CATALOG", raising=False)
    path = tmp_path / "config.yaml"
    path.write_text('gateway:\n  public_tools_catalog: "${PUBLIC_TOOLS_CATALOG:-false}"\n')
    assert load_config(str(path)).gateway.public_tools_catalog is False
    monkeypatch.setenv("PUBLIC_TOOLS_CATALOG", "false")
    assert load_config(str(path)).gateway.public_tools_catalog is False


@pytest.mark.parametrize("origin", ["*", "null", "https://example.test/path", "https://user@example.test", "https://*.test", "https://example.test:bad"])
def test_allowed_origins_require_exact_web_origins(origin):
    with pytest.raises(ValueError):
        AppConfig.from_dict({"gateway": {"allowed_origins": [origin]}})


@pytest.mark.parametrize("method", ["POST", "OPTIONS", "GET", "DELETE"])
def test_mcp_rejects_unapproved_origins_before_dispatch(method):
    async def run():
        config = _config_with_upstreams([_upstream()])
        config.gateway.allow_unauthenticated = True
        config.gateway.allowed_origins = ["https://client.example"]
        telemetry = GatewayTelemetry()
        gateway = Gateway(config, PostgresStore(""), Logger(False), telemetry)
        gateway.handle = AsyncMock()
        server = HttpServer(config, gateway, Logger(False), telemetry)
        async with TestClient(TestServer(server.build_app())) as client:
            for origin in ["https://other.example", "null", "https://client.example.evil"]:
                response = await client.request(method, "/mcp", headers={"Origin": origin})
                assert response.status == 403
                assert "Access-Control-Allow-Origin" not in response.headers
            response = await client.options("/mcp", headers={"Origin": "https://client.example", "Access-Control-Request-Headers": "x-unapproved"})
            assert response.status == 204
            assert response.headers["Access-Control-Allow-Origin"] == "https://client.example"
            assert "x-unapproved" not in response.headers["Access-Control-Allow-Headers"]
            native = await client.options("/mcp")
            assert native.status == 204
            assert "Access-Control-Allow-Origin" not in native.headers
        gateway.handle.assert_not_awaited()
        await telemetry.close()
    asyncio.run(run())


def test_denied_tool_schemas_are_hidden_in_fresh_and_cached_discovery():
    async def run():
        upstream = _upstream(deny_tools=["hidden"])
        config = _config_with_upstreams([upstream])
        gateway = Gateway(config, PostgresStore(""), Logger(False), GatewayTelemetry())
        tools = [{"name": "allowed", "inputSchema": {}}, {"name": "hidden", "description": "private", "inputSchema": {}}]
        gateway._call_upstream = AsyncMock(return_value=UpstreamResponse({"result": {"tools": tools}}, True))
        fresh, success, _ = await gateway._aggregate_list({"id": 1, "method": "tools/list"}, "tools/list")
        assert success
        assert fresh["result"]["tools"] == [tools[0]]
        cached = await gateway._cached_tools_list_response(2)
        assert cached["result"]["tools"] == [tools[0]]
        assert gateway._tool_registry["hidden"] == upstream.id
        assert gateway._deny(upstream, "hidden")
    asyncio.run(run())
