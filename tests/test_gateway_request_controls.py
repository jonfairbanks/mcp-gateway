from __future__ import annotations

import asyncio

import pytest

from mcp_gateway.gateway import Gateway
from mcp_gateway.postgres import PostgresStore
from mcp_gateway.request_context import AuthenticatedPrincipal, RequestContext
from mcp_gateway.telemetry import GatewayTelemetry
from mcp_gateway.upstreams import UpstreamResponse
from tests.gateway_test_support import RecordingLogger, RecordingStore, _config_with_upstreams, _upstream


def setup_gateway(upstreams=None):
    config = _config_with_upstreams(upstreams if upstreams is not None else [_upstream("alpha"), _upstream("beta")])
    store, logger = RecordingStore(), RecordingLogger()
    gateway = Gateway(config, store, logger, GatewayTelemetry())
    return gateway, store, logger


def context():
    return RequestContext(client_id="caller", principal=AuthenticatedPrincipal(subject="gateway", auth_scheme="shared_bearer"))


def call(name):
    return {"jsonrpc": "2.0", "id": 7, "method": "tools/call", "params": {"name": name, "arguments": {}}}


@pytest.mark.parametrize("name", ["unknown_tool", "", None, [], 3])
def test_unmatched_tool_call_is_local_and_audited(name):
    gateway, store, _ = setup_gateway()
    sent = []

    async def upstream_call(*args):
        sent.append(args)
        raise AssertionError("unexpected upstream execution")

    gateway._call_upstream = upstream_call
    result = asyncio.run(gateway.handle(call(name), context()))
    assert result.payload["id"] == 7
    assert result.payload["error"]["code"] == -32602
    assert sent == []
    assert store.request_args["request_id"] == store.response_args["request_id"]
    assert store.request_args["upstream_id"] is None


@pytest.mark.parametrize("name,expected", [
    ("raw_known", "beta"), ("beta___raw_known", "beta"),
    ("alpha.unlisted", "alpha"), ("explicit_unlisted", "beta"),
])
def test_registered_alias_and_explicit_routes_are_preserved(name, expected):
    gateway, _, _ = setup_gateway([_upstream("alpha"), _upstream("beta", tool_routes=["explicit_"])])
    gateway._tool_registry = {"raw_known": "beta"}
    gateway._tool_alias_registry = {"beta___raw_known": "raw_known"}
    sent = []

    async def upstream_call(upstream, payload):
        sent.append((upstream.id, payload))
        return UpstreamResponse(payload={"jsonrpc": "2.0", "id": 7, "result": {}}, success=True)

    gateway._call_upstream = upstream_call
    assert asyncio.run(gateway.handle(call(name), context())).success
    assert sent[0][0] == expected
    assert sent[0][1]["params"]["name"] == ("raw_known" if name == "beta___raw_known" else name)


def test_generic_method_keeps_default_upstream_passthrough():
    gateway, _, _ = setup_gateway()
    routed = asyncio.run(gateway._route_request(
        {"method": "custom/extension"}, "custom/extension", {}, context(),
    ))
    assert routed.upstream.id == "alpha"


def test_no_configured_upstream_keeps_its_distinct_error():
    gateway, _, _ = setup_gateway([])
    result = asyncio.run(gateway.handle(call("unknown"), context()))
    assert result.payload["error"] == {"code": -32000, "message": "No upstream configured"}


def test_error_logging_redacts_nested_fields_without_changing_client_response():
    gateway, _, logger = setup_gateway()
    gateway._tool_registry = {"tool": "alpha"}
    error = {"code": -32020, "message": "upstream rejected request", "data": {
        "access_token": "fixture-token", "reason": "expired", "nested": [{"password": "fixture-password"}],
    }}

    async def upstream_call(_upstream, _payload):
        return UpstreamResponse(payload={"jsonrpc": "2.0", "id": 7, "error": error}, success=False)

    gateway._call_upstream = upstream_call
    result = asyncio.run(gateway.handle(call("tool"), context()))
    event = next(fields for name, fields in logger.infos if name == "mcp_response")
    assert event["error"] == {"code": -32020, "message": "upstream rejected request", "data": {
        "access_token": "[REDACTED]", "reason": "expired", "nested": [{"password": "[REDACTED]"}],
    }}
    assert result.payload["error"] == error
    assert error["data"]["access_token"] == "fixture-token"


@pytest.mark.parametrize("global_enabled", [False, True])
def test_breaker_rejections_do_not_extend_cooldown_or_count_as_upstream_failures(monkeypatch, global_enabled):
    alpha, beta = _upstream("alpha"), _upstream("beta")
    config = _config_with_upstreams([alpha, beta])
    config.gateway.global_circuit_breaker_enabled = global_enabled
    config.gateway.circuit_breaker_fail_threshold = 1
    config.gateway.circuit_breaker_open_seconds = 10
    gateway = Gateway(config, PostgresStore(""), RecordingLogger(), GatewayTelemetry())
    clock = 100.0
    monkeypatch.setattr("mcp_gateway.runtime_state.time.monotonic", lambda: clock)
    sent = []
    failing = True

    class Client:
        def __init__(self, upstream):
            self.upstream = upstream

        async def call(self, payload):
            sent.append(payload["id"])
            if failing and self.upstream.id == "alpha":
                return UpstreamResponse(payload={"error": {"code": -32000, "message": "unavailable"}}, success=False)
            return UpstreamResponse(payload={"result": {}}, success=True)

        async def notify(self, _payload):
            pass

    async def get_client(upstream):
        return Client(upstream)

    gateway._get_upstream_client = get_client

    async def execute(upstream, request_id):
        payload = call(f"{upstream.id}.tool")
        payload["id"] = request_id
        return await gateway.handle(payload, context())

    async def run():
        nonlocal clock, failing
        assert not (await execute(alpha, 1)).success
        deadline = gateway._upstream_breakers["alpha"]["open_until"]
        clock = 101.0
        assert not (await execute(alpha, 2)).success
        assert gateway._upstream_breakers["alpha"] == {"consecutive_failures": 1, "open_until": deadline}
        if global_enabled:
            assert not (await execute(beta, 3)).success
            assert "beta" not in gateway._upstream_breakers
            assert gateway._global_breaker["consecutive_failures"] == 1
        await gateway.handle({"jsonrpc": "2.0", "id": 5, "method": "resources/list"}, context())
        assert gateway._upstream_breakers["alpha"] == {"consecutive_failures": 1, "open_until": deadline}
        if global_enabled:
            assert "beta" not in gateway._upstream_breakers
            await gateway.warmup()
            assert gateway._upstream_breakers["alpha"] == {"consecutive_failures": 1, "open_until": deadline}
            assert "beta" not in gateway._upstream_breakers
        clock = 111.0
        failing = False
        assert (await execute(alpha, 4)).success
        assert gateway._upstream_breakers["alpha"]["consecutive_failures"] == 0

    asyncio.run(run())
    assert sent == ([1, 4] if global_enabled else [1, 5, 4])
