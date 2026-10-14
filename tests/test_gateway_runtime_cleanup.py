from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from mcp_gateway.gateway import Gateway
from mcp_gateway.logging import Logger
from mcp_gateway.postgres import PostgresStore
from mcp_gateway.request_context import AuthenticatedPrincipal, RequestContext
from mcp_gateway.telemetry import GatewayTelemetry
from mcp_gateway.upstreams import UpstreamResponse
from tests.gateway_test_support import RecordingLogger, _config_with_upstreams, _upstream


def _gateway(upstreams) -> Gateway:
    return Gateway(_config_with_upstreams(upstreams), PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())


@pytest.mark.parametrize("global_enabled", [False, True])
def test_failing_upstream_only_blocks_other_upstreams_when_shared_breaker_enabled(global_enabled) -> None:
    failing = _upstream("failing")
    healthy = _upstream("healthy")
    gateway = _gateway([failing, healthy])
    gateway._config.gateway.circuit_breaker_fail_threshold = 2
    gateway._config.gateway.global_circuit_breaker_enabled = global_enabled
    calls = []

    async def call(payload):
        calls.append(payload)
        return UpstreamResponse(payload={"jsonrpc": "2.0", "id": payload["id"], "result": {}}, success=True)

    gateway._http_upstreams[healthy.id] = SimpleNamespace(call=call)

    async def run_test() -> None:
        await gateway._record_health(failing.id, "tools/call", False)
        await gateway._record_health(failing.id, "tools/call", False)
        assert gateway._runtime.is_upstream_breaker_open(failing.id)
        assert not gateway._runtime.is_upstream_breaker_open(healthy.id)
        assert gateway._runtime.is_global_breaker_open() is global_enabled
        assert gateway._runtime.status_snapshot()["global_breaker"]["open"] is global_enabled
        request = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "healthy_tool"}}
        if global_enabled:
            with pytest.raises(RuntimeError, match="Global circuit breaker open"):
                await gateway._call_upstream(healthy, request)
            assert calls == []
        else:
            assert (await gateway._call_upstream(healthy, request)).success
            assert calls == [request]
            assert gateway._global_breaker == {"consecutive_failures": 0, "open_until": 0.0}
        with pytest.raises(RuntimeError, match="circuit breaker open"):
            await gateway._call_upstream(failing, request)

    asyncio.run(run_test())


def test_health_outcomes_preserve_breaker_thresholds_cooldowns_and_telemetry(monkeypatch) -> None:
    upstream = _upstream("one")
    upstream.circuit_breaker_fail_threshold = 2
    upstream.circuit_breaker_open_seconds = 5
    gateway = _gateway([upstream])
    gateway._config.gateway.global_circuit_breaker_enabled = True
    gateway._config.gateway.circuit_breaker_fail_threshold = 3
    gateway._config.gateway.circuit_breaker_open_seconds = 10
    now = 100.0
    monkeypatch.setattr("mcp_gateway.runtime_state.time.monotonic", lambda: now)
    outcomes = []
    gateway._telemetry = SimpleNamespace(record_upstream_outcome=lambda *args: outcomes.append(args))

    async def run_test() -> None:
        await gateway._record_health("one", "tools/call", False)
        assert not gateway._runtime.is_upstream_breaker_open("one")
        await gateway._record_health("one", "tools/call", False)
        assert gateway._runtime.is_upstream_breaker_open("one")
        assert not gateway._runtime.is_global_breaker_open()
        await gateway._record_health("one", "tools/list", False)
        assert gateway._runtime.is_global_breaker_open()
        assert gateway._runtime.status_snapshot()["global_breaker"]["open_seconds_remaining"] == 10
        await gateway._record_health("one", "tools/list", True)
        assert gateway._upstream_breakers["one"]["consecutive_failures"] == 0
        assert gateway._global_breaker["consecutive_failures"] == 0
        # A successful response resets failures without shortening an open cooldown.
        assert gateway._runtime.is_global_breaker_open()

    asyncio.run(run_test())
    now = 106.0
    assert not gateway._runtime.is_upstream_breaker_open("one")
    assert gateway._runtime.is_global_breaker_open()
    now = 111.0
    assert not gateway._runtime.is_global_breaker_open()
    assert outcomes == [
        ("one", "tools/call", False),
        ("one", "tools/call", False),
        ("one", "tools/list", False),
        ("one", "tools/list", True),
    ]


def test_registry_dedup_preserves_order_and_routes_denied_tools() -> None:
    upstream = _upstream("one", deny_tools=["hidden"])
    gateway = _gateway([upstream])
    first = {"name": "beta", "description": "first"}
    state = gateway._build_tool_registry_state([(upstream, [
        first, {"name": "hidden"}, {"name": "beta", "description": "duplicate"},
        {"name": "alpha"}, {"name": "hidden"}, {"name": ""}, {"name": None},
    ])])
    assert state.tools == [first, {"name": "alpha"}]
    assert state.upstream_tools == {"one": ["beta", "hidden", "alpha"]}
    assert state.registry == {"beta": "one", "hidden": "one", "alpha": "one"}
    assert state.duplicates == {}

    async def run_test() -> None:
        await gateway._apply_tool_registry_state(state)
        result = await gateway.handle(
            {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "hidden"}},
            RequestContext(client_id="client", principal=AuthenticatedPrincipal(
                subject="shared", auth_scheme="shared_bearer",
            )),
        )
        assert not result.success
        assert result.payload["error"]["data"]["policy_type"] == "deny_tools"
        assert result.payload["error"]["data"]["upstream_id"] == "one"

    asyncio.run(run_test())


def test_registry_reports_cross_upstream_collision_even_for_denied_tools() -> None:
    one = _upstream("one", deny_tools=["shared"])
    two = _upstream("two")
    gateway = _gateway([one, two])
    state = gateway._build_tool_registry_state([
        (one, [{"name": "shared"}, {"name": "shared"}]),
        (two, [{"name": "other"}, {"name": "shared"}]),
    ])
    assert state.duplicates == {"shared": {"one", "two"}}
    assert state.upstream_tools == {"one": ["shared"], "two": ["other", "shared"]}
    assert state.tools == [{"name": "other"}]


@pytest.mark.parametrize("failure", [RuntimeError("close failed"), asyncio.CancelledError()])
def test_close_attempts_every_client_and_propagates_failure(failure) -> None:
    gateway = _gateway([])
    calls = []

    def client(name, error=None):
        async def close() -> None:
            calls.append(name)
            if error is not None:
                raise error
        return SimpleNamespace(close=close)

    gateway._http_upstreams = {"one": client("one", failure), "two": client("two")}
    gateway._stdio_upstreams = {"three": client("three")}
    with pytest.raises(type(failure)) as raised:
        asyncio.run(gateway.close())
    assert raised.value is failure
    assert calls == ["one", "two", "three"]


def test_close_logs_every_failure_and_propagates_last_failure() -> None:
    gateway = _gateway([])
    logger = RecordingLogger()
    gateway._logger = logger
    first = RuntimeError("first close failed")
    second = ValueError("second close failed")

    async def first_close() -> None:
        raise first

    async def second_close() -> None:
        raise second

    gateway._http_upstreams = {"one": SimpleNamespace(close=first_close)}
    gateway._stdio_upstreams = {"two": SimpleNamespace(close=second_close)}
    with pytest.raises(ValueError) as raised:
        asyncio.run(gateway.close())
    assert raised.value is second
    assert logger.errors == [
        ("upstream_close_failed", {"upstream_id": "one", "error_type": "RuntimeError", "error": "first close failed"}),
        ("upstream_close_failed", {"upstream_id": "two", "error_type": "ValueError", "error": "second close failed"}),
    ]


def test_close_finishes_remaining_clients_after_task_cancellation() -> None:
    gateway = _gateway([])
    calls = []

    async def run_test() -> None:
        started = asyncio.Event()

        async def blocked_close() -> None:
            calls.append("one")
            started.set()
            await asyncio.Event().wait()

        async def last_close() -> None:
            calls.append("two")

        gateway._http_upstreams = {"one": SimpleNamespace(close=blocked_close)}
        gateway._stdio_upstreams = {"two": SimpleNamespace(close=last_close)}
        task = asyncio.create_task(gateway.close())
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert task.cancelled()

    asyncio.run(run_test())
    assert calls == ["one", "two"]
