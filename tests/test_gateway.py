from __future__ import annotations
import asyncio
from types import SimpleNamespace
from uuid import UUID

from aiohttp import web

from mcp_gateway.config import AppConfig, CacheConfig, GatewayConfig, LoggingConfig, UpstreamConfig
from mcp_gateway.gateway import Gateway, GatewayResult
from mcp_gateway.logging import Logger
from mcp_gateway.postgres import PostgresStore
from mcp_gateway.router import build_routes, select_upstream
from mcp_gateway.server_http import HttpServer, SseSession
from mcp_gateway.telemetry import GatewayTelemetry
from mcp_gateway.upstreams import UpstreamResponse


def _upstream(
    upstream_id: str = "notion",
    *,
    deny_tools: list[str] | None = None,
    tool_routes: list[str] | None = None,
) -> UpstreamConfig:
    return UpstreamConfig(
        id=upstream_id,
        name=upstream_id,
        transport="http_sse",
        endpoint="http://example.com/rpc",
        http_headers={},
        bearer_token_env_var=None,
        http_serialize_requests=False,
        command=None,
        env={},
        cwd=None,
        timeout_ms=1000,
        stdio_read_limit_bytes=1024 * 1024,
        max_in_flight=10,
        deny_tools=deny_tools or [],
        cache_ttl_minutes=None,
        circuit_breaker_fail_threshold=None,
        circuit_breaker_open_seconds=None,
        tool_routes=tool_routes or [],
    )


def _config_with_upstreams(upstreams: list[UpstreamConfig]) -> AppConfig:
    return AppConfig(
        gateway=GatewayConfig(
            listen_host="0.0.0.0",
            listen_port=8080,
            api_key="secret",
            allow_unauthenticated=False,
            trusted_proxies=["127.0.0.1", "::1"],
            request_max_bytes=2 * 1024 * 1024,
            rate_limit_per_minute=120,
            circuit_breaker_fail_threshold=10,
            circuit_breaker_open_seconds=30,
            sse_queue_max_messages=100,
            max_sse_sessions=1000,
        ),
        logging=LoggingConfig(stdout_json=False, extra_redact_fields=[]),
        cache=CacheConfig(enabled=True, max_entries=100, default_ttl_minutes=60, client_scoped_tools=[]),
        upstreams=upstreams,
    )


class RecordingStore:
    def __init__(self) -> None:
        self.request_args = None
        self.response_args = None

    async def log_request(self, **kwargs) -> None:
        self.request_args = kwargs

    async def log_response(self, **kwargs) -> None:
        self.response_args = kwargs

    async def log_denial(self, *args, **kwargs) -> None:
        return None

    async def cache_get(self, cache_key: str):
        return None

    async def cache_set(self, cache_key: str, response, ttl_seconds: int) -> None:
        return None


class RecordingLogger:
    def __init__(self) -> None:
        self.warnings: list[tuple[str, dict]] = []
        self.errors: list[tuple[str, dict]] = []
        self.infos: list[tuple[str, dict]] = []

    def warn(self, event: str, **fields) -> None:
        self.warnings.append((event, fields))

    def error(self, event: str, **fields) -> None:
        self.errors.append((event, fields))

    def info(self, event: str, **fields) -> None:
        self.infos.append((event, fields))


def test_denylist_matches_tool_name() -> None:
    config = _config_with_upstreams([_upstream(deny_tools=["notion.createPage"])])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    upstream = config.upstreams[0]
    assert gateway._deny(upstream, "notion.createPage") is not None
    assert gateway._deny(upstream, "notion.updatePage") is None


def test_cache_key_normalization() -> None:
    config = _config_with_upstreams([_upstream()])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    upstream = config.upstreams[0]
    params_a = {"b": 2, "a": 1}
    params_b = {"a": 1, "b": 2}
    key_a = gateway._cache_key(upstream, "tools/call", "notion.get", params_a, None)
    key_b = gateway._cache_key(upstream, "tools/call", "notion.get", params_b, None)
    assert key_a == key_b


def test_http_auth_header_parsing() -> None:
    config = _config_with_upstreams([_upstream()])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    request_ok = SimpleNamespace(headers={"Authorization": "Bearer secret"})
    request_bad = SimpleNamespace(headers={"Authorization": "Bearer wrong"})

    assert server._authorize(request_ok) is None
    assert server._authorize(request_bad) is not None


def test_client_id_ignores_untrusted_x_client_id() -> None:
    config = _config_with_upstreams([_upstream()])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    request = SimpleNamespace(
        headers={"X-Client-Id": "spoofed"},
        remote="203.0.113.10",
    )
    assert server._client_id(request) == "203.0.113.10"


def test_client_id_accepts_trusted_proxy_x_client_id() -> None:
    config = _config_with_upstreams([_upstream()])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    request = SimpleNamespace(
        headers={"X-Client-Id": "tenant-123"},
        remote="127.0.0.1",
    )
    assert server._client_id(request) == "tenant-123"


def test_select_upstream_uses_longest_prefix_match() -> None:
    upstreams = [
        _upstream("default", tool_routes=["notion."]),
        _upstream("notion-special", tool_routes=["notion.api."]),
    ]
    routes = build_routes(upstreams)
    selected = select_upstream(upstreams, routes, "notion.api.create")
    assert selected is not None
    assert selected.id == "notion-special"


def test_redacts_sensitive_request_and_response_fields_before_storage() -> None:
    config = _config_with_upstreams([_upstream()])
    config.logging.extra_redact_fields = ["custom_secret"]
    store = RecordingStore()
    gateway = Gateway(config, store, Logger(stdout_json=False), GatewayTelemetry())

    asyncio.run(
        gateway._safe_log_request(
            request_id=UUID("00000000-0000-0000-0000-000000000000"),
            method="tools/call",
            params={"token": "abc", "nested": {"custom_secret": "xyz", "ok": 1}},
            raw_request={"headers": {"Authorization": "Bearer value"}, "password": "pw"},
            upstream_id="notion",
            tool_name="notion.get",
            client_id="client-1",
            cache_key=None,
        )
    )
    asyncio.run(
        gateway._safe_log_response(
            response_id=UUID("00000000-0000-0000-0000-000000000001"),
            request_id=UUID("00000000-0000-0000-0000-000000000000"),
            success=False,
            latency_ms=12,
            cache_hit=False,
            response={"error": {"data": {"access_token": "secret-token", "value": 1}}},
        )
    )

    assert store.request_args is not None
    assert store.request_args["params"]["token"] == "[REDACTED]"
    assert store.request_args["params"]["nested"]["custom_secret"] == "[REDACTED]"
    assert store.request_args["params"]["nested"]["ok"] == 1
    assert store.request_args["raw_request"]["headers"]["Authorization"] == "[REDACTED]"
    assert store.request_args["raw_request"]["password"] == "[REDACTED]"

    assert store.response_args is not None
    assert store.response_args["response"]["error"]["data"]["access_token"] == "[REDACTED]"
    assert store.response_args["response"]["error"]["data"]["value"] == 1


def test_warmup_fails_on_duplicate_tool_names() -> None:
    config = _config_with_upstreams([_upstream("one"), _upstream("two")])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())

    async def fake_call(upstream, payload):
        if payload["method"] == "initialize":
            return UpstreamResponse(payload={"jsonrpc": "2.0", "id": payload.get("id"), "result": {}}, success=True)
        if payload["method"] == "tools/list":
            return UpstreamResponse(
                payload={
                    "jsonrpc": "2.0",
                    "id": payload.get("id"),
                    "result": {"tools": [{"name": "shared.tool"}]},
                },
                success=True,
            )
        raise AssertionError(payload["method"])

    async def fake_notify(upstream, payload) -> None:
        return None

    gateway._call_upstream = fake_call  # type: ignore[method-assign]
    gateway._notify_upstream = fake_notify  # type: ignore[method-assign]

    try:
        asyncio.run(gateway.warmup())
    except RuntimeError as exc:
        assert "Duplicate tool names detected across upstreams" in str(exc)
        assert "shared.tool" in str(exc)
        return
    raise AssertionError("warmup should fail on duplicate tool names")


def test_enqueue_session_payload_closes_session_when_queue_is_full() -> None:
    config = _config_with_upstreams([_upstream()])
    config.gateway.sse_queue_max_messages = 1
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    async def write_eof() -> None:
        return None

    session = SseSession(SimpleNamespace(write_eof=write_eof), max_messages=1)
    session.queue.put_nowait("existing")
    server._sessions["session-1"] = session

    enqueued = asyncio.run(server._enqueue_session_payload("session-1", "later"))

    assert enqueued is False
    assert "session-1" not in server._sessions
    assert session.closed is True


def test_sse_handler_rejects_when_session_capacity_is_exceeded() -> None:
    config = _config_with_upstreams([_upstream()])
    config.gateway.max_sse_sessions = 1
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())
    server._sessions["existing"] = SseSession(SimpleNamespace(write_eof=lambda: None), max_messages=1)

    request = SimpleNamespace(headers={"Authorization": "Bearer secret"}, remote="127.0.0.1")

    response = asyncio.run(server.sse_handler(request))

    assert isinstance(response, web.Response)
    assert response.status == 503


def test_message_handler_returns_retryable_error_when_session_queue_is_full() -> None:
    config = _config_with_upstreams([_upstream()])
    config.gateway.sse_queue_max_messages = 1
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    async def write_eof() -> None:
        return None

    session = SseSession(SimpleNamespace(write_eof=write_eof), max_messages=1)
    session.queue.put_nowait("existing")
    server._sessions["session-1"] = session

    async def fake_json():
        return {"jsonrpc": "2.0", "id": "1", "method": "tools/list", "params": {}}

    async def fake_handle(payload, client_id):
        return GatewayResult(
            payload={"jsonrpc": "2.0", "id": "1", "result": {"ok": True}},
            success=True,
            cache_hit=False,
            upstream_id="notion",
            tool_name=None,
            request_id=UUID("00000000-0000-0000-0000-000000000000"),
        )

    gateway.handle = fake_handle  # type: ignore[method-assign]
    request = SimpleNamespace(
        headers={"Authorization": "Bearer secret"},
        remote="127.0.0.1",
        query={"session_id": "session-1"},
        json=fake_json,
    )

    response = asyncio.run(server.message_handler(request))

    assert response.status == 503
    assert "session-1" not in server._sessions
