from __future__ import annotations

import asyncio
import json
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
            public_tools_catalog=False,
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


def test_cache_key_ignores_progress_token_metadata() -> None:
    config = _config_with_upstreams([_upstream()])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    upstream = config.upstreams[0]
    params_a = {
        "name": "query-docs",
        "arguments": {"libraryId": "/fastapi/fastapi", "query": "hello"},
        "_meta": {"progressToken": 1},
    }
    params_b = {
        "_meta": {"progressToken": 2},
        "arguments": {"query": "hello", "libraryId": "/fastapi/fastapi"},
        "name": "query-docs",
    }
    key_a = gateway._cache_key(upstream, "tools/call", "query-docs", params_a, None)
    key_b = gateway._cache_key(upstream, "tools/call", "query-docs", params_b, None)
    assert key_a == key_b


def test_cache_key_treats_progress_token_only_meta_as_absent() -> None:
    config = _config_with_upstreams([_upstream()])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    upstream = config.upstreams[0]
    params_a = {
        "name": "query-docs",
        "arguments": {"libraryId": "/fastapi/fastapi", "query": "hello"},
        "_meta": {"progressToken": 7},
    }
    params_b = {
        "name": "query-docs",
        "arguments": {"libraryId": "/fastapi/fastapi", "query": "hello"},
    }
    key_a = gateway._cache_key(upstream, "tools/call", "query-docs", params_a, None)
    key_b = gateway._cache_key(upstream, "tools/call", "query-docs", params_b, None)
    assert key_a == key_b


def test_http_auth_header_parsing() -> None:
    config = _config_with_upstreams([_upstream()])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    request_ok = SimpleNamespace(headers={"Authorization": "Bearer secret"})
    request_bad = SimpleNamespace(headers={"Authorization": "Bearer wrong"})

    assert server._authorize(request_ok) is None
    assert server._authorize(request_bad) is not None


def test_tools_handler_returns_browser_friendly_unauthorized_html() -> None:
    config = _config_with_upstreams([_upstream()])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    request = SimpleNamespace(
        headers={"Accept": "text/html"},
        remote="127.0.0.1",
    )

    response = asyncio.run(server.tools_handler(request))

    assert response.status == 401
    assert response.content_type == "text/html"
    assert "&lt;gateway.api_key&gt;" in response.text
    assert response.headers["WWW-Authenticate"] == 'Bearer realm="mcp-gateway"'


def test_tools_handler_returns_clear_json_when_not_a_browser_request() -> None:
    config = _config_with_upstreams([_upstream()])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    request = SimpleNamespace(
        headers={"Accept": "application/json"},
        remote="127.0.0.1",
    )

    response = asyncio.run(server.tools_handler(request))

    assert response.status == 401
    assert response.content_type == "application/json"
    assert b"Browsers do not send this header automatically" in response.body
    assert response.headers["WWW-Authenticate"] == 'Bearer realm="mcp-gateway"'


def test_tools_handler_can_be_public_without_auth() -> None:
    config = _config_with_upstreams([_upstream()])
    config.gateway.public_tools_catalog = True
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    request = SimpleNamespace(
        headers={"Accept": "application/json"},
        remote="127.0.0.1",
    )

    response = asyncio.run(server.tools_handler(request))

    assert response.status == 200
    payload = json.loads(response.body.decode("utf-8"))
    assert "upstreams" in payload
    assert "tools" not in payload["upstreams"][0]


def test_public_tools_handler_still_applies_rate_limits() -> None:
    config = _config_with_upstreams([_upstream()])
    config.gateway.public_tools_catalog = True
    config.gateway.rate_limit_per_minute = 1
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    request = SimpleNamespace(
        headers={"Accept": "application/json"},
        remote="127.0.0.1",
    )

    first = asyncio.run(server.tools_handler(request))
    second = asyncio.run(server.tools_handler(request))

    assert first.status == 200
    assert second.status == 429


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


def test_execute_upstream_operation_returns_successful_request_payload() -> None:
    config = _config_with_upstreams([_upstream()])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    upstream = config.upstreams[0]

    async def fake_call(upstream_cfg, payload):
        assert upstream_cfg.id == upstream.id
        assert payload["method"] == "tools/call"
        return UpstreamResponse(
            payload={"jsonrpc": "2.0", "id": payload.get("id"), "result": {"ok": True}},
            success=True,
        )

    gateway._call_upstream = fake_call  # type: ignore[method-assign]

    execution = asyncio.run(
        gateway._execute_upstream_operation(
            upstream,
            "tools/call",
            {"jsonrpc": "2.0", "id": "1", "method": "tools/call", "params": {}},
        )
    )

    assert execution.success is True
    assert execution.payload["result"] == {"ok": True}
    assert execution.log_payload == execution.payload
    assert execution.error is None


def test_execute_upstream_operation_maps_notification_failures() -> None:
    config = _config_with_upstreams([_upstream()])
    logger = RecordingLogger()
    gateway = Gateway(config, PostgresStore(""), logger, GatewayTelemetry())
    upstream = config.upstreams[0]

    async def fake_notify(upstream_cfg, payload) -> None:
        assert upstream_cfg.id == upstream.id
        assert payload["method"] == "notifications/test"
        raise RuntimeError("broken upstream")

    gateway._notify_upstream = fake_notify  # type: ignore[method-assign]

    execution = asyncio.run(
        gateway._execute_upstream_operation(
            upstream,
            "notifications/test",
            {"jsonrpc": "2.0", "method": "notifications/test", "params": {}},
            notification=True,
        )
    )

    assert execution.success is False
    assert execution.payload == {"accepted": False}
    assert execution.log_payload["accepted"] is False
    assert execution.log_payload["error"] == {"code": -32004, "message": "Upstream unavailable"}
    assert execution.error == {"code": -32004, "message": "Upstream unavailable"}
    assert logger.errors == [
        (
            "upstream_request_failed",
            {
                "upstream_id": upstream.id,
                "method": "notifications/test",
                "notification": True,
                "code": -32004,
                "client_message": "Upstream unavailable",
                "error_type": "RuntimeError",
                "error": "broken upstream",
            },
        )
    ]


def test_execute_upstream_operation_returns_generic_error_payload_for_unexpected_exceptions() -> None:
    config = _config_with_upstreams([_upstream()])
    logger = RecordingLogger()
    gateway = Gateway(config, PostgresStore(""), logger, GatewayTelemetry())
    upstream = config.upstreams[0]

    async def fake_call(upstream_cfg, payload):
        assert upstream_cfg.id == upstream.id
        assert payload["method"] == "tools/call"
        raise Exception("secret token leaked")

    gateway._call_upstream = fake_call  # type: ignore[method-assign]

    execution = asyncio.run(
        gateway._execute_upstream_operation(
            upstream,
            "tools/call",
            {"jsonrpc": "2.0", "id": "1", "method": "tools/call", "params": {}},
        )
    )

    assert execution.success is False
    assert execution.payload["error"] == {"code": -32003, "message": "Upstream request failed"}
    assert execution.log_payload == execution.payload
    assert execution.error == {"code": -32003, "message": "Upstream request failed"}
    assert "secret token leaked" not in json.dumps(execution.payload)
    assert logger.errors == [
        (
            "upstream_request_failed",
            {
                "upstream_id": upstream.id,
                "method": "tools/call",
                "notification": False,
                "code": -32003,
                "client_message": "Upstream request failed",
                "error_type": "Exception",
                "error": "secret token leaked",
            },
        )
    ]


def test_warmup_and_tools_list_build_identical_tool_registry_state() -> None:
    config = _config_with_upstreams([_upstream("one"), _upstream("two")])
    warmup_gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    list_gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    tool_payloads = {
        "one": [{"name": "alpha.tool"}, {"name": "alpha.tool"}],
        "two": [{"name": "beta.tool"}],
    }

    async def fake_call(upstream, payload):
        if payload["method"] == "initialize":
            return UpstreamResponse(payload={"jsonrpc": "2.0", "id": payload.get("id"), "result": {}}, success=True)
        if payload["method"] == "tools/list":
            return UpstreamResponse(
                payload={
                    "jsonrpc": "2.0",
                    "id": payload.get("id"),
                    "result": {"tools": tool_payloads[upstream.id]},
                },
                success=True,
            )
        raise AssertionError(payload["method"])

    async def fake_notify(upstream, payload) -> None:
        return None

    for gateway in (warmup_gateway, list_gateway):
        gateway._call_upstream = fake_call  # type: ignore[method-assign]
        gateway._notify_upstream = fake_notify  # type: ignore[method-assign]

    asyncio.run(warmup_gateway.warmup())
    asyncio.run(
        list_gateway.handle(
            {"jsonrpc": "2.0", "id": "1", "method": "tools/list", "params": {}},
            client_id=None,
        )
    )

    expected_registry = {"alpha.tool": "one", "beta.tool": "two"}
    expected_upstream_tools = {"one": ["alpha.tool"], "two": ["beta.tool"]}
    assert warmup_gateway._tool_registry == expected_registry
    assert list_gateway._tool_registry == expected_registry
    assert warmup_gateway._upstream_tools == expected_upstream_tools
    assert list_gateway._upstream_tools == expected_upstream_tools
    assert warmup_gateway._tool_alias_registry == list_gateway._tool_alias_registry


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


def test_startup_summary_reports_ready_and_failed_upstreams() -> None:
    config = _config_with_upstreams([_upstream("ready"), _upstream("failed")])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    gateway._warmup_status = {
        "ready": {
            "initialize_success": True,
            "initialize_error": None,
            "tools_list_success": True,
            "tools_list_error": None,
            "tool_count": 2,
            "tools": ["one", "two"],
        },
        "failed": {
            "initialize_success": False,
            "initialize_error": {"message": "aws login required"},
            "tools_list_success": False,
            "tools_list_error": {"message": "Invalid request parameters"},
            "tool_count": 0,
            "tools": [],
        },
    }

    summary = gateway.startup_summary()

    assert summary["gateway_ready"] is True
    assert summary["ready_upstream_count"] == 1
    assert summary["degraded_upstream_count"] == 0
    assert summary["failed_upstream_count"] == 1
    assert summary["upstreams"] == [
        {"id": "ready", "status": "ready", "tool_count": 2},
        {
            "id": "failed",
            "status": "failed",
            "tool_count": 0,
            "stage": "initialize",
            "reason": "aws login required",
        },
    ]


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


def test_preflight_request_rejects_unauthorized_requests() -> None:
    config = _config_with_upstreams([_upstream()])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    request = SimpleNamespace(headers={}, remote="127.0.0.1")

    response = asyncio.run(server._preflight_request(request))

    assert response is not None
    assert response.status == 401


def test_execution_endpoints_still_require_auth_when_tools_catalog_is_public() -> None:
    config = _config_with_upstreams([_upstream()])
    config.gateway.public_tools_catalog = True
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    request = SimpleNamespace(headers={}, remote="127.0.0.1")

    response = asyncio.run(server._preflight_request(request))

    assert response is not None
    assert response.status == 401


def test_preflight_request_rejects_rate_limited_requests() -> None:
    config = _config_with_upstreams([_upstream()])
    config.gateway.rate_limit_per_minute = 1
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    request = SimpleNamespace(headers={"Authorization": "Bearer secret"}, remote="127.0.0.1")

    assert asyncio.run(server._preflight_request(request)) is None
    response = asyncio.run(server._preflight_request(request))

    assert response is not None
    assert response.status == 429


def test_parse_json_request_rejects_invalid_json() -> None:
    config = _config_with_upstreams([_upstream()])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    async def bad_json():
        raise json.JSONDecodeError("bad", "{}", 0)

    request = SimpleNamespace(json=bad_json)

    payload, response = asyncio.run(server._parse_json_request(request))

    assert payload is None
    assert response is not None
    assert response.status == 400


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
