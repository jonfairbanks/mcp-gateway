from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from uuid import UUID

from mcp_gateway.config import AppConfig, CacheConfig, GatewayConfig, LoggingConfig, UpstreamConfig
from mcp_gateway.errors import ConflictError, NotFoundError
from mcp_gateway.gateway import Gateway, GatewayResult, UpstreamExecution
from mcp_gateway.logging import Logger
from mcp_gateway.postgres import PostgresStore
from mcp_gateway.protocol import CURRENT_PROTOCOL_VERSION
from mcp_gateway.request_context import AuthenticatedPrincipal, RequestContext
from mcp_gateway.router import build_routes, select_upstream
from mcp_gateway.server_http import HttpServer
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
        transport="streamable_http",
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
            auth_mode="single_shared",
            api_key="secret",
            bootstrap_admin_api_key="",
            allow_unauthenticated=False,
            public_tools_catalog=False,
            trusted_proxies=["127.0.0.1", "::1"],
            request_max_bytes=2 * 1024 * 1024,
            rate_limit_per_minute=120,
            circuit_breaker_fail_threshold=10,
            circuit_breaker_open_seconds=30,
        ),
        logging=LoggingConfig(stdout_json=False, extra_redact_fields=[]),
        cache=CacheConfig(enabled=True, max_entries=100, default_ttl_minutes=60, client_scoped_tools=[]),
        upstreams=upstreams,
    )


def _request(
    *,
    headers: dict | None = None,
    remote: str = "127.0.0.1",
    body=None,
    query: dict | None = None,
    match_info: dict | None = None,
):
    async def json_loader():
        if isinstance(body, Exception):
            raise body
        return body

    return SimpleNamespace(
        headers=headers or {},
        remote=remote,
        json=json_loader,
        query=query or {},
        match_info=match_info or {},
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

    principal, unauthorized_ok = asyncio.run(server._authenticate(request_ok))
    rejected_principal, unauthorized_bad = asyncio.run(server._authenticate(request_bad))

    assert principal is not None
    assert principal.subject == "gateway"
    assert principal.auth_scheme == "shared_bearer"
    assert principal.role == "admin"
    assert unauthorized_ok is None
    assert rejected_principal is None
    assert unauthorized_bad is not None


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
    assert "&lt;token&gt;" in response.text
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


def test_standard_user_can_reach_authenticated_transport() -> None:
    config = _config_with_upstreams([_upstream()])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    async def fake_authenticate_token(token):
        return AuthenticatedPrincipal(subject="alice", auth_scheme="postgres_api_key")

    gateway.authenticate_token = fake_authenticate_token  # type: ignore[method-assign]
    gateway.auth_required = lambda: True  # type: ignore[method-assign]
    request = SimpleNamespace(headers={"Authorization": "Bearer user-key"}, remote="127.0.0.1")

    request_context, response = asyncio.run(server._preflight_request(request))

    assert response is None
    assert request_context is not None
    assert request_context.principal is not None
    assert request_context.principal.role is None


def test_management_endpoints_require_auth_even_when_gateway_allows_unauthenticated() -> None:
    config = _config_with_upstreams([_upstream()])
    config.gateway.auth_mode = "postgres_api_keys"
    config.gateway.allow_unauthenticated = True
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    request = _request(headers={})

    response = asyncio.run(server.me_handler(request))

    assert response.status == 401


def test_me_handler_returns_principal_profile_and_user_metadata() -> None:
    config = _config_with_upstreams([_upstream()])
    config.gateway.auth_mode = "postgres_api_keys"
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    async def fake_authenticate_token(token):
        assert token == "admin-key"
        return AuthenticatedPrincipal(
            subject="jonfairbanks",
            auth_scheme="postgres_api_key",
            role="admin",
            user_id="user-1",
            api_key_id="key-1",
        )

    async def fake_get_user_by_id(user_id):
        assert user_id == "user-1"
        return {
            "id": "user-1",
            "subject": "jonfairbanks",
            "display_name": "Jon Fairbanks",
            "role": "admin",
            "is_active": True,
            "created_at": "2026-03-01T00:00:00+00:00",
            "updated_at": "2026-03-01T00:00:00+00:00",
        }

    gateway.authenticate_token = fake_authenticate_token  # type: ignore[method-assign]
    gateway.get_user_by_id = fake_get_user_by_id  # type: ignore[method-assign]
    request = _request(headers={"Authorization": "Bearer admin-key"})

    response = asyncio.run(server.me_handler(request))

    assert response.status == 200
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["subject"] == "jonfairbanks"
    assert payload["role"] == "admin"
    assert payload["user"]["display_name"] == "Jon Fairbanks"


def test_my_api_keys_handlers_list_and_create_keys() -> None:
    config = _config_with_upstreams([_upstream()])
    config.gateway.auth_mode = "postgres_api_keys"
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    async def fake_authenticate_token(token):
        return AuthenticatedPrincipal(
            subject="alice",
            auth_scheme="postgres_api_key",
            user_id="user-1",
            api_key_id="key-1",
        )

    async def fake_list_api_keys(*, user_id: str):
        assert user_id == "user-1"
        return [{"api_key_id": "key-1", "key_name": "default", "key_prefix": "abcd1234"}]

    async def fake_issue_api_key_for_user(*, user_id: str, key_name: str, expires_at):
        assert user_id == "user-1"
        assert key_name == "laptop"
        assert expires_at is not None
        return {"api_key_id": "key-2", "key_name": "laptop", "api_key": "mgw_generated"}

    gateway.authenticate_token = fake_authenticate_token  # type: ignore[method-assign]
    gateway.list_api_keys = fake_list_api_keys  # type: ignore[method-assign]
    gateway.issue_api_key_for_user = fake_issue_api_key_for_user  # type: ignore[method-assign]

    list_response = asyncio.run(server.my_api_keys_list_handler(_request(headers={"Authorization": "Bearer user-key"})))
    create_response = asyncio.run(
        server.my_api_keys_create_handler(
            _request(
                headers={"Authorization": "Bearer user-key"},
                body={"label": "laptop", "expires_at": "2026-03-20T12:00:00Z"},
            )
        )
    )

    assert list_response.status == 200
    assert json.loads(list_response.body.decode("utf-8"))["items"][0]["key_name"] == "default"
    assert create_response.status == 201
    assert json.loads(create_response.body.decode("utf-8"))["api_key"] == "mgw_generated"


def test_my_api_keys_create_handler_hides_validation_error_details() -> None:
    config = _config_with_upstreams([_upstream()])
    config.gateway.auth_mode = "postgres_api_keys"
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    logger = RecordingLogger()
    server = HttpServer(config, gateway, logger, GatewayTelemetry())

    async def fake_authenticate_token(token):
        return AuthenticatedPrincipal(
            subject="alice",
            auth_scheme="postgres_api_key",
            user_id="user-1",
            api_key_id="key-1",
        )

    async def fake_issue_api_key_for_user(*, user_id: str, key_name: str, expires_at):
        raise ValueError("user_id is required")

    gateway.authenticate_token = fake_authenticate_token  # type: ignore[method-assign]
    gateway.issue_api_key_for_user = fake_issue_api_key_for_user  # type: ignore[method-assign]

    response = asyncio.run(
        server.my_api_keys_create_handler(
            _request(
                headers={"Authorization": "Bearer user-key"},
                body={"label": "laptop"},
            )
        )
    )

    assert response.status == 400
    payload = json.loads(response.body.decode("utf-8"))
    assert payload == {"error": "InvalidRequest", "message": "Unable to issue API key for this request."}
    assert logger.warnings[0][0] == "http_invalid_request"
    assert logger.warnings[0][1]["endpoint"] == "/v1/me/api-keys"
    assert logger.warnings[0][1]["operation"] == "issue_api_key_for_user"
    assert logger.warnings[0][1]["error"] == "user_id is required"


def test_my_api_keys_handler_rejects_bootstrap_admin_without_managed_user() -> None:
    config = _config_with_upstreams([_upstream()])
    config.gateway.auth_mode = "postgres_api_keys"
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    async def fake_authenticate_token(token):
        return AuthenticatedPrincipal(subject="bootstrap-admin", auth_scheme="bootstrap_admin", role="admin")

    gateway.authenticate_token = fake_authenticate_token  # type: ignore[method-assign]

    response = asyncio.run(server.my_api_keys_list_handler(_request(headers={"Authorization": "Bearer bootstrap"})))

    assert response.status == 400


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


def test_rate_limit_scope_key_prefers_api_key_id_then_user_id_then_subject_then_client_id() -> None:
    config = _config_with_upstreams([_upstream()])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    api_key_context = RequestContext(
        client_id="client-a",
        principal=AuthenticatedPrincipal(subject="alice", auth_scheme="postgres_api_key", api_key_id="key-1", user_id="user-1"),
    )
    user_context = RequestContext(
        client_id="client-b",
        principal=AuthenticatedPrincipal(subject="alice", auth_scheme="postgres_api_key", user_id="user-1"),
    )
    subject_context = RequestContext(
        client_id="client-c",
        principal=AuthenticatedPrincipal(subject="gateway", auth_scheme="shared_bearer"),
    )
    anonymous_context = RequestContext(client_id="client-d")

    assert server._rate_limit_scope_key(api_key_context) == "api_key:key-1"
    assert server._rate_limit_scope_key(user_context) == "user:user-1"
    assert server._rate_limit_scope_key(subject_context) == "subject:shared_bearer:gateway"
    assert server._rate_limit_scope_key(anonymous_context) == "client:client-d"


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
            request_context=RequestContext(client_id="client-1"),
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
            RequestContext(client_id=None),
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


def test_fanout_initialize_executes_upstreams_concurrently() -> None:
    config = _config_with_upstreams([_upstream("one"), _upstream("two"), _upstream("three")])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    active_calls = 0
    max_active_calls = 0
    lock = asyncio.Lock()

    async def fake_execute(upstream, method, payload, *, notification=False):
        nonlocal active_calls, max_active_calls
        assert method == "initialize"
        async with lock:
            active_calls += 1
            max_active_calls = max(max_active_calls, active_calls)
        await asyncio.sleep(0.01)
        async with lock:
            active_calls -= 1
        return UpstreamExecution(
            success=True,
            payload={"jsonrpc": "2.0", "id": payload.get("id"), "result": {"capabilities": {}, "protocolVersion": CURRENT_PROTOCOL_VERSION}},
            log_payload={"ok": True},
            error=None,
        )

    gateway._execute_upstream_operation = fake_execute  # type: ignore[method-assign]

    response_payload, success = asyncio.run(
        gateway._fanout_initialize(
            {
                "jsonrpc": "2.0",
                "id": "init-1",
                "method": "initialize",
                "params": {"protocolVersion": CURRENT_PROTOCOL_VERSION},
            }
        )
    )

    assert success is True
    assert response_payload["result"]["protocolVersion"] == CURRENT_PROTOCOL_VERSION
    assert max_active_calls > 1


def test_fanout_initialize_negotiates_latest_supported_version_for_unknown_client_version() -> None:
    config = _config_with_upstreams([_upstream("one")])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    seen_versions: list[str] = []

    async def fake_execute(upstream, method, payload, *, notification=False):
        assert method == "initialize"
        seen_versions.append(payload["params"]["protocolVersion"])
        return UpstreamExecution(
            success=True,
            payload={"jsonrpc": "2.0", "id": payload.get("id"), "result": {"capabilities": {}}},
            log_payload={"ok": True},
            error=None,
        )

    gateway._execute_upstream_operation = fake_execute  # type: ignore[method-assign]

    response_payload, success = asyncio.run(
        gateway._fanout_initialize(
            {
                "jsonrpc": "2.0",
                "id": "init-1",
                "method": "initialize",
                "params": {"protocolVersion": "2099-01-01"},
            }
        )
    )

    assert success is True
    assert response_payload["result"]["protocolVersion"] == CURRENT_PROTOCOL_VERSION
    assert seen_versions == [CURRENT_PROTOCOL_VERSION]


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


def test_gateway_is_not_ready_when_initialize_succeeds_but_tools_list_fails() -> None:
    config = _config_with_upstreams([_upstream("degraded")])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    gateway._warmup_status = {
        "degraded": {
            "initialize_success": True,
            "initialize_error": None,
            "tools_list_success": False,
            "tools_list_error": {"message": "downstream unavailable"},
            "tool_count": 0,
            "tools": [],
        }
    }

    assert gateway.is_ready() is False


def test_preflight_request_rejects_unauthorized_requests() -> None:
    config = _config_with_upstreams([_upstream()])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    request = SimpleNamespace(headers={}, remote="127.0.0.1")

    request_context, response = asyncio.run(server._preflight_request(request))

    assert request_context is None
    assert response is not None
    assert response.status == 401


def test_execution_endpoints_still_require_auth_when_tools_catalog_is_public() -> None:
    config = _config_with_upstreams([_upstream()])
    config.gateway.public_tools_catalog = True
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    request = SimpleNamespace(headers={}, remote="127.0.0.1")

    request_context, response = asyncio.run(server._preflight_request(request))

    assert request_context is None
    assert response is not None
    assert response.status == 401


def test_preflight_request_rejects_rate_limited_requests() -> None:
    config = _config_with_upstreams([_upstream()])
    config.gateway.rate_limit_per_minute = 1
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    request = SimpleNamespace(headers={"Authorization": "Bearer secret"}, remote="127.0.0.1")

    first_context, first_response = asyncio.run(server._preflight_request(request))
    second_context, response = asyncio.run(server._preflight_request(request))

    assert first_response is None
    assert first_context is not None
    assert first_context.client_id == "127.0.0.1"
    assert first_context.principal is not None
    assert first_context.principal.subject == "gateway"
    assert first_context.principal.auth_scheme == "shared_bearer"
    assert second_context is None
    assert response is not None
    assert response.status == 429


def test_preflight_request_returns_request_context_for_public_tools() -> None:
    config = _config_with_upstreams([_upstream()])
    config.gateway.public_tools_catalog = True
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    request = SimpleNamespace(headers={}, remote="127.0.0.1")

    request_context, response = asyncio.run(server._preflight_request(request, endpoint="/tools", require_auth=False))

    assert response is None
    assert request_context == RequestContext(client_id="127.0.0.1", principal=None)


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


def test_mcp_get_handler_returns_method_not_allowed() -> None:
    config = _config_with_upstreams([_upstream()])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    request = SimpleNamespace()

    response = asyncio.run(server.mcp_get_handler(request))

    assert response.status == 405
    assert response.headers["Allow"] == "POST"


def test_standard_user_cannot_call_tools_without_grants() -> None:
    config = _config_with_upstreams([_upstream()])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    request_context = RequestContext(
        client_id="client-1",
        principal=AuthenticatedPrincipal(
            subject="alice",
            auth_scheme="postgres_api_key",
            user_id="user-1",
            api_key_id="key-1",
        ),
    )

    result = asyncio.run(
        gateway.handle(
            {
                "jsonrpc": "2.0",
                "id": "1",
                "method": "tools/call",
                "params": {"name": "notion.get", "arguments": {}},
            },
            request_context,
        )
    )

    assert result.success is False
    assert result.payload["error"]["data"]["category"] == "policy_denied"


def test_admin_users_create_requires_admin_role() -> None:
    config = _config_with_upstreams([_upstream()])
    config.gateway.auth_mode = "postgres_api_keys"
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    async def fake_authenticate_token(token):
        return AuthenticatedPrincipal(
            subject="alice",
            auth_scheme="postgres_api_key",
            user_id="user-1",
            api_key_id="key-1",
        )

    gateway.authenticate_token = fake_authenticate_token  # type: ignore[method-assign]

    response = asyncio.run(
        server.admin_users_create_handler(
            _request(
                headers={"Authorization": "Bearer user-key"},
                body={"subject": "bob", "display_name": "Bob"},
            )
        )
    )

    assert response.status == 403


def test_admin_users_create_returns_conflict_for_existing_subject() -> None:
    config = _config_with_upstreams([_upstream()])
    config.gateway.auth_mode = "postgres_api_keys"
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    async def fake_authenticate_token(token):
        return AuthenticatedPrincipal(
            subject="jonfairbanks",
            auth_scheme="postgres_api_key",
            role="admin",
            user_id="user-1",
            api_key_id="key-1",
        )

    async def fake_create_user(*, subject: str, display_name: str | None, role: str | None):
        assert subject == "jonfairbanks"
        return None

    gateway.authenticate_token = fake_authenticate_token  # type: ignore[method-assign]
    gateway.create_user = fake_create_user  # type: ignore[method-assign]

    response = asyncio.run(
        server.admin_users_create_handler(
            _request(
                headers={"Authorization": "Bearer admin-key"},
                body={"subject": "jonfairbanks", "display_name": "Jon", "role": "admin"},
            )
        )
    )

    assert response.status == 409


def test_admin_users_create_normalizes_admin_role_before_call() -> None:
    config = _config_with_upstreams([_upstream()])
    config.gateway.auth_mode = "postgres_api_keys"
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    async def fake_authenticate_token(token):
        return AuthenticatedPrincipal(
            subject="jonfairbanks",
            auth_scheme="postgres_api_key",
            role="admin",
            user_id="user-1",
            api_key_id="key-1",
        )

    async def fake_create_user(*, subject: str, display_name: str | None, role: str | None):
        assert subject == "alice"
        assert display_name == "Alice"
        assert role == "admin"
        return {
            "id": "user-2",
            "subject": "alice",
            "display_name": "Alice",
            "role": role,
            "is_active": True,
            "created_at": "2026-03-01T00:00:00+00:00",
            "updated_at": "2026-03-01T00:00:00+00:00",
        }

    gateway.authenticate_token = fake_authenticate_token  # type: ignore[method-assign]
    gateway.create_user = fake_create_user  # type: ignore[method-assign]

    response = asyncio.run(
        server.admin_users_create_handler(
            _request(
                headers={"Authorization": "Bearer admin-key"},
                body={"subject": "alice", "display_name": "Alice", "role": " ADMIN "},
            )
        )
    )

    assert response.status == 201
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["user"]["role"] == "admin"


def test_admin_users_create_defaults_to_standard_user_without_role() -> None:
    config = _config_with_upstreams([_upstream()])
    config.gateway.auth_mode = "postgres_api_keys"
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    async def fake_authenticate_token(token):
        return AuthenticatedPrincipal(
            subject="jonfairbanks",
            auth_scheme="postgres_api_key",
            role="admin",
            user_id="user-1",
            api_key_id="key-1",
        )

    async def fake_create_user(*, subject: str, display_name: str | None, role: str | None):
        assert subject == "alice"
        assert display_name == "Alice"
        assert role is None
        return {
            "id": "user-2",
            "subject": "alice",
            "display_name": "Alice",
            "role": None,
            "is_active": True,
            "created_at": "2026-03-01T00:00:00+00:00",
            "updated_at": "2026-03-01T00:00:00+00:00",
        }

    gateway.authenticate_token = fake_authenticate_token  # type: ignore[method-assign]
    gateway.create_user = fake_create_user  # type: ignore[method-assign]

    response = asyncio.run(
        server.admin_users_create_handler(
            _request(
                headers={"Authorization": "Bearer admin-key"},
                body={"subject": "alice", "display_name": "Alice"},
            )
        )
    )

    assert response.status == 201
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["user"]["role"] is None


def test_admin_users_update_clears_role_when_null_is_provided() -> None:
    config = _config_with_upstreams([_upstream()])
    config.gateway.auth_mode = "postgres_api_keys"
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    async def fake_authenticate_token(token):
        return AuthenticatedPrincipal(
            subject="jonfairbanks",
            auth_scheme="postgres_api_key",
            role="admin",
            user_id="user-1",
            api_key_id="key-1",
        )

    async def fake_update_user(
        user_id: str,
        *,
        display_name: str | None = None,
        role: str | None = None,
        role_provided: bool = False,
        is_active: bool | None = None,
    ):
        assert user_id == "user-2"
        assert display_name is None
        assert role is None
        assert role_provided is True
        assert is_active is None
        return {
            "id": "user-2",
            "subject": "alice",
            "display_name": "Alice",
            "role": None,
            "is_active": True,
            "created_at": "2026-03-01T00:00:00+00:00",
            "updated_at": "2026-03-02T00:00:00+00:00",
        }

    gateway.authenticate_token = fake_authenticate_token  # type: ignore[method-assign]
    gateway.update_user = fake_update_user  # type: ignore[method-assign]

    response = asyncio.run(
        server.admin_users_update_handler(
            _request(
                headers={"Authorization": "Bearer admin-key"},
                body={"role": None},
                match_info={"user_id": "user-2"},
            )
        )
    )

    assert response.status == 200
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["role"] is None


def test_admin_usage_handler_returns_grouped_rows() -> None:
    config = _config_with_upstreams([_upstream()])
    config.gateway.auth_mode = "postgres_api_keys"
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    async def fake_authenticate_token(token):
        return AuthenticatedPrincipal(
            subject="jonfairbanks",
            auth_scheme="postgres_api_key",
            role="admin",
            user_id="user-1",
            api_key_id="key-1",
        )

    async def fake_usage_summary(*, group_by: str, from_timestamp, to_timestamp):
        assert group_by == "api_key"
        assert from_timestamp is not None
        assert to_timestamp is not None
        return [{"api_key_id": "key-1", "request_count": 3}]

    gateway.authenticate_token = fake_authenticate_token  # type: ignore[method-assign]
    gateway.usage_summary = fake_usage_summary  # type: ignore[method-assign]

    response = asyncio.run(
        server.admin_usage_handler(
            _request(
                headers={"Authorization": "Bearer admin-key"},
                query={"group_by": "api_key", "from": "2026-03-01T00:00:00Z", "to": "2026-03-31T23:59:59Z"},
            )
        )
    )

    assert response.status == 200
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["group_by"] == "api_key"
    assert payload["items"] == [{"api_key_id": "key-1", "request_count": 3}]


def test_admin_usage_handler_hides_invalid_timestamp_details() -> None:
    config = _config_with_upstreams([_upstream()])
    config.gateway.auth_mode = "postgres_api_keys"
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    logger = RecordingLogger()
    server = HttpServer(config, gateway, logger, GatewayTelemetry())

    async def fake_authenticate_token(token):
        return AuthenticatedPrincipal(
            subject="jonfairbanks",
            auth_scheme="postgres_api_key",
            role="admin",
            user_id="user-1",
            api_key_id="key-1",
        )

    gateway.authenticate_token = fake_authenticate_token  # type: ignore[method-assign]

    response = asyncio.run(
        server.admin_usage_handler(
            _request(
                headers={"Authorization": "Bearer admin-key"},
                query={"from": "not-a-timestamp"},
            )
        )
    )

    assert response.status == 400
    payload = json.loads(response.body.decode("utf-8"))
    assert payload == {"error": "InvalidRequest", "message": "from must be a valid ISO-8601 timestamp"}
    assert logger.warnings[0][0] == "http_invalid_request"
    assert logger.warnings[0][1]["endpoint"] == "/v1/admin/usage"
    assert logger.warnings[0][1]["field"] == "from"


def test_admin_groups_create_handler_allows_admin_api_key() -> None:
    config = _config_with_upstreams([_upstream("jira")])
    config.gateway.auth_mode = "postgres_api_keys"
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    async def fake_authenticate_token(token):
        return AuthenticatedPrincipal(
            subject="alice",
            auth_scheme="postgres_api_key",
            role="admin",
            user_id="user-1",
        )

    async def fake_create_group(*, name: str, description: str | None):
        assert name == "sales"
        assert description == "Sales team"
        return {"id": "group-1", "name": "sales", "description": "Sales team"}

    gateway.authenticate_token = fake_authenticate_token  # type: ignore[method-assign]
    gateway.create_group = fake_create_group  # type: ignore[method-assign]

    response = asyncio.run(
        server.admin_groups_create_handler(
            _request(
                headers={"Authorization": "Bearer admin-key"},
                body={"name": "sales", "description": "Sales team"},
            )
        )
    )

    assert response.status == 201
    payload = json.loads(response.body.decode("utf-8"))
    assert payload == {"id": "group-1", "name": "sales", "description": "Sales team"}


def test_admin_groups_list_handler_requires_group_read_permission() -> None:
    config = _config_with_upstreams([_upstream("jira")])
    config.gateway.auth_mode = "postgres_api_keys"
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    async def fake_authenticate_token(token):
        return AuthenticatedPrincipal(
            subject="bob",
            auth_scheme="postgres_api_key",
            group_names=("sales",),
            user_id="user-2",
        )

    gateway.authenticate_token = fake_authenticate_token  # type: ignore[method-assign]

    response = asyncio.run(server.admin_groups_list_handler(_request(headers={"Authorization": "Bearer user-key"})))

    assert response.status == 403
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["error"]["data"]["category"] == "policy_denied"
    assert payload["error"]["data"]["permission"] == "admin.groups.read"


def test_mcp_post_handler_returns_accepted_for_notification_batches() -> None:
    config = _config_with_upstreams([_upstream()])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    async def fake_json():
        return [
            {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
            {"jsonrpc": "2.0", "method": "ping"},
        ]

    async def fake_handle(payload, request_context):
        assert request_context.client_id == "127.0.0.1"
        assert request_context.principal is not None
        assert request_context.principal.subject == "gateway"
        assert request_context.principal.auth_scheme == "shared_bearer"
        return GatewayResult(
            payload={"jsonrpc": "2.0", "id": payload.get("id"), "result": {"ok": True}},
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
        json=fake_json,
    )

    response = asyncio.run(server.mcp_post_handler(request))

    assert response.status == 202


def test_mcp_post_handler_returns_invalid_request_for_empty_batch() -> None:
    config = _config_with_upstreams([_upstream()])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    async def fake_json():
        return []

    request = SimpleNamespace(
        headers={"Authorization": "Bearer secret"},
        remote="127.0.0.1",
        json=fake_json,
    )

    response = asyncio.run(server.mcp_post_handler(request))

    assert response.status == 400
    payload = json.loads(response.body.decode("utf-8"))
    assert payload["error"]["code"] == -32600


def test_mcp_post_handler_preserves_batch_order_and_omits_notifications() -> None:
    config = _config_with_upstreams([_upstream()])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())
    handled_ids: list[str | None] = []

    async def fake_json():
        return [
            {"jsonrpc": "2.0", "id": "1", "method": "tools/list", "params": {}},
            {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
            {"jsonrpc": "2.0", "id": "2", "method": "tools/list", "params": {}},
        ]

    async def fake_handle(payload, request_context):
        handled_ids.append(payload.get("id"))
        return GatewayResult(
            payload={"jsonrpc": "2.0", "id": payload.get("id"), "result": {"id": payload.get("id")}},
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
        json=fake_json,
    )

    response = asyncio.run(server.mcp_post_handler(request))

    assert response.status == 200
    payload = json.loads(response.body.decode("utf-8"))
    assert [item["id"] for item in payload] == ["1", "2"]
    assert handled_ids == ["1", None, "2"]


def test_mcp_post_handler_returns_negotiated_protocol_header_on_initialize() -> None:
    config = _config_with_upstreams([_upstream()])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    async def fake_json():
        return {
            "jsonrpc": "2.0",
            "id": "init-1",
            "method": "initialize",
            "params": {
                "protocolVersion": CURRENT_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "test-client", "version": "0.1.0"},
            },
        }

    async def fake_handle(payload, request_context):
        return GatewayResult(
            payload={
                "jsonrpc": "2.0",
                "id": payload.get("id"),
                "result": {
                    "protocolVersion": CURRENT_PROTOCOL_VERSION,
                    "capabilities": {},
                    "serverInfo": {"name": "mcp-gateway", "version": "0.1.0"},
                },
            },
            success=True,
            cache_hit=False,
            upstream_id=None,
            tool_name=None,
            request_id=UUID("00000000-0000-0000-0000-000000000000"),
        )

    gateway.handle = fake_handle  # type: ignore[method-assign]
    request = SimpleNamespace(
        headers={"Authorization": "Bearer secret"},
        remote="127.0.0.1",
        json=fake_json,
    )

    response = asyncio.run(server.mcp_post_handler(request))

    assert response.status == 200
    assert response.headers["MCP-Protocol-Version"] == CURRENT_PROTOCOL_VERSION


def test_mcp_post_handler_rejects_unsupported_protocol_header() -> None:
    config = _config_with_upstreams([_upstream()])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, Logger(stdout_json=False), GatewayTelemetry())

    async def fake_json():
        return {"jsonrpc": "2.0", "id": "list-1", "method": "tools/list", "params": {}}

    request = SimpleNamespace(
        headers={"Authorization": "Bearer secret", "MCP-Protocol-Version": "2099-01-01"},
        remote="127.0.0.1",
        json=fake_json,
    )

    response = asyncio.run(server.mcp_post_handler(request))

    assert response.status == 400
    assert response.text == "Unsupported MCP-Protocol-Version header."


def test_error_middleware_maps_gateway_http_errors_to_rest_responses() -> None:
    config = _config_with_upstreams([_upstream()])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    logger = RecordingLogger()
    server = HttpServer(config, gateway, logger, GatewayTelemetry())
    request = SimpleNamespace(path="/v1/admin/groups")

    async def failing_handler(_request):
        raise NotFoundError("Group not found.")

    response = asyncio.run(server._error_middleware(request, failing_handler))

    assert response.status == 404
    payload = json.loads(response.body.decode("utf-8"))
    assert payload == {"error": "NotFound", "message": "Group not found."}
    assert logger.warnings == [
        (
            "http_service_error",
            {
                "endpoint": "/v1/admin/groups",
                "status": 404,
                "error": "NotFound",
                "message": "Group not found.",
            },
        )
    ]


def test_error_middleware_maps_conflicts_to_rest_responses() -> None:
    config = _config_with_upstreams([_upstream()])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False), GatewayTelemetry())
    server = HttpServer(config, gateway, RecordingLogger(), GatewayTelemetry())
    request = SimpleNamespace(path="/v1/admin/groups")

    async def failing_handler(_request):
        raise ConflictError("A group with that name already exists.")

    response = asyncio.run(server._error_middleware(request, failing_handler))

    assert response.status == 409
    payload = json.loads(response.body.decode("utf-8"))
    assert payload == {"error": "Conflict", "message": "A group with that name already exists."}
