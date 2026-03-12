from __future__ import annotations
from types import SimpleNamespace

from mcp_gateway.config import AppConfig, CacheConfig, GatewayConfig, LoggingConfig, UpstreamConfig
from mcp_gateway.gateway import Gateway
from mcp_gateway.logging import Logger
from mcp_gateway.postgres import PostgresStore
from mcp_gateway.router import build_routes, select_upstream
from mcp_gateway.server_http import HttpServer
from mcp_gateway.telemetry import GatewayTelemetry


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
            trusted_proxies=["127.0.0.1", "::1"],
            request_max_bytes=2 * 1024 * 1024,
            rate_limit_per_minute=120,
            circuit_breaker_fail_threshold=10,
            circuit_breaker_open_seconds=30,
        ),
        logging=LoggingConfig(stdout_json=False),
        cache=CacheConfig(enabled=True, max_entries=100, default_ttl_minutes=60, client_scoped_tools=[]),
        upstreams=upstreams,
    )


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
