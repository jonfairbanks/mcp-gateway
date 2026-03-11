from __future__ import annotations

from types import SimpleNamespace

from mcp_gateway.config import AppConfig, CacheConfig, GatewayConfig, LoggingConfig, UpstreamConfig
from mcp_gateway.gateway import Gateway
from mcp_gateway.logging import Logger
from mcp_gateway.postgres import PostgresStore
from mcp_gateway.server_http import HttpServer


def _config_with_upstream(deny_tools=None):
    upstream = UpstreamConfig(
        id="notion",
        name="notion",
        transport="http_sse",
        endpoint="http://example.com/rpc",
        command=None,
        env={},
        cwd=None,
        timeout_ms=1000,
        deny_tools=deny_tools or [],
        cache_ttl_seconds=None,
        tool_routes=[],
    )
    return AppConfig(
        gateway=GatewayConfig("0.0.0.0", 8080, "secret", 60),
        logging=LoggingConfig(stdout_json=False),
        cache=CacheConfig(enabled=True, max_entries=100, default_ttl_seconds=60),
        upstreams=[upstream],
    )


def test_denylist_matches_tool_name():
    config = _config_with_upstream(deny_tools=["notion.createPage"])
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False))
    upstream = config.upstreams[0]
    assert gateway._deny(upstream, "notion.createPage") is not None
    assert gateway._deny(upstream, "notion.updatePage") is None


def test_cache_key_normalization():
    config = _config_with_upstream()
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False))
    upstream = config.upstreams[0]
    params_a = {"b": 2, "a": 1}
    params_b = {"a": 1, "b": 2}
    key_a = gateway._cache_key(upstream, "tools/call", "notion.get", params_a)
    key_b = gateway._cache_key(upstream, "tools/call", "notion.get", params_b)
    assert key_a == key_b


def test_http_auth_header_parsing():
    config = _config_with_upstream()
    gateway = Gateway(config, PostgresStore(""), Logger(stdout_json=False))
    server = HttpServer(config, gateway, Logger(stdout_json=False))

    request_ok = SimpleNamespace(headers={"Authorization": "Bearer secret"})
    request_bad = SimpleNamespace(headers={"Authorization": "Bearer wrong"})

    assert server._authorize(request_ok) is None
    assert server._authorize(request_bad) is not None
