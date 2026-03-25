from __future__ import annotations

from types import SimpleNamespace

from mcp_gateway.config import AppConfig, CacheConfig, GatewayConfig, LoggingConfig, UpstreamConfig


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
        cache=CacheConfig(enabled=True, max_entries=100, default_ttl_minutes=60),
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
