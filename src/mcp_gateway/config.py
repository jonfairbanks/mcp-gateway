from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import yaml


@dataclass
class GatewayConfig:
    listen_host: str
    listen_port: int
    api_key: str
    trusted_proxies: List[str]
    request_max_bytes: int
    rate_limit_per_minute: int
    circuit_breaker_fail_threshold: int
    circuit_breaker_open_seconds: int


@dataclass
class LoggingConfig:
    stdout_json: bool


@dataclass
class CacheConfig:
    enabled: bool
    max_entries: int
    default_ttl_minutes: int
    client_scoped_tools: List[str]


@dataclass
class UpstreamConfig:
    id: str
    name: str
    transport: str
    endpoint: Optional[str]
    http_headers: Dict[str, str]
    bearer_token_env_var: Optional[str]
    http_serialize_requests: bool
    command: Optional[List[str]]
    env: Dict[str, str]
    cwd: Optional[str]
    timeout_ms: int
    stdio_read_limit_bytes: int
    max_in_flight: int
    deny_tools: List[str]
    cache_ttl_minutes: Optional[int]
    circuit_breaker_fail_threshold: Optional[int]
    circuit_breaker_open_seconds: Optional[int]
    tool_routes: List[str]


@dataclass
class AppConfig:
    gateway: GatewayConfig
    logging: LoggingConfig
    cache: CacheConfig
    upstreams: List[UpstreamConfig]


def _get(data: Dict[str, Any], key: str, default: Any) -> Any:
    value = data.get(key, default)
    if value is None:
        return default
    return value


def _normalize_stdio_command(item: Dict[str, Any]) -> Optional[List[str]]:
    command_raw = item.get("command")
    args_raw = item.get("args", []) or []

    if command_raw is None:
        if args_raw:
            raise ValueError("Upstream config uses 'args' but is missing 'command'")
        return None

    if isinstance(command_raw, str):
        command: List[str] = [command_raw]
    elif isinstance(command_raw, list):
        command = [str(part) for part in command_raw]
    else:
        raise ValueError("Upstream 'command' must be a string or list of strings")

    if not isinstance(args_raw, list):
        raise ValueError("Upstream 'args' must be a list of strings")
    command.extend(str(arg) for arg in args_raw)
    return command


def load_config(path: str) -> AppConfig:
    with open(path, "r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}

    gateway_raw = raw.get("gateway", {})
    logging_raw = raw.get("logging", {})
    cache_raw = raw.get("cache", {})

    gateway = GatewayConfig(
        listen_host=_get(gateway_raw, "listen_host", "0.0.0.0"),
        listen_port=int(_get(gateway_raw, "listen_port", 8080)),
        api_key=_get(gateway_raw, "api_key", ""),
        trusted_proxies=list(_get(gateway_raw, "trusted_proxies", ["127.0.0.1", "::1"])),
        request_max_bytes=int(_get(gateway_raw, "request_max_bytes", 2 * 1024 * 1024)),
        rate_limit_per_minute=int(_get(gateway_raw, "rate_limit_per_minute", 120)),
        circuit_breaker_fail_threshold=int(_get(gateway_raw, "circuit_breaker_fail_threshold", 20)),
        circuit_breaker_open_seconds=int(_get(gateway_raw, "circuit_breaker_open_seconds", 30)),
    )

    logging_cfg = LoggingConfig(
        stdout_json=bool(_get(logging_raw, "stdout_json", True)),
    )

    cache_cfg = CacheConfig(
        enabled=bool(_get(cache_raw, "enabled", True)),
        max_entries=int(_get(cache_raw, "max_entries", 1000)),
        default_ttl_minutes=int(_get(cache_raw, "default_ttl_minutes", 60)),
        client_scoped_tools=list(_get(cache_raw, "client_scoped_tools", [])),
    )

    upstreams = []
    for item in raw.get("upstreams", []) or []:
        upstreams.append(
            UpstreamConfig(
                id=item["id"],
                name=item.get("name", item["id"]),
                transport=item["transport"],
                endpoint=item.get("endpoint"),
                http_headers=item.get("http_headers", {}) or {},
                bearer_token_env_var=item.get("bearer_token_env_var"),
                http_serialize_requests=bool(item.get("http_serialize_requests", False)),
                command=_normalize_stdio_command(item),
                env=item.get("env", {}) or {},
                cwd=item.get("cwd"),
                timeout_ms=int(item.get("timeout_ms", 10000)),
                stdio_read_limit_bytes=int(item.get("stdio_read_limit_bytes", 100 * 1024 * 1024)),
                max_in_flight=int(item.get("max_in_flight", 20)),
                deny_tools=item.get("deny_tools", []) or [],
                cache_ttl_minutes=(int(item["cache_ttl_minutes"]) if item.get("cache_ttl_minutes") is not None else None),
                circuit_breaker_fail_threshold=item.get("circuit_breaker_fail_threshold"),
                circuit_breaker_open_seconds=item.get("circuit_breaker_open_seconds"),
                tool_routes=item.get("tool_routes", []) or [],
            )
        )

    return AppConfig(
        gateway=gateway,
        logging=logging_cfg,
        cache=cache_cfg,
        upstreams=upstreams,
    )
