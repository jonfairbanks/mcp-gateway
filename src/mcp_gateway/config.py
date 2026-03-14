from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import yaml

ENV_REF_PATTERN = re.compile(r"\$\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?::-(?P<default>[^}]*))?\}")


@dataclass
class GatewayConfig:
    listen_host: str
    listen_port: int
    api_key: str
    allow_unauthenticated: bool
    trusted_proxies: List[str]
    request_max_bytes: int
    rate_limit_per_minute: int
    circuit_breaker_fail_threshold: int
    circuit_breaker_open_seconds: int
    sse_queue_max_messages: int
    max_sse_sessions: int

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GatewayConfig":
        return cls(
            listen_host=_get(data, "listen_host", "0.0.0.0"),
            listen_port=int(_get(data, "listen_port", 8080)),
            api_key=_get(data, "api_key", ""),
            allow_unauthenticated=bool(_get(data, "allow_unauthenticated", False)),
            trusted_proxies=[str(proxy) for proxy in list(_get(data, "trusted_proxies", ["127.0.0.1", "::1"]))],
            request_max_bytes=int(_get(data, "request_max_bytes", 2 * 1024 * 1024)),
            rate_limit_per_minute=int(_get(data, "rate_limit_per_minute", 120)),
            circuit_breaker_fail_threshold=int(_get(data, "circuit_breaker_fail_threshold", 20)),
            circuit_breaker_open_seconds=int(_get(data, "circuit_breaker_open_seconds", 30)),
            sse_queue_max_messages=int(_get(data, "sse_queue_max_messages", 100)),
            max_sse_sessions=int(_get(data, "max_sse_sessions", 1000)),
        )


@dataclass
class LoggingConfig:
    stdout_json: bool
    extra_redact_fields: List[str]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LoggingConfig":
        return cls(
            stdout_json=bool(_get(data, "stdout_json", True)),
            extra_redact_fields=[str(field) for field in list(_get(data, "extra_redact_fields", []))],
        )


@dataclass
class CacheConfig:
    enabled: bool
    max_entries: int
    default_ttl_minutes: int
    client_scoped_tools: List[str]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CacheConfig":
        return cls(
            enabled=bool(_get(data, "enabled", True)),
            max_entries=int(_get(data, "max_entries", 1000)),
            default_ttl_minutes=int(_get(data, "default_ttl_minutes", 60)),
            client_scoped_tools=[str(tool) for tool in list(_get(data, "client_scoped_tools", []))],
        )


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

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "UpstreamConfig":
        return cls(
            id=data["id"],
            name=data.get("name", data["id"]),
            transport=data["transport"],
            endpoint=data.get("endpoint"),
            http_headers={str(key): str(value) for key, value in (data.get("http_headers", {}) or {}).items()},
            bearer_token_env_var=data.get("bearer_token_env_var"),
            http_serialize_requests=bool(data.get("http_serialize_requests", False)),
            command=_normalize_stdio_command(data),
            env={str(key): str(value) for key, value in (data.get("env", {}) or {}).items()},
            cwd=data.get("cwd"),
            timeout_ms=int(data.get("timeout_ms", 10000)),
            stdio_read_limit_bytes=int(data.get("stdio_read_limit_bytes", 100 * 1024 * 1024)),
            max_in_flight=int(data.get("max_in_flight", 20)),
            deny_tools=[str(tool) for tool in (data.get("deny_tools", []) or [])],
            cache_ttl_minutes=_optional_int(data.get("cache_ttl_minutes")),
            circuit_breaker_fail_threshold=_optional_int(data.get("circuit_breaker_fail_threshold")),
            circuit_breaker_open_seconds=_optional_int(data.get("circuit_breaker_open_seconds")),
            tool_routes=[str(route) for route in (data.get("tool_routes", []) or [])],
        )


@dataclass
class AppConfig:
    gateway: GatewayConfig
    logging: LoggingConfig
    cache: CacheConfig
    upstreams: List[UpstreamConfig]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AppConfig":
        return cls(
            gateway=GatewayConfig.from_dict(data.get("gateway", {})),
            logging=LoggingConfig.from_dict(data.get("logging", {})),
            cache=CacheConfig.from_dict(data.get("cache", {})),
            upstreams=[UpstreamConfig.from_dict(item) for item in (data.get("upstreams", []) or [])],
        )


def _get(data: Dict[str, Any], key: str, default: Any) -> Any:
    value = data.get(key, default)
    if value is None:
        return default
    return value


def _optional_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    return int(value)


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


def _expand_env_string(value: str, path: str) -> str:
    def replace(match: re.Match[str]) -> str:
        name = match.group("name")
        default = match.group("default")
        env_value = os.getenv(name)
        if default is not None:
            return env_value if env_value not in (None, "") else default
        if env_value is None:
            raise ValueError(f"Missing required environment variable '{name}' for config value '{path}'")
        return env_value

    return ENV_REF_PATTERN.sub(replace, value)


def _expand_env_refs(value: Any, path: str = "config") -> Any:
    if isinstance(value, dict):
        return {key: _expand_env_refs(item, f"{path}.{key}") for key, item in value.items()}
    if isinstance(value, list):
        return [_expand_env_refs(item, f"{path}[{index}]") for index, item in enumerate(value)]
    if isinstance(value, str):
        # Only expand explicit ${...} markers so ordinary strings remain literal.
        return _expand_env_string(value, path)
    return value


def load_config(path: str) -> AppConfig:
    with open(path, "r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    return AppConfig.from_dict(_expand_env_refs(raw))
