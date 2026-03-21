from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import yaml

ENV_REF_PATTERN = re.compile(r"\$\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?::-(?P<default>[^}]*))?\}")
ENV_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

AUTH_MODE_SINGLE_SHARED = "single_shared"
AUTH_MODE_POSTGRES_API_KEYS = "postgres_api_keys"
VALID_AUTH_MODES = frozenset(
    {
        AUTH_MODE_SINGLE_SHARED,
        AUTH_MODE_POSTGRES_API_KEYS,
    }
)
VALID_UPSTREAM_TRANSPORTS = frozenset({"stdio", "streamable_http"})


@dataclass
class GatewayConfig:
    listen_host: str
    listen_port: int
    auth_mode: str
    api_key: str
    bootstrap_admin_api_key: str
    allow_unauthenticated: bool
    public_tools_catalog: bool
    trusted_proxies: List[str]
    request_max_bytes: int
    rate_limit_per_minute: int
    circuit_breaker_fail_threshold: int
    circuit_breaker_open_seconds: int

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "GatewayConfig":
        auth_mode = str(_get(data, "auth_mode", AUTH_MODE_SINGLE_SHARED))
        if auth_mode not in VALID_AUTH_MODES:
            raise ValueError("gateway.auth_mode must be one of: single_shared, postgres_api_keys")
        return cls(
            listen_host=_get(data, "listen_host", "0.0.0.0"),
            listen_port=int(_get(data, "listen_port", 8080)),
            auth_mode=auth_mode,
            api_key=_get(data, "api_key", ""),
            bootstrap_admin_api_key=_get(data, "bootstrap_admin_api_key", ""),
            allow_unauthenticated=bool(_get(data, "allow_unauthenticated", False)),
            public_tools_catalog=bool(_get(data, "public_tools_catalog", False)),
            trusted_proxies=[str(proxy) for proxy in list(_get(data, "trusted_proxies", ["127.0.0.1", "::1"]))],
            request_max_bytes=int(_get(data, "request_max_bytes", 2 * 1024 * 1024)),
            rate_limit_per_minute=int(_get(data, "rate_limit_per_minute", 120)),
            circuit_breaker_fail_threshold=int(_get(data, "circuit_breaker_fail_threshold", 20)),
            circuit_breaker_open_seconds=int(_get(data, "circuit_breaker_open_seconds", 30)),
        )


@dataclass
class LoggingConfig:
    stdout_json: bool
    extra_redact_fields: List[str]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LoggingConfig":
        return cls(
            stdout_json=bool(_get(data, "stdout_json", True)),
            extra_redact_fields=[str(field) for field in _string_list(_get(data, "extra_redact_fields", []), "logging.extra_redact_fields")],
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
            client_scoped_tools=[str(tool) for tool in _string_list(_get(data, "client_scoped_tools", []), "cache.client_scoped_tools")],
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
        upstream_id = data["id"]
        bearer_token_env_var = data.get("bearer_token_env_var")
        _validate_env_var_name(bearer_token_env_var, f"upstreams[{upstream_id}].bearer_token_env_var")
        transport = str(data["transport"])
        if transport == "http_sse":
            raise ValueError("upstreams[].transport 'http_sse' has been removed; use 'streamable_http'")
        if transport not in VALID_UPSTREAM_TRANSPORTS:
            raise ValueError("upstreams[].transport must be one of: stdio, streamable_http")
        return cls(
            id=upstream_id,
            name=data.get("name", upstream_id),
            transport=transport,
            endpoint=data.get("endpoint"),
            http_headers=_string_map(data.get("http_headers", {}), f"upstreams[{upstream_id}].http_headers"),
            bearer_token_env_var=bearer_token_env_var,
            http_serialize_requests=bool(data.get("http_serialize_requests", False)),
            command=_normalize_stdio_command(data),
            env=_string_map(data.get("env", {}), f"upstreams[{upstream_id}].env"),
            cwd=data.get("cwd"),
            timeout_ms=int(data.get("timeout_ms", 10000)),
            stdio_read_limit_bytes=int(data.get("stdio_read_limit_bytes", 100 * 1024 * 1024)),
            max_in_flight=int(data.get("max_in_flight", 20)),
            deny_tools=[str(tool) for tool in _string_list(data.get("deny_tools", []), f"upstreams[{upstream_id}].deny_tools")],
            cache_ttl_minutes=_optional_int(data.get("cache_ttl_minutes")),
            circuit_breaker_fail_threshold=_optional_int(data.get("circuit_breaker_fail_threshold")),
            circuit_breaker_open_seconds=_optional_int(data.get("circuit_breaker_open_seconds")),
            tool_routes=[str(route) for route in _string_list(data.get("tool_routes", []), f"upstreams[{upstream_id}].tool_routes")],
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


def _string_map(value: Any, path: str) -> Dict[str, str]:
    if value in (None, {}):
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{path} must be a mapping of string keys to string values")
    return {str(key): str(item) for key, item in value.items()}


def _string_list(value: Any, path: str) -> List[Any]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"{path} must be a list")
    return value


def _validate_env_var_name(value: Any, path: str) -> None:
    if value in (None, ""):
        return
    if not isinstance(value, str) or not ENV_NAME_PATTERN.fullmatch(value):
        raise ValueError(f"Invalid environment variable name for '{path}': {value!r}")


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
