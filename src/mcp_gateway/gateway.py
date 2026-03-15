from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
from uuid import UUID, uuid4

from .auth import AuthService
from .authorization import AuthorizationService
from .cache import TTLCache
from .config import AppConfig, UpstreamConfig
from .jsonrpc import make_error_response, normalize_params
from .logging import Logger, Timer
from .postgres import PostgresStore
from .request_context import AuthenticatedPrincipal, RequestContext
from .router import build_routes, select_upstream
from .telemetry import GatewayTelemetry
from .upstreams import HTTPUpstream, StdioUpstream, UpstreamResponse

BUILTIN_REDACT_FIELDS = frozenset(
    {
        "authorization",
        "x-api-key",
        "api_key",
        "apikey",
        "token",
        "access_token",
        "refresh_token",
        "password",
        "secret",
        "client_secret",
    }
)

DISCOVERY_METHODS = frozenset({"tools/list", "resources/list", "resources/templates/list", "prompts/list"})
OPTIONAL_DISCOVERY_METHODS = frozenset({"resources/list", "resources/templates/list", "prompts/list"})


@dataclass
class GatewayResult:
    payload: Dict[str, Any]
    success: bool
    cache_hit: bool
    upstream_id: Optional[str]
    tool_name: Optional[str]
    request_id: UUID


@dataclass
class ToolRegistryState:
    tools: list[Dict[str, Any]]
    registry: Dict[str, str]
    upstream_tools: Dict[str, list[str]]
    duplicates: Dict[str, set[str]]


@dataclass
class RoutedRequest:
    payload: Dict[str, Any]
    params: Optional[Dict[str, Any]]
    requested_tool_name: Optional[str]
    tool_name: Optional[str]
    upstream: Optional[UpstreamConfig]
    cache_key: Optional[str]


@dataclass
class UpstreamExecution:
    success: bool
    payload: Dict[str, Any]
    log_payload: Dict[str, Any]
    error: Optional[Dict[str, Any]]


class Gateway:
    def __init__(self, config: AppConfig, store: PostgresStore, logger: Logger, telemetry: GatewayTelemetry) -> None:
        self._config = config
        self._store = store
        self._logger = logger
        self._telemetry = telemetry
        self._auth = AuthService(config, store, logger)
        self._authorization = AuthorizationService(config, store)
        self._routes = build_routes(config.upstreams)
        self._memory_cache = TTLCache(config.cache.max_entries)
        self._http_upstreams: Dict[str, HTTPUpstream] = {}
        self._stdio_upstreams: Dict[str, StdioUpstream] = {}
        self._upstream_semaphores: Dict[str, asyncio.Semaphore] = {}
        self._tool_registry: Dict[str, str] = {}
        self._tool_alias_registry: Dict[str, str] = {}
        self._upstream_tools: Dict[str, list[str]] = {u.id: [] for u in config.upstreams}
        self._registry_lock = asyncio.Lock()
        self._upstream_by_id: Dict[str, UpstreamConfig] = {u.id: u for u in config.upstreams}
        self._health_counters: Dict[str, Dict[str, Dict[str, int]]] = {}
        self._health_lock = asyncio.Lock()
        self._breaker_lock = asyncio.Lock()
        self._upstream_breakers: Dict[str, Dict[str, float]] = {}
        self._global_breaker: Dict[str, float] = {"consecutive_failures": 0, "open_until": 0.0}
        self._warmup_status: Dict[str, Dict[str, Any]] = {}
        self._redact_fields = BUILTIN_REDACT_FIELDS | {
            field.strip().lower() for field in config.logging.extra_redact_fields if field.strip()
        }

    async def close(self) -> None:
        await self._auth.close()
        for client in self._http_upstreams.values():
            await client.close()
        for client in self._stdio_upstreams.values():
            await client.close()

    async def authenticate_token(self, token: Optional[str]) -> Optional[AuthenticatedPrincipal]:
        return await self._auth.authenticate_token(token)

    def auth_required(self) -> bool:
        return self._auth.auth_required()

    def auth_mode_requires_database(self) -> bool:
        return self._auth.auth_mode_requires_database()

    async def get_user_by_id(self, user_id: str) -> Optional[Dict[str, Any]]:
        return await self._auth.get_user_by_id(user_id)

    async def list_users(self) -> list[Dict[str, Any]]:
        return await self._auth.list_users()

    async def create_user(self, *, subject: str, display_name: Optional[str], role: str) -> Optional[Dict[str, Any]]:
        return await self._auth.create_user(subject=subject, display_name=display_name, role=role)

    async def update_user(
        self,
        user_id: str,
        *,
        display_name: Optional[str] = None,
        role: Optional[str] = None,
        is_active: Optional[bool] = None,
    ) -> Optional[Dict[str, Any]]:
        return await self._auth.update_user(user_id, display_name=display_name, role=role, is_active=is_active)

    async def list_api_keys(self, *, user_id: str) -> list[Dict[str, Any]]:
        return await self._auth.list_api_keys(user_id=user_id)

    async def list_identities(self) -> list[Dict[str, Any]]:
        return await self._auth.list_identities()

    async def put_identity(
        self,
        *,
        subject: str,
        display_name: Optional[str] = None,
        email: Optional[str] = None,
        is_active: bool = True,
    ) -> Dict[str, Any]:
        return await self._auth.put_identity(
            subject,
            display_name=display_name,
            email=email,
            is_active=is_active,
        )

    async def patch_identity(
        self,
        subject: str,
        *,
        display_name: Optional[str] = None,
        email: Optional[str] = None,
        is_active: Optional[bool] = None,
    ) -> Optional[Dict[str, Any]]:
        return await self._auth.patch_identity(
            subject,
            display_name=display_name,
            email=email,
            is_active=is_active,
        )

    async def issue_api_key_for_user(
        self,
        *,
        user_id: str,
        key_name: str,
        expires_at: Optional[Any] = None,
    ) -> Dict[str, Optional[str]]:
        return await self._auth.issue_api_key_for_user(user_id=user_id, key_name=key_name, expires_at=expires_at)

    async def revoke_api_key(self, api_key_id: str, *, user_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        return await self._auth.revoke_api_key(api_key_id, user_id=user_id)

    async def list_groups(self) -> list[Dict[str, Any]]:
        return await self._auth.list_groups()

    async def create_group(self, *, name: str, description: Optional[str]) -> Optional[Dict[str, Any]]:
        return await self._auth.create_group(name=name, description=description)

    async def update_group(
        self,
        group_id: str,
        *,
        name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        return await self._auth.update_group(group_id, name=name, description=description)

    async def delete_group(self, group_id: str) -> bool:
        return await self._auth.delete_group(group_id)

    async def add_group_member(self, group_id: str, *, subject: str) -> Dict[str, Any]:
        return await self._auth.add_group_member(group_id, subject=subject)

    async def remove_group_member(self, group_id: str, *, subject: str) -> bool:
        return await self._auth.remove_group_member(group_id, subject=subject)

    async def list_group_integration_grants(self, group_id: str) -> list[Dict[str, Any]]:
        return await self._auth.list_group_integration_grants(group_id)

    async def add_group_integration_grant(self, group_id: str, *, upstream_id: str) -> Dict[str, Any]:
        if upstream_id not in self._upstream_by_id:
            raise ValueError("upstream_id is not configured")
        return await self._auth.add_group_integration_grant(group_id, upstream_id=upstream_id)

    async def remove_group_integration_grant(self, group_id: str, *, upstream_id: str) -> bool:
        return await self._auth.remove_group_integration_grant(group_id, upstream_id=upstream_id)

    async def list_group_platform_grants(self, group_id: str) -> list[Dict[str, Any]]:
        return await self._auth.list_group_platform_grants(group_id)

    async def add_group_platform_grant(self, group_id: str, *, permission: str) -> Dict[str, Any]:
        return await self._auth.add_group_platform_grant(group_id, permission=permission)

    async def remove_group_platform_grant(self, group_id: str, *, permission: str) -> bool:
        return await self._auth.remove_group_platform_grant(group_id, permission=permission)

    async def authorize_platform(self, principal: Optional[AuthenticatedPrincipal], permission: str) -> bool:
        return await self._authorization.authorize_platform(principal, permission)

    async def authorize_integration(self, principal: Optional[AuthenticatedPrincipal], upstream_id: str) -> bool:
        return await self._authorization.authorize_integration(principal, upstream_id)

    async def list_integrations(self) -> list[Dict[str, Any]]:
        return [{"id": upstream.id, "name": upstream.name} for upstream in self._config.upstreams]

    async def usage_summary(
        self,
        *,
        group_by: str,
        from_timestamp: Optional[Any] = None,
        to_timestamp: Optional[Any] = None,
    ) -> list[Dict[str, Any]]:
        return await self._auth.usage_summary(
            group_by=group_by,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
        )

    def _redact_for_storage(self, value: Any) -> Any:
        if isinstance(value, dict):
            redacted: Dict[str, Any] = {}
            for key, item in value.items():
                normalized_key = str(key).strip().lower()
                if normalized_key in self._redact_fields:
                    redacted[key] = "[REDACTED]"
                else:
                    redacted[key] = self._redact_for_storage(item)
            return redacted
        if isinstance(value, list):
            return [self._redact_for_storage(item) for item in value]
        return value

    def _duplicate_tool_message(self, duplicates: Dict[str, set[str]]) -> str:
        details = ", ".join(
            f"{tool_name} ({', '.join(sorted(upstream_ids))})"
            for tool_name, upstream_ids in sorted(duplicates.items())
        )
        return f"Duplicate tool names detected across upstreams: {details}"

    def _find_duplicate_tool_assignments(self, tool_sources: Dict[str, set[str]]) -> Dict[str, set[str]]:
        return {
            tool_name: upstream_ids
            for tool_name, upstream_ids in tool_sources.items()
            if len(upstream_ids) > 1
        }

    def _build_tool_registry_state(
        self,
        tool_payloads: list[tuple[UpstreamConfig, list[Dict[str, Any]]]],
    ) -> ToolRegistryState:
        seen: set[str] = set()
        merged_tools: list[Dict[str, Any]] = []
        registry: Dict[str, str] = {}
        tool_sources: Dict[str, set[str]] = {}
        upstream_tools: Dict[str, list[str]] = {upstream.id: [] for upstream in self._config.upstreams}

        for upstream, tools in tool_payloads:
            upstream_tool_names: list[str] = []
            for tool in tools:
                name = tool.get("name")
                if not isinstance(name, str) or not name:
                    continue
                if name not in upstream_tool_names:
                    upstream_tool_names.append(name)
                tool_sources.setdefault(name, set()).add(upstream.id)
                # Keep denied tools in the registry so tools/call can be routed and
                # rejected with an explicit policy-denied response.
                registry[name] = upstream.id
                if name in seen:
                    continue
                seen.add(name)
                merged_tools.append(tool)
            upstream_tools[upstream.id] = upstream_tool_names

        return ToolRegistryState(
            tools=merged_tools,
            registry=registry,
            upstream_tools=upstream_tools,
            duplicates=self._find_duplicate_tool_assignments(tool_sources),
        )

    async def _apply_tool_registry_state(self, state: ToolRegistryState) -> None:
        async with self._registry_lock:
            self._tool_registry = dict(state.registry)
            self._tool_alias_registry = self._build_tool_alias_registry(state.registry)
            self._upstream_tools = {upstream_id: list(tool_names) for upstream_id, tool_names in state.upstream_tools.items()}

    def _cache_ttl_seconds(self, upstream: UpstreamConfig) -> int:
        ttl_minutes = upstream.cache_ttl_minutes or self._config.cache.default_ttl_minutes
        return max(1, int(ttl_minutes)) * 60

    def _replace_tool_name(
        self,
        payload: Dict[str, Any],
        params: Optional[Dict[str, Any]],
        tool_name: str,
    ) -> tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
        params_copy = dict(params or {})
        if "name" in params_copy:
            params_copy["name"] = tool_name
        if "tool" in params_copy:
            params_copy["tool"] = tool_name
        payload_copy = dict(payload)
        payload_copy["params"] = params_copy
        return payload_copy, params_copy

    async def _safe_log_request(
        self,
        request_id: UUID,
        method: str,
        params: Optional[Dict[str, Any]],
        raw_request: Dict[str, Any],
        upstream_id: Optional[str],
        tool_name: Optional[str],
        request_context: RequestContext,
        cache_key: Optional[str],
    ) -> None:
        redacted_params = self._redact_for_storage(params) if params is not None else None
        redacted_raw_request = self._redact_for_storage(raw_request)
        principal = request_context.principal
        try:
            await self._store.log_request(
                request_id=request_id,
                method=method,
                params=redacted_params,
                raw_request=redacted_raw_request,
                upstream_id=upstream_id,
                tool_name=tool_name,
                client_id=request_context.client_id,
                auth_user_id=principal.user_id if principal else None,
                auth_api_key_id=principal.api_key_id if principal else None,
                auth_role=principal.role if principal else None,
                auth_subject=principal.subject if principal else None,
                auth_scheme=principal.auth_scheme if principal else None,
                auth_group_names=list(principal.group_names) if principal else [],
                authorized_upstream_id=upstream_id,
                cache_key=cache_key,
            )
        except Exception as exc:  # noqa: BLE001
            self._logger.warn(
                "store_operation_failed",
                operation="log_request",
                request_id=str(request_id),
                method=method,
                upstream_id=upstream_id,
                tool_name=tool_name,
                error=str(exc),
            )

    async def _safe_log_response(
        self,
        response_id: UUID,
        request_id: UUID,
        success: bool,
        latency_ms: int,
        cache_hit: bool,
        response: Dict[str, Any],
    ) -> None:
        redacted_response = self._redact_for_storage(response)
        try:
            await self._store.log_response(
                response_id=response_id,
                request_id=request_id,
                success=success,
                latency_ms=latency_ms,
                cache_hit=cache_hit,
                response=redacted_response,
            )
        except Exception as exc:  # noqa: BLE001
            self._logger.warn(
                "store_operation_failed",
                operation="log_response",
                request_id=str(request_id),
                error=str(exc),
            )

    async def _safe_log_denial(
        self,
        denial_id: UUID,
        request_id: UUID,
        upstream_id: Optional[str],
        tool_name: Optional[str],
        reason: str,
    ) -> None:
        try:
            await self._store.log_denial(denial_id, request_id, upstream_id, tool_name, reason)
        except Exception as exc:  # noqa: BLE001
            self._logger.warn(
                "store_operation_failed",
                operation="log_denial",
                request_id=str(request_id),
                upstream_id=upstream_id,
                tool_name=tool_name,
                error=str(exc),
            )

    async def _safe_cache_get(self, cache_key: str, request_id: UUID) -> Optional[Dict[str, Any]]:
        try:
            return await self._store.cache_get(cache_key)
        except Exception as exc:  # noqa: BLE001
            self._logger.warn(
                "store_operation_failed",
                operation="cache_get",
                request_id=str(request_id),
                cache_key=cache_key,
                error=str(exc),
            )
            return None

    async def _safe_cache_set(self, cache_key: str, response: Dict[str, Any], ttl_seconds: int, request_id: UUID) -> None:
        try:
            await self._store.cache_set(cache_key, response, ttl_seconds)
        except Exception as exc:  # noqa: BLE001
            self._logger.warn(
                "store_operation_failed",
                operation="cache_set",
                request_id=str(request_id),
                cache_key=cache_key,
                error=str(exc),
            )

    async def _record_health(self, upstream_id: str, method: str, success: bool) -> None:
        async with self._breaker_lock:
            upstream_cfg = self._upstream_by_id.get(upstream_id)
            upstream_state = self._upstream_breakers.setdefault(upstream_id, {"consecutive_failures": 0, "open_until": 0.0})
            if success:
                upstream_state["consecutive_failures"] = 0
                self._global_breaker["consecutive_failures"] = 0
            else:
                upstream_state["consecutive_failures"] += 1
                self._global_breaker["consecutive_failures"] += 1
                upstream_threshold = int(
                    upstream_cfg.circuit_breaker_fail_threshold
                    if upstream_cfg and upstream_cfg.circuit_breaker_fail_threshold is not None
                    else self._config.gateway.circuit_breaker_fail_threshold
                )
                upstream_open_seconds = int(
                    upstream_cfg.circuit_breaker_open_seconds
                    if upstream_cfg and upstream_cfg.circuit_breaker_open_seconds is not None
                    else self._config.gateway.circuit_breaker_open_seconds
                )
                if int(upstream_state["consecutive_failures"]) >= max(1, upstream_threshold):
                    upstream_state["open_until"] = time.monotonic() + max(1, upstream_open_seconds)
                if int(self._global_breaker["consecutive_failures"]) >= max(1, self._config.gateway.circuit_breaker_fail_threshold):
                    self._global_breaker["open_until"] = (
                        time.monotonic() + max(1, self._config.gateway.circuit_breaker_open_seconds)
                    )
        async with self._health_lock:
            upstream_entry = self._health_counters.setdefault(upstream_id, {})
            method_entry = upstream_entry.setdefault(method, {"success": 0, "fail": 0})
            method_entry["success" if success else "fail"] += 1
        self._telemetry.record_upstream_outcome(upstream_id, method, success)

    def _get_tool_name(self, method: str, params: Optional[Dict[str, Any]]) -> Optional[str]:
        if method != "tools/call" or not params:
            return None
        return params.get("tool") or params.get("name")

    def _is_cacheable(self, method: str, tool_name: Optional[str]) -> bool:
        if not self._config.cache.enabled:
            return False
        if method != "tools/call":
            return False
        if not tool_name:
            return False
        return True

    def _cache_key(
        self,
        upstream: UpstreamConfig,
        method: str,
        tool_name: str,
        params: Any,
        client_id: Optional[str],
    ) -> str:
        normalized = normalize_params(params)
        if tool_name in self._config.cache.client_scoped_tools:
            scoped_client = client_id or "anonymous"
            return f"{upstream.id}:{method}:{tool_name}:client={scoped_client}:{normalized}"
        return f"{upstream.id}:{method}:{tool_name}:{normalized}"

    def _deny(self, upstream: UpstreamConfig, tool_name: Optional[str]) -> Optional[str]:
        if not tool_name:
            return None
        if tool_name in upstream.deny_tools:
            return f"Tool '{tool_name}' denied for upstream '{upstream.id}'"
        return None

    def _tool_alias(self, upstream_id: Optional[str], tool_name: Optional[str]) -> Optional[str]:
        if not upstream_id or not tool_name:
            return None
        return f"{upstream_id}.{tool_name}"

    def _aliases_for_tool(self, upstream_id: str, tool_name: str) -> set[str]:
        aliases: set[str] = {tool_name}
        single = re.sub(r"[^A-Za-z0-9_-]", "_", tool_name)
        triple = re.sub(r"[^A-Za-z0-9_-]", "___", tool_name)
        aliases.update({single, triple})
        aliases.update(
            {
                f"{upstream_id}_{single}",
                f"{upstream_id}___{single}",
                f"{upstream_id}_{triple}",
                f"{upstream_id}___{triple}",
            }
        )
        if tool_name.startswith(f"{upstream_id}."):
            stripped = tool_name[len(upstream_id) + 1 :]
            aliases.update(
                {
                    stripped,
                    re.sub(r"[^A-Za-z0-9_-]", "_", stripped),
                    re.sub(r"[^A-Za-z0-9_-]", "___", stripped),
                }
            )
        return {alias for alias in aliases if alias}

    def _build_tool_alias_registry(self, registry: Dict[str, str]) -> Dict[str, str]:
        alias_to_tool: Dict[str, str] = {}
        collisions: set[str] = set()
        for tool_name, upstream_id in registry.items():
            for alias in self._aliases_for_tool(upstream_id, tool_name):
                if alias in collisions:
                    continue
                existing = alias_to_tool.get(alias)
                if existing and existing != tool_name:
                    alias_to_tool.pop(alias, None)
                    collisions.add(alias)
                    continue
                alias_to_tool[alias] = tool_name
        return alias_to_tool

    def _resolve_tool_name(self, requested_tool_name: str) -> str:
        if requested_tool_name in self._tool_registry:
            return requested_tool_name
        resolved = self._tool_alias_registry.get(requested_tool_name)
        return resolved or requested_tool_name

    async def _route_request(
        self,
        payload: Dict[str, Any],
        method: str,
        params: Optional[Dict[str, Any]],
        client_id: Optional[str],
    ) -> RoutedRequest:
        requested_tool_name = self._get_tool_name(method, params)
        tool_name = requested_tool_name
        registry_upstream_id: Optional[str] = None

        if method == "tools/call" and tool_name:
            async with self._registry_lock:
                resolved = self._resolve_tool_name(tool_name)
                registry_upstream_id = self._tool_registry.get(resolved)
            if resolved != tool_name:
                payload, params = self._replace_tool_name(payload, params, resolved)
                tool_name = resolved
            else:
                tool_name = resolved

        upstream = self._upstream_by_id.get(registry_upstream_id) if registry_upstream_id else None
        if upstream is None:
            upstream = select_upstream(self._config.upstreams, self._routes, tool_name)

        cache_key: Optional[str] = None
        if upstream and self._is_cacheable(method, tool_name):
            cache_key = self._cache_key(upstream, method, tool_name or "", params, client_id)

        return RoutedRequest(
            payload=payload,
            params=params,
            requested_tool_name=requested_tool_name,
            tool_name=tool_name,
            upstream=upstream,
            cache_key=cache_key,
        )

    def _log_upstream_stderr(self, upstream_id: str, line: str) -> None:
        event_name = "upstream_process_log"
        base_fields = {"upstream_id": upstream_id, "stream": "stderr", "line": line}
        lowered = line.lower()
        error_markers = (" error ", "\terror\t", "fatal", "panic", "exception", "traceback")
        warn_markers = (" warn ", "\twarn\t", "deprecated", "deprecation", "retrying", "rate limit")
        info_markers = (" info ", "\tinfo\t", "running on stdio", "starting stdio server", "initialized")
        if any(marker in lowered for marker in error_markers):
            self._logger.error(event_name, **base_fields)
            return
        if any(marker in lowered for marker in warn_markers):
            self._logger.warn(event_name, **base_fields)
            return
        if any(marker in lowered for marker in info_markers):
            self._logger.info(event_name, **base_fields)
            return
        self._logger.warn(event_name, **base_fields)

    def _merge_dicts(self, base: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(base)
        for key, value in incoming.items():
            if key in out and isinstance(out[key], dict) and isinstance(value, dict):
                out[key] = self._merge_dicts(out[key], value)
            else:
                out[key] = value
        return out

    def status_snapshot(self) -> Dict[str, Any]:
        now = time.monotonic()
        upstream_breakers = {}
        for upstream_id, state in self._upstream_breakers.items():
            upstream_breakers[upstream_id] = {
                "consecutive_failures": int(state.get("consecutive_failures", 0)),
                "open": now < float(state.get("open_until", 0.0)),
                "open_seconds_remaining": max(0, int(float(state.get("open_until", 0.0)) - now)),
            }
        return {
            "warmup": self._warmup_status,
            "global_breaker": {
                "consecutive_failures": int(self._global_breaker.get("consecutive_failures", 0)),
                "open": now < float(self._global_breaker.get("open_until", 0.0)),
                "open_seconds_remaining": max(0, int(float(self._global_breaker.get("open_until", 0.0)) - now)),
            },
            "upstream_breakers": upstream_breakers,
        }

    def _startup_failure_details(self, status: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
        if not status.get("initialize_success"):
            return "initialize", self._error_message(status.get("initialize_error"))
        if not status.get("tools_list_success"):
            return "tools/list", self._error_message(status.get("tools_list_error"))
        return None, None

    def _error_message(self, error: Any) -> Optional[str]:
        if isinstance(error, dict):
            message = error.get("message")
            data = error.get("data")
            if isinstance(data, str) and data.strip():
                if isinstance(message, str) and message.strip() and data.strip() != message.strip():
                    return f"{message}: {data}"
                return data.strip()
            if isinstance(message, str) and message.strip():
                return message.strip()
            return None
        if isinstance(error, str) and error.strip():
            return error.strip()
        return None

    def startup_summary(self) -> Dict[str, Any]:
        upstreams: list[Dict[str, Any]] = []
        ready_upstream_count = 0
        degraded_upstream_count = 0
        failed_upstream_count = 0

        for upstream in self._config.upstreams:
            status = self._warmup_status.get(upstream.id, {})
            initialize_success = bool(status.get("initialize_success"))
            tools_list_success = bool(status.get("tools_list_success"))
            if initialize_success and tools_list_success:
                lifecycle_status = "ready"
                ready_upstream_count += 1
            elif initialize_success or tools_list_success:
                lifecycle_status = "degraded"
                degraded_upstream_count += 1
            else:
                lifecycle_status = "failed"
                failed_upstream_count += 1

            entry: Dict[str, Any] = {
                "id": upstream.id,
                "status": lifecycle_status,
                "tool_count": int(status.get("tool_count", 0)),
            }
            stage, reason = self._startup_failure_details(status)
            if stage:
                entry["stage"] = stage
            if reason:
                entry["reason"] = reason
            upstreams.append(entry)

        return {
            "gateway_ready": self.is_ready(),
            "ready_upstream_count": ready_upstream_count,
            "degraded_upstream_count": degraded_upstream_count,
            "failed_upstream_count": failed_upstream_count,
            "upstreams": upstreams,
        }

    def is_ready(self) -> bool:
        return any(status.get("initialize_success") for status in self._warmup_status.values())

    async def tools_catalog(self) -> Dict[str, Any]:
        async with self._registry_lock:
            upstream_tools = {k: list(v) for k, v in self._upstream_tools.items()}
            registry_size = len(self._tool_registry)
        upstreams_payload = []
        for upstream in self._config.upstreams:
            discovered = upstream_tools.get(upstream.id, [])
            exposed = [tool for tool in discovered if tool not in upstream.deny_tools]
            upstreams_payload.append(
                {
                    "id": upstream.id,
                    "name": upstream.name,
                    "tool_count": len(discovered),
                    "exposed_tool_count": len(exposed),
                    "exposed_tools": exposed,
                    "deny_tools": list(upstream.deny_tools),
                }
            )
        return {
            "exposed_tool_registry_size": registry_size,
            "upstreams": upstreams_payload,
        }

    def _is_global_breaker_open(self) -> bool:
        return time.monotonic() < float(self._global_breaker.get("open_until", 0.0))

    def _is_upstream_breaker_open(self, upstream_id: str) -> bool:
        state = self._upstream_breakers.get(upstream_id)
        if not state:
            return False
        return time.monotonic() < float(state.get("open_until", 0.0))

    async def _call_upstream(self, upstream: UpstreamConfig, payload: Dict[str, Any]) -> UpstreamResponse:
        if self._is_global_breaker_open():
            raise RuntimeError("Global circuit breaker open")
        if self._is_upstream_breaker_open(upstream.id):
            raise RuntimeError(f"Upstream circuit breaker open: {upstream.id}")
        client = await self._get_upstream_client(upstream)
        semaphore = self._upstream_semaphores.setdefault(upstream.id, asyncio.Semaphore(max(1, upstream.max_in_flight)))
        async with semaphore:
            return await client.call(payload)

    async def _notify_upstream(self, upstream: UpstreamConfig, payload: Dict[str, Any]) -> None:
        if self._is_global_breaker_open():
            raise RuntimeError("Global circuit breaker open")
        if self._is_upstream_breaker_open(upstream.id):
            raise RuntimeError(f"Upstream circuit breaker open: {upstream.id}")
        client = await self._get_upstream_client(upstream)
        semaphore = self._upstream_semaphores.setdefault(upstream.id, asyncio.Semaphore(max(1, upstream.max_in_flight)))
        async with semaphore:
            await client.notify(payload)

    def _log_upstream_exception(
        self,
        upstream_id: str,
        method: str,
        code: int,
        client_message: str,
        exc: BaseException,
        *,
        notification: bool,
    ) -> None:
        detail = str(exc) or type(exc).__name__
        self._logger.error(
            "upstream_request_failed",
            upstream_id=upstream_id,
            method=method,
            notification=notification,
            code=code,
            client_message=client_message,
            error_type=type(exc).__name__,
            error=detail,
        )

    async def _execute_upstream_operation(
        self,
        upstream: UpstreamConfig,
        method: str,
        payload: Dict[str, Any],
        *,
        notification: bool = False,
    ) -> UpstreamExecution:
        try:
            if notification:
                await self._notify_upstream(upstream, payload)
                accepted = {"accepted": True}
                return UpstreamExecution(success=True, payload=accepted, log_payload=accepted, error=None)

            response = await self._call_upstream(upstream, payload)
            error = response.payload.get("error")
            return UpstreamExecution(
                success=response.success,
                payload=response.payload,
                log_payload=response.payload,
                error=error if isinstance(error, dict) else None,
            )
        except asyncio.TimeoutError as exc:
            error = {"code": -32002, "message": "Upstream timeout"}
            self._log_upstream_exception(
                upstream.id,
                method,
                error["code"],
                error["message"],
                exc,
                notification=notification,
            )
        except RuntimeError as exc:
            error = {"code": -32004, "message": "Upstream unavailable"}
            self._log_upstream_exception(
                upstream.id,
                method,
                error["code"],
                error["message"],
                exc,
                notification=notification,
            )
        except Exception as exc:  # noqa: BLE001
            error = {"code": -32003, "message": "Upstream request failed"}
            self._log_upstream_exception(
                upstream.id,
                method,
                error["code"],
                error["message"],
                exc,
                notification=notification,
            )

        if notification:
            return UpstreamExecution(
                success=False,
                payload={"accepted": False},
                log_payload={"accepted": False, "error": error},
                error=error,
            )

        error_payload = make_error_response(payload.get("id"), error["code"], error["message"])
        return UpstreamExecution(
            success=False,
            payload=error_payload,
            log_payload=error_payload,
            error=error,
        )

    async def _load_cached_response(self, cache_key: str, request_id: UUID) -> Optional[Dict[str, Any]]:
        cached = await self._memory_cache.get(cache_key)
        if cached is not None:
            return cached
        return await self._safe_cache_get(cache_key, request_id)

    async def _log_request_start(
        self,
        request_id: UUID,
        method: str,
        params: Optional[Dict[str, Any]],
        raw_request: Dict[str, Any],
        upstream_id: Optional[str],
        tool_name: Optional[str],
        request_context: RequestContext,
        cache_key: Optional[str],
        requested_tool_name: Optional[str] = None,
    ) -> None:
        await self._safe_log_request(
            request_id=request_id,
            method=method,
            params=params,
            raw_request=raw_request,
            upstream_id=upstream_id,
            tool_name=tool_name,
            request_context=request_context,
            cache_key=cache_key,
        )
        principal = request_context.principal
        self._logger.info(
            "mcp_request",
            request_id=str(request_id),
            method=method,
            upstream_id=upstream_id,
            tool_name=tool_name,
            tool_name_requested=requested_tool_name,
            tool_alias=self._tool_alias(upstream_id, tool_name),
            client_id=request_context.client_id,
            auth_subject=principal.subject if principal else None,
            auth_scheme=principal.auth_scheme if principal else None,
            auth_role=principal.role if principal else None,
            auth_groups=list(principal.group_names) if principal else [],
            cache_key=cache_key,
        )

    async def _finalize_request(
        self,
        request_id: UUID,
        method: str,
        response_payload: Dict[str, Any],
        success: bool,
        cache_hit: bool,
        latency_ms: int,
        upstream_id: Optional[str],
        tool_name: Optional[str],
        *,
        log_response: bool = True,
        store_payload: Optional[Dict[str, Any]] = None,
        error: Optional[Dict[str, Any]] = None,
        event_name: str = "mcp_response",
        extra_log_fields: Optional[Dict[str, Any]] = None,
    ) -> GatewayResult:
        if log_response:
            await self._safe_log_response(
                response_id=uuid4(),
                request_id=request_id,
                success=success,
                latency_ms=latency_ms,
                cache_hit=cache_hit,
                response=store_payload if store_payload is not None else response_payload,
            )

        log_fields: Dict[str, Any] = {
            "request_id": str(request_id),
            "method": method,
            "upstream_id": upstream_id,
            "tool_name": tool_name,
            "tool_alias": self._tool_alias(upstream_id, tool_name),
            "cache_hit": cache_hit,
            "latency_ms": latency_ms,
            "success": success,
        }
        if extra_log_fields:
            log_fields.update(extra_log_fields)
        if error is not None:
            log_fields["error"] = error
        self._logger.info(event_name, **log_fields)
        self._telemetry.record_response(
            method=method,
            success=success,
            cache_hit=cache_hit,
            latency_ms=latency_ms,
            upstream_id=upstream_id,
            tool_name=tool_name,
        )
        return GatewayResult(
            payload=response_payload,
            success=success,
            cache_hit=cache_hit,
            upstream_id=upstream_id,
            tool_name=tool_name,
            request_id=request_id,
        )

    async def _aggregate_list(self, payload: Dict[str, Any], method: str) -> Tuple[Dict[str, Any], bool, list[Dict[str, Any]]]:
        results: list[Dict[str, Any]] = []
        successful_upstreams = 0
        upstream_errors: list[Dict[str, Any]] = []

        for upstream in self._config.upstreams:
            execution = await self._execute_upstream_operation(upstream, method, payload)
            error = execution.error
            if isinstance(error, dict):
                code = error.get("code")
                # Some MCP servers do not implement optional discovery methods.
                if code == -32601 and method in OPTIONAL_DISCOVERY_METHODS:
                    await self._record_health(upstream.id, method, True)
                    successful_upstreams += 1
                    results.append({"upstream": upstream, "result": {}})
                    continue
                await self._record_health(upstream.id, method, False)
                upstream_errors.append(
                    {
                        "upstream_id": upstream.id,
                        "reason": "upstream_error",
                        "code": code,
                        "message": error.get("message"),
                    }
                )
                continue
            result = execution.payload.get("result")
            if isinstance(result, dict):
                await self._record_health(upstream.id, method, True)
                successful_upstreams += 1
                results.append({"upstream": upstream, "result": result})
            else:
                await self._record_health(upstream.id, method, False)
                upstream_errors.append(
                    {
                        "upstream_id": upstream.id,
                        "reason": "invalid_result",
                    }
                )

        merged: Dict[str, Any] = {}
        if method == "tools/list":
            registry_state = self._build_tool_registry_state(
                [(item["upstream"], item["result"].get("tools", []) or []) for item in results]
            )
            if registry_state.duplicates:
                message = self._duplicate_tool_message(registry_state.duplicates)
                upstream_errors.append(
                    {
                        "reason": "duplicate_tools",
                        "duplicates": {name: sorted(upstream_ids) for name, upstream_ids in registry_state.duplicates.items()},
                    }
                )
                return make_error_response(payload.get("id"), -32003, message), False, upstream_errors
            merged["tools"] = registry_state.tools
            await self._apply_tool_registry_state(registry_state)
        elif method == "resources/list":
            seen = set()
            resources: list[Dict[str, Any]] = []
            for item in results:
                for resource in item["result"].get("resources", []) or []:
                    uri = resource.get("uri")
                    if not uri or uri in seen:
                        continue
                    seen.add(uri)
                    resources.append(resource)
            merged["resources"] = resources
        elif method == "prompts/list":
            seen = set()
            prompts: list[Dict[str, Any]] = []
            for item in results:
                for prompt in item["result"].get("prompts", []) or []:
                    name = prompt.get("name")
                    if not name or name in seen:
                        continue
                    seen.add(name)
                    prompts.append(prompt)
            merged["prompts"] = prompts
        elif method == "resources/templates/list":
            seen = set()
            resource_templates: list[Dict[str, Any]] = []
            for item in results:
                for template in item["result"].get("resourceTemplates", []) or []:
                    uri_template = template.get("uriTemplate")
                    if not uri_template or uri_template in seen:
                        continue
                    seen.add(uri_template)
                    resource_templates.append(template)
            merged["resourceTemplates"] = resource_templates

        success = successful_upstreams > 0
        return {"jsonrpc": "2.0", "id": payload.get("id"), "result": merged}, success, upstream_errors

    async def _get_upstream_client(self, upstream: UpstreamConfig):
        if upstream.transport == "http_sse":
            client = self._http_upstreams.get(upstream.id)
            if not client:
                if not upstream.endpoint:
                    raise RuntimeError(f"Upstream '{upstream.id}' missing endpoint")
                client = HTTPUpstream(
                    upstream.endpoint,
                    upstream.timeout_ms,
                    headers=upstream.http_headers,
                    bearer_token_env_var=upstream.bearer_token_env_var,
                    serialize_requests=upstream.http_serialize_requests,
                )
                self._http_upstreams[upstream.id] = client
            return client
        if upstream.transport == "stdio":
            client = self._stdio_upstreams.get(upstream.id)
            if not client:
                if not upstream.command:
                    raise RuntimeError(f"Upstream '{upstream.id}' missing command")
                client = StdioUpstream(
                    upstream.command,
                    upstream.env,
                    upstream.cwd,
                    upstream.timeout_ms,
                    upstream.stdio_read_limit_bytes,
                    upstream.id,
                    on_stderr_line=self._log_upstream_stderr,
                )
                self._stdio_upstreams[upstream.id] = client
            return client
        raise RuntimeError(f"Unknown transport '{upstream.transport}'")

    async def _fanout_initialize(self, payload: Dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
        successful = 0
        merged_capabilities: Dict[str, Any] = {}
        protocol_version: Optional[str] = None
        server_name = "mcp-gateway"
        server_version = "0.1.0"

        for upstream in self._config.upstreams:
            execution = await self._execute_upstream_operation(upstream, "initialize", payload)
            result = execution.payload.get("result")
            success = execution.success and isinstance(result, dict)
            await self._record_health(upstream.id, "initialize", success)
            if not success:
                continue
            successful += 1
            if protocol_version is None and isinstance(result.get("protocolVersion"), str):
                protocol_version = result["protocolVersion"]
            capabilities = result.get("capabilities")
            if isinstance(capabilities, dict):
                merged_capabilities = self._merge_dicts(merged_capabilities, capabilities)

        success = successful > 0
        if not success:
            return make_error_response(payload.get("id"), -32003, "No upstream responded to initialize"), False

        result_payload = {
            "protocolVersion": protocol_version or "2024-11-05",
            "capabilities": merged_capabilities,
            "serverInfo": {"name": server_name, "version": server_version},
        }
        return {"jsonrpc": "2.0", "id": payload.get("id"), "result": result_payload}, True

    async def _fanout_initialized_notification(self, payload: Dict[str, Any]) -> bool:
        successful = 0
        for upstream in self._config.upstreams:
            execution = await self._execute_upstream_operation(
                upstream,
                "notifications/initialized",
                payload,
                notification=True,
            )
            await self._record_health(upstream.id, "notifications/initialized", execution.success)
            if execution.success:
                successful += 1
        return successful > 0

    async def warmup(self) -> None:
        # Prime upstream sessions and seed tool registry before client traffic.
        tool_payloads: list[tuple[UpstreamConfig, list[Dict[str, Any]]]] = []
        for upstream in self._config.upstreams:
            init_success = False
            tools_list_success = False
            tool_names: list[str] = []
            init_error: Optional[Dict[str, Any]] = None
            tools_list_error: Optional[Dict[str, Any]] = None

            init_payload = {
                "jsonrpc": "2.0",
                "id": "warmup-initialize",
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {},
                    "clientInfo": {"name": "mcp-gateway", "version": "0.1.0"},
                },
            }
            init_execution = await self._execute_upstream_operation(upstream, "initialize", init_payload)
            init_success = init_execution.success and isinstance(init_execution.payload.get("result"), dict)
            if not init_success:
                init_error = init_execution.error
            await self._record_health(upstream.id, "initialize", init_success)

            if init_success:
                notify_payload = {
                    "jsonrpc": "2.0",
                    "method": "notifications/initialized",
                    "params": {},
                }
                notify_execution = await self._execute_upstream_operation(
                    upstream,
                    "notifications/initialized",
                    notify_payload,
                    notification=True,
                )
                await self._record_health(upstream.id, "notifications/initialized", notify_execution.success)

            tools_payload = {
                "jsonrpc": "2.0",
                "id": "warmup-tools-list",
                "method": "tools/list",
                "params": {},
            }
            tools_execution = await self._execute_upstream_operation(upstream, "tools/list", tools_payload)
            result = tools_execution.payload.get("result")
            if tools_execution.success and isinstance(result, dict):
                tools = result.get("tools", []) or []
                tool_payloads.append((upstream, tools))
                tool_names = [t.get("name") for t in tools if isinstance(t, dict) and isinstance(t.get("name"), str)]
                tools_list_success = True
            else:
                tools_list_success = False
                tools_list_error = tools_execution.error
            await self._record_health(upstream.id, "tools/list", tools_list_success)

            self._logger.info(
                "upstream_warmup",
                upstream_id=upstream.id,
                initialize_success=init_success,
                initialize_error=init_error,
                tools_list_success=tools_list_success,
                tools_list_error=tools_list_error,
                tool_count=len(tool_names),
                tools=tool_names,
            )
            self._warmup_status[upstream.id] = {
                "initialize_success": init_success,
                "initialize_error": init_error,
                "tools_list_success": tools_list_success,
                "tools_list_error": tools_list_error,
                "tool_count": len(tool_names),
                "tools": tool_names,
            }

        registry_state = self._build_tool_registry_state(tool_payloads)
        if registry_state.duplicates:
            raise RuntimeError(self._duplicate_tool_message(registry_state.duplicates))
        await self._apply_tool_registry_state(registry_state)

    async def handle(self, payload: Dict[str, Any], request_context: RequestContext) -> GatewayResult:
        request_id = uuid4()
        method = payload.get("method")
        params = payload.get("params")
        client_id = request_context.client_id
        if not isinstance(method, str):
            error_payload = make_error_response(payload.get("id"), -32600, "Invalid Request")
            return await self._finalize_request(
                request_id=request_id,
                method="invalid",
                response_payload=error_payload,
                success=False,
                cache_hit=False,
                latency_ms=0,
                upstream_id=None,
                tool_name=None,
                log_response=False,
                error=error_payload.get("error"),
            )
        self._telemetry.record_request(method)

        if method == "initialize":
            await self._log_request_start(
                request_id=request_id,
                method=method,
                params=params,
                raw_request=payload,
                upstream_id="*",
                tool_name=None,
                request_context=request_context,
                cache_key=None,
            )
            timer = Timer()
            response_payload, success = await self._fanout_initialize(payload)
            return await self._finalize_request(
                request_id=request_id,
                method=method,
                response_payload=response_payload,
                success=success,
                cache_hit=False,
                latency_ms=timer.elapsed_ms(),
                upstream_id="*",
                tool_name=None,
                error=response_payload.get("error") if isinstance(response_payload.get("error"), dict) else None,
            )

        if method == "notifications/initialized" and payload.get("id") is None:
            await self._log_request_start(
                request_id=request_id,
                method=method,
                params=params,
                raw_request=payload,
                upstream_id="*",
                tool_name=None,
                request_context=request_context,
                cache_key=None,
            )
            timer = Timer()
            success = await self._fanout_initialized_notification(payload)
            latency_ms = timer.elapsed_ms()
            response_payload = {"accepted": success}
            return await self._finalize_request(
                request_id=request_id,
                method=method,
                response_payload=response_payload,
                success=success,
                cache_hit=False,
                latency_ms=latency_ms,
                upstream_id="*",
                tool_name=None,
            )

        routed = await self._route_request(payload, method, params, client_id)
        if not routed.upstream:
            error_payload = make_error_response(payload.get("id"), -32000, "No upstream configured")
            return await self._finalize_request(
                request_id=request_id,
                method=method,
                response_payload=error_payload,
                success=False,
                cache_hit=False,
                latency_ms=0,
                upstream_id=None,
                tool_name=routed.tool_name,
                log_response=False,
                error=error_payload.get("error"),
            )
        await self._log_request_start(
            request_id=request_id,
            method=method,
            params=routed.params,
            raw_request=routed.payload,
            upstream_id=routed.upstream.id,
            tool_name=routed.tool_name,
            request_context=request_context,
            cache_key=routed.cache_key,
            requested_tool_name=routed.requested_tool_name,
        )

        if method in DISCOVERY_METHODS:
            timer = Timer()
            response_payload, success, upstream_errors = await self._aggregate_list(routed.payload, method)
            return await self._finalize_request(
                request_id=request_id,
                method=method,
                response_payload=response_payload,
                success=success,
                cache_hit=False,
                latency_ms=timer.elapsed_ms(),
                upstream_id="*",
                tool_name=None,
                error=response_payload.get("error") if isinstance(response_payload.get("error"), dict) else None,
                extra_log_fields={
                    "upstream_error_count": len(upstream_errors),
                    "upstream_errors": upstream_errors,
                },
            )

        if method == "tools/call" and (request_context.principal is not None or self.auth_required()):
            authorized = await self.authorize_integration(request_context.principal, routed.upstream.id)
            if not authorized:
                principal = request_context.principal
                denial_reason = (
                    f"Principal '{principal.subject if principal else 'anonymous'}' is not allowed to call "
                    f"integration '{routed.upstream.id}'"
                )
                denial_payload = make_error_response(
                    routed.payload.get("id"),
                    -32001,
                    "Blocked by gateway policy: integration not allowed",
                    data={
                        "category": "policy_denied",
                        "enforcer": "pycasbin",
                        "subject": principal.subject if principal else None,
                        "upstream_id": routed.upstream.id,
                        "tool_name": routed.tool_name,
                        "retryable": False,
                        "suggestion": "Use an allowed integration grant or contact gateway admin to update access.",
                    },
                )
                denial_id = uuid4()
                await self._safe_log_denial(denial_id, request_id, routed.upstream.id, routed.tool_name, denial_reason)
                self._telemetry.record_denial(routed.upstream.id, routed.tool_name)
                return await self._finalize_request(
                    request_id=request_id,
                    method=method,
                    response_payload=denial_payload,
                    success=False,
                    cache_hit=False,
                    latency_ms=0,
                    upstream_id=routed.upstream.id,
                    tool_name=routed.tool_name,
                    log_response=False,
                    event_name="mcp_denied",
                    error=denial_payload.get("error"),
                    extra_log_fields={"reason": denial_reason},
                )

        denial_reason = self._deny(routed.upstream, routed.tool_name)
        if denial_reason:
            denial_payload = make_error_response(
                routed.payload.get("id"),
                -32001,
                "Blocked by gateway policy: tool not allowed",
                data={
                    "category": "policy_denied",
                    "enforcer": "mcp-gateway",
                    "upstream_id": routed.upstream.id,
                    "tool_name": routed.tool_name,
                    "policy_type": "deny_tools",
                    "retryable": False,
                    "suggestion": "Use an allowed tool or contact gateway admin to update deny_tools policy.",
                },
            )
            denial_id = uuid4()
            await self._safe_log_denial(denial_id, request_id, routed.upstream.id, routed.tool_name, denial_reason)
            self._telemetry.record_denial(routed.upstream.id, routed.tool_name)
            return await self._finalize_request(
                request_id=request_id,
                method=method,
                response_payload=denial_payload,
                success=False,
                cache_hit=False,
                latency_ms=0,
                upstream_id=routed.upstream.id,
                tool_name=routed.tool_name,
                log_response=False,
                event_name="mcp_denied",
                error=denial_payload.get("error"),
                extra_log_fields={"reason": denial_reason},
            )

        if routed.payload.get("id") is None:
            timer = Timer()
            execution = await self._execute_upstream_operation(
                routed.upstream,
                method,
                routed.payload,
                notification=True,
            )
            await self._record_health(routed.upstream.id, method, execution.success)
            return await self._finalize_request(
                request_id=request_id,
                method=method,
                response_payload=execution.payload,
                success=execution.success,
                cache_hit=False,
                latency_ms=timer.elapsed_ms(),
                upstream_id=routed.upstream.id,
                tool_name=routed.tool_name,
                store_payload=execution.log_payload,
                error=execution.error,
            )

        if routed.cache_key:
            timer = Timer()
            cached = await self._load_cached_response(routed.cache_key, request_id)
            if cached is not None:
                return await self._finalize_request(
                    request_id=request_id,
                    method=method,
                    response_payload=cached,
                    success=True,
                    cache_hit=True,
                    latency_ms=timer.elapsed_ms(),
                    upstream_id=routed.upstream.id,
                    tool_name=routed.tool_name,
                )

        timer = Timer()
        execution = await self._execute_upstream_operation(routed.upstream, method, routed.payload)
        await self._record_health(routed.upstream.id, method, execution.success)
        result = await self._finalize_request(
            request_id=request_id,
            method=method,
            response_payload=execution.payload,
            success=execution.success,
            cache_hit=False,
            latency_ms=timer.elapsed_ms(),
            upstream_id=routed.upstream.id,
            tool_name=routed.tool_name,
            store_payload=execution.log_payload,
            error=execution.error,
        )
        if routed.cache_key and execution.success:
            ttl_seconds = self._cache_ttl_seconds(routed.upstream)
            await self._memory_cache.set(routed.cache_key, execution.payload, ttl_seconds)
            await self._safe_cache_set(routed.cache_key, execution.payload, ttl_seconds, request_id)
        return result
