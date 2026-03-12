from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
from uuid import UUID, uuid4

from .cache import TTLCache
from .config import AppConfig, UpstreamConfig
from .jsonrpc import make_error_response, normalize_params
from .logging import Logger, Timer
from .postgres import PostgresStore
from .router import build_routes, select_upstream
from .telemetry import GatewayTelemetry
from .upstreams import HTTPUpstream, StdioUpstream, UpstreamResponse


@dataclass
class GatewayResult:
    payload: Dict[str, Any]
    success: bool
    cache_hit: bool
    upstream_id: Optional[str]
    tool_name: Optional[str]
    request_id: UUID


class Gateway:
    def __init__(self, config: AppConfig, store: PostgresStore, logger: Logger, telemetry: GatewayTelemetry) -> None:
        self._config = config
        self._store = store
        self._logger = logger
        self._telemetry = telemetry
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

    async def close(self) -> None:
        for client in self._http_upstreams.values():
            await client.close()
        for client in self._stdio_upstreams.values():
            await client.close()

    async def _safe_log_request(
        self,
        request_id: UUID,
        method: str,
        params: Optional[Dict[str, Any]],
        raw_request: Dict[str, Any],
        upstream_id: Optional[str],
        tool_name: Optional[str],
        client_id: Optional[str],
        cache_key: Optional[str],
    ) -> None:
        try:
            await self._store.log_request(
                request_id=request_id,
                method=method,
                params=params,
                raw_request=raw_request,
                upstream_id=upstream_id,
                tool_name=tool_name,
                client_id=client_id,
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
        try:
            await self._store.log_response(
                response_id=response_id,
                request_id=request_id,
                success=success,
                latency_ms=latency_ms,
                cache_hit=cache_hit,
                response=response,
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
            success_count = method_entry["success"]
            fail_count = method_entry["fail"]
        self._logger.info(
            "upstream_health",
            upstream_id=upstream_id,
            method=method,
            success_count=success_count,
            fail_count=fail_count,
            last_outcome="success" if success else "fail",
        )
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
                    "tools": discovered,
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

    async def _aggregate_list(self, payload: Dict[str, Any], method: str) -> Tuple[Dict[str, Any], bool, list[Dict[str, Any]]]:
        results: list[Dict[str, Any]] = []
        successful_upstreams = 0
        optional_discovery_methods = {"resources/list", "resources/templates/list", "prompts/list"}
        upstream_errors: list[Dict[str, Any]] = []

        for upstream in self._config.upstreams:
            try:
                response = await self._call_upstream(upstream, payload)
            except Exception as exc:  # noqa: BLE001
                await self._record_health(upstream.id, method, False)
                upstream_errors.append(
                    {
                        "upstream_id": upstream.id,
                        "reason": "exception",
                        "error": str(exc),
                    }
                )
                continue
            error = response.payload.get("error")
            if isinstance(error, dict):
                code = error.get("code")
                # Some MCP servers do not implement optional discovery methods.
                if code == -32601 and method in optional_discovery_methods:
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
            result = response.payload.get("result")
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
            seen: set[str] = set()
            tools: list[Dict[str, Any]] = []
            registry: Dict[str, str] = {}
            upstream_tools: Dict[str, list[str]] = {}
            for item in results:
                upstream = item["upstream"]
                upstream_tool_names: list[str] = []
                for tool in item["result"].get("tools", []) or []:
                    name = tool.get("name")
                    if not name:
                        continue
                    if name not in upstream_tool_names:
                        upstream_tool_names.append(name)
                    # Keep full registry (including denied tools) so tools/call can
                    # be routed and then explicitly denied with a clear error message.
                    registry[name] = upstream.id
                    if name in seen:
                        continue
                    seen.add(name)
                    tools.append(tool)
                upstream_tools[upstream.id] = upstream_tool_names
            merged["tools"] = tools
            async with self._registry_lock:
                self._tool_registry = registry
                self._tool_alias_registry = self._build_tool_alias_registry(registry)
                self._upstream_tools.update(upstream_tools)
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
            try:
                response = await self._call_upstream(upstream, payload)
            except Exception:  # noqa: BLE001
                await self._record_health(upstream.id, "initialize", False)
                continue
            if not response.success:
                await self._record_health(upstream.id, "initialize", False)
                continue
            result = response.payload.get("result")
            if not isinstance(result, dict):
                await self._record_health(upstream.id, "initialize", False)
                continue
            await self._record_health(upstream.id, "initialize", True)
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
            try:
                await self._notify_upstream(upstream, payload)
                await self._record_health(upstream.id, "notifications/initialized", True)
                successful += 1
            except Exception:  # noqa: BLE001
                await self._record_health(upstream.id, "notifications/initialized", False)
                continue
        return successful > 0

    async def warmup(self) -> None:
        # Prime upstream sessions and seed tool registry before client traffic.
        registry_updates: Dict[str, str] = {}
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
            try:
                init_response = await self._call_upstream(upstream, init_payload)
                init_success = init_response.success and isinstance(init_response.payload.get("result"), dict)
                if not init_success:
                    init_error = init_response.payload.get("error") if isinstance(init_response.payload, dict) else None
            except Exception:  # noqa: BLE001
                init_success = False
                init_error = {"message": "exception during initialize"}
            await self._record_health(upstream.id, "initialize", init_success)

            if init_success:
                notify_payload = {
                    "jsonrpc": "2.0",
                    "method": "notifications/initialized",
                    "params": {},
                }
                try:
                    await self._notify_upstream(upstream, notify_payload)
                    await self._record_health(upstream.id, "notifications/initialized", True)
                except Exception:  # noqa: BLE001
                    await self._record_health(upstream.id, "notifications/initialized", False)

            tools_payload = {
                "jsonrpc": "2.0",
                "id": "warmup-tools-list",
                "method": "tools/list",
                "params": {},
            }
            try:
                tools_response = await self._call_upstream(upstream, tools_payload)
                result = tools_response.payload.get("result")
                if tools_response.success and isinstance(result, dict):
                    tools = result.get("tools", []) or []
                    tool_names = [t.get("name") for t in tools if isinstance(t, dict) and isinstance(t.get("name"), str)]
                    tools_list_success = True
                    for tool_name in tool_names:
                        # Keep denied tools in the registry so tool invocations can be
                        # rejected by policy with an explicit deny response.
                        registry_updates[tool_name] = upstream.id
                else:
                    tools_list_success = False
                    tools_list_error = tools_response.payload.get("error") if isinstance(tools_response.payload, dict) else None
            except Exception:  # noqa: BLE001
                tools_list_success = False
                tools_list_error = {"message": "exception during tools/list"}
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
                "tools_list_success": tools_list_success,
                "tool_count": len(tool_names),
                "tools": tool_names,
            }

        if registry_updates:
            async with self._registry_lock:
                self._tool_registry.update(registry_updates)
                self._tool_alias_registry = self._build_tool_alias_registry(self._tool_registry)
                for upstream_id, status in self._warmup_status.items():
                    self._upstream_tools[upstream_id] = list(status.get("tools", []))

    async def handle(self, payload: Dict[str, Any], client_id: Optional[str]) -> GatewayResult:
        request_id = uuid4()
        method = payload.get("method")
        params = payload.get("params")
        if not isinstance(method, str):
            error_payload = make_error_response(payload.get("id"), -32600, "Invalid Request")
            self._telemetry.record_response(
                method="invalid",
                success=False,
                cache_hit=False,
                latency_ms=0,
                upstream_id=None,
                tool_name=None,
            )
            return GatewayResult(
                payload=error_payload,
                success=False,
                cache_hit=False,
                upstream_id=None,
                tool_name=None,
                request_id=request_id,
            )
        self._telemetry.record_request(method)
        if method == "initialize":
            await self._safe_log_request(
                request_id=request_id,
                method=method,
                params=params,
                raw_request=payload,
                upstream_id="*",
                tool_name=None,
                client_id=client_id,
                cache_key=None,
            )
            self._logger.info(
                "mcp_request",
                request_id=str(request_id),
                method=method,
                upstream_id="*",
                tool_name=None,
                tool_alias=None,
                client_id=client_id,
                cache_key=None,
            )
            timer = Timer()
            response_payload, success = await self._fanout_initialize(payload)
            latency_ms = timer.elapsed_ms()
            await self._safe_log_response(
                response_id=uuid4(),
                request_id=request_id,
                success=success,
                latency_ms=latency_ms,
                cache_hit=False,
                response=response_payload,
            )
            self._logger.info(
                "mcp_response",
                request_id=str(request_id),
                method=method,
                upstream_id="*",
                tool_name=None,
                tool_alias=None,
                cache_hit=False,
                latency_ms=latency_ms,
                success=success,
                error=response_payload.get("error"),
            )
            self._telemetry.record_response(
                method=method,
                success=success,
                cache_hit=False,
                latency_ms=latency_ms,
                upstream_id="*",
                tool_name=None,
            )
            return GatewayResult(
                payload=response_payload,
                success=success,
                cache_hit=False,
                upstream_id="*",
                tool_name=None,
                request_id=request_id,
            )

        if method == "notifications/initialized" and payload.get("id") is None:
            await self._safe_log_request(
                request_id=request_id,
                method=method,
                params=params,
                raw_request=payload,
                upstream_id="*",
                tool_name=None,
                client_id=client_id,
                cache_key=None,
            )
            self._logger.info(
                "mcp_request",
                request_id=str(request_id),
                method=method,
                upstream_id="*",
                tool_name=None,
                tool_alias=None,
                client_id=client_id,
                cache_key=None,
            )
            timer = Timer()
            success = await self._fanout_initialized_notification(payload)
            latency_ms = timer.elapsed_ms()
            response_payload = {"accepted": success}
            await self._safe_log_response(
                response_id=uuid4(),
                request_id=request_id,
                success=success,
                latency_ms=latency_ms,
                cache_hit=False,
                response=response_payload,
            )
            self._logger.info(
                "mcp_response",
                request_id=str(request_id),
                method=method,
                upstream_id="*",
                tool_name=None,
                tool_alias=None,
                cache_hit=False,
                latency_ms=latency_ms,
                success=success,
            )
            self._telemetry.record_response(
                method=method,
                success=success,
                cache_hit=False,
                latency_ms=latency_ms,
                upstream_id="*",
                tool_name=None,
            )
            return GatewayResult(
                payload=response_payload,
                success=success,
                cache_hit=False,
                upstream_id="*",
                tool_name=None,
                request_id=request_id,
            )

        requested_tool_name = self._get_tool_name(method, params)
        tool_name = requested_tool_name
        if method == "tools/call" and tool_name:
            async with self._registry_lock:
                resolved = self._resolve_tool_name(tool_name)
            if resolved != tool_name:
                tool_name = resolved
                params_copy = dict(params or {})
                if "name" in params_copy:
                    params_copy["name"] = tool_name
                if "tool" in params_copy:
                    params_copy["tool"] = tool_name
                payload = dict(payload)
                payload["params"] = params_copy
                params = params_copy
        upstream = None
        if method == "tools/call" and tool_name:
            async with self._registry_lock:
                upstream_id = self._tool_registry.get(tool_name)
            if upstream_id:
                for candidate in self._config.upstreams:
                    if candidate.id == upstream_id:
                        upstream = candidate
                        break
        if upstream is None:
            upstream = select_upstream(self._config.upstreams, self._routes, tool_name)
        cache_key: Optional[str] = None

        if not upstream:
            error_payload = make_error_response(payload.get("id"), -32000, "No upstream configured")
            self._telemetry.record_response(
                method=method,
                success=False,
                cache_hit=False,
                latency_ms=0,
                upstream_id=None,
                tool_name=tool_name,
            )
            return GatewayResult(
                payload=error_payload,
                success=False,
                cache_hit=False,
                upstream_id=None,
                tool_name=tool_name,
                request_id=request_id,
            )

        if self._is_cacheable(method, tool_name):
            cache_key = self._cache_key(upstream, method, tool_name or "", params, client_id)

        await self._safe_log_request(
            request_id=request_id,
            method=method or "",
            params=params,
            raw_request=payload,
            upstream_id=upstream.id,
            tool_name=tool_name,
            client_id=client_id,
            cache_key=cache_key,
        )
        self._logger.info(
            "mcp_request",
            request_id=str(request_id),
            method=method,
            upstream_id=upstream.id,
            tool_name=tool_name,
            tool_name_requested=requested_tool_name,
            tool_alias=self._tool_alias(upstream.id, tool_name),
            client_id=client_id,
            cache_key=cache_key,
        )

        if method in {"tools/list", "resources/list", "resources/templates/list", "prompts/list"}:
            timer = Timer()
            response_payload, success, upstream_errors = await self._aggregate_list(payload, method)
            latency_ms = timer.elapsed_ms()
            await self._safe_log_response(
                response_id=uuid4(),
                request_id=request_id,
                success=success,
                latency_ms=latency_ms,
                cache_hit=False,
                response=response_payload,
            )
            self._logger.info(
                "mcp_response",
                request_id=str(request_id),
                method=method,
                upstream_id="*",
                tool_name=None,
                cache_hit=False,
                latency_ms=latency_ms,
                success=success,
                upstream_error_count=len(upstream_errors),
                upstream_errors=upstream_errors,
            )
            self._telemetry.record_response(
                method=method,
                success=success,
                cache_hit=False,
                latency_ms=latency_ms,
                upstream_id="*",
                tool_name=None,
            )
            return GatewayResult(
                payload=response_payload,
                success=success,
                cache_hit=False,
                upstream_id="*",
                tool_name=None,
                request_id=request_id,
            )

        denial_reason = self._deny(upstream, tool_name)
        if denial_reason:
            denial_payload = make_error_response(
                payload.get("id"),
                -32001,
                "Blocked by gateway policy: tool not allowed",
                data={
                    "category": "policy_denied",
                    "enforcer": "mcp-gateway",
                    "upstream_id": upstream.id,
                    "tool_name": tool_name,
                    "policy_type": "deny_tools",
                    "retryable": False,
                    "suggestion": "Use an allowed tool or contact gateway admin to update deny_tools policy.",
                },
            )
            denial_id = uuid4()
            await self._safe_log_denial(denial_id, request_id, upstream.id, tool_name, denial_reason)
            self._logger.info(
                "mcp_denied",
                request_id=str(request_id),
                upstream_id=upstream.id,
                tool_name=tool_name,
                tool_alias=self._tool_alias(upstream.id, tool_name),
                reason=denial_reason,
            )
            self._telemetry.record_denial(upstream.id, tool_name)
            self._telemetry.record_response(
                method=method,
                success=False,
                cache_hit=False,
                latency_ms=0,
                upstream_id=upstream.id,
                tool_name=tool_name,
            )
            return GatewayResult(
                payload=denial_payload,
                success=False,
                cache_hit=False,
                upstream_id=upstream.id,
                tool_name=tool_name,
                request_id=request_id,
            )

        if payload.get("id") is None:
            timer = Timer()
            success = True
            error: Optional[Dict[str, Any]] = None
            try:
                await self._notify_upstream(upstream, payload)
                await self._record_health(upstream.id, method, True)
            except asyncio.TimeoutError:
                await self._record_health(upstream.id, method, False)
                success = False
                error = {"code": -32002, "message": "Upstream timeout"}
            except RuntimeError as exc:
                await self._record_health(upstream.id, method, False)
                success = False
                error = {"code": -32004, "message": str(exc)}
            except Exception as exc:  # noqa: BLE001
                await self._record_health(upstream.id, method, False)
                success = False
                error = {"code": -32003, "message": f"Upstream error: {exc}"}

            latency_ms = timer.elapsed_ms()
            response_payload: Dict[str, Any] = {"accepted": success}
            await self._safe_log_response(
                response_id=uuid4(),
                request_id=request_id,
                success=success,
                latency_ms=latency_ms,
                cache_hit=False,
                response=response_payload if error is None else {"accepted": False, "error": error},
            )
            self._logger.info(
                "mcp_response",
                request_id=str(request_id),
                method=method,
                upstream_id=upstream.id,
                tool_name=tool_name,
                tool_alias=self._tool_alias(upstream.id, tool_name),
                cache_hit=False,
                latency_ms=latency_ms,
                success=success,
                error=error,
            )
            self._telemetry.record_response(
                method=method,
                success=success,
                cache_hit=False,
                latency_ms=latency_ms,
                upstream_id=upstream.id,
                tool_name=tool_name,
            )
            return GatewayResult(
                payload=response_payload,
                success=success,
                cache_hit=False,
                upstream_id=upstream.id,
                tool_name=tool_name,
                request_id=request_id,
            )

        timer = Timer()
        cache_hit = False

        if cache_key:
            cached = await self._memory_cache.get(cache_key)
            if cached is None:
                cached = await self._safe_cache_get(cache_key, request_id)
            if cached is not None:
                cache_hit = True
                response_payload = cached
                await self._safe_log_response(
                    response_id=uuid4(),
                    request_id=request_id,
                    success=True,
                    latency_ms=timer.elapsed_ms(),
                    cache_hit=True,
                    response=response_payload,
                )
                self._logger.info(
                    "mcp_response",
                    request_id=str(request_id),
                    method=method,
                    upstream_id=upstream.id,
                    tool_name=tool_name,
                    tool_alias=self._tool_alias(upstream.id, tool_name),
                    cache_hit=True,
                    latency_ms=timer.elapsed_ms(),
                )
                self._telemetry.record_response(
                    method=method,
                    success=True,
                    cache_hit=True,
                    latency_ms=timer.elapsed_ms(),
                    upstream_id=upstream.id,
                    tool_name=tool_name,
                )
                return GatewayResult(
                    payload=response_payload,
                    success=True,
                    cache_hit=True,
                    upstream_id=upstream.id,
                    tool_name=tool_name,
                    request_id=request_id,
                )

        try:
            response = await self._call_upstream(upstream, payload)
            success = response.success
            await self._record_health(upstream.id, method, success)
        except asyncio.TimeoutError:
            await self._record_health(upstream.id, method, False)
            response = UpstreamResponse(
                payload=make_error_response(payload.get("id"), -32002, "Upstream timeout"),
                success=False,
            )
            success = False
        except RuntimeError as exc:
            await self._record_health(upstream.id, method, False)
            response = UpstreamResponse(
                payload=make_error_response(payload.get("id"), -32004, str(exc)),
                success=False,
            )
            success = False
        except Exception as exc:  # noqa: BLE001
            await self._record_health(upstream.id, method, False)
            response = UpstreamResponse(
                payload=make_error_response(payload.get("id"), -32003, f"Upstream error: {exc}"),
                success=False,
            )
            success = False

        latency_ms = timer.elapsed_ms()
        await self._safe_log_response(
            response_id=uuid4(),
            request_id=request_id,
            success=success,
            latency_ms=latency_ms,
            cache_hit=cache_hit,
            response=response.payload,
        )

        self._logger.info(
            "mcp_response",
            request_id=str(request_id),
            method=method,
            upstream_id=upstream.id,
            tool_name=tool_name,
            tool_alias=self._tool_alias(upstream.id, tool_name),
            cache_hit=cache_hit,
            latency_ms=latency_ms,
            success=success,
            error=response.payload.get("error"),
        )
        self._telemetry.record_response(
            method=method,
            success=success,
            cache_hit=cache_hit,
            latency_ms=latency_ms,
            upstream_id=upstream.id,
            tool_name=tool_name,
        )

        if cache_key and success:
            ttl_minutes = upstream.cache_ttl_minutes or self._config.cache.default_ttl_minutes
            ttl_seconds = max(1, int(ttl_minutes)) * 60
            await self._memory_cache.set(cache_key, response.payload, ttl_seconds)
            await self._safe_cache_set(cache_key, response.payload, ttl_seconds, request_id)

        return GatewayResult(
            payload=response.payload,
            success=success,
            cache_hit=cache_hit,
            upstream_id=upstream.id,
            tool_name=tool_name,
            request_id=request_id,
        )
