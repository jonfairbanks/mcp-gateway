from __future__ import annotations

import asyncio
import math
import time
from typing import Any, Dict, Optional

from .config import READINESS_MODE_REQUIRED, READINESS_MODE_THRESHOLD, AppConfig, UpstreamConfig


class GatewayRuntimeState:
    def __init__(self, config: AppConfig, upstream_by_id: Dict[str, UpstreamConfig]) -> None:
        self._config = config
        self._upstream_by_id = upstream_by_id
        self._health_counters: Dict[str, Dict[str, Dict[str, int]]] = {}
        self._health_lock = asyncio.Lock()
        self._breaker_lock = asyncio.Lock()
        self._upstream_breakers: Dict[str, Dict[str, float]] = {}
        self._global_breaker: Dict[str, float] = {"consecutive_failures": 0, "open_until": 0.0}
        self._warmup_status: Dict[str, Dict[str, Any]] = {}

    @property
    def warmup_status(self) -> Dict[str, Dict[str, Any]]:
        return self._warmup_status

    @warmup_status.setter
    def warmup_status(self, value: Dict[str, Dict[str, Any]]) -> None:
        self._warmup_status = value

    @property
    def upstream_breakers(self) -> Dict[str, Dict[str, float]]:
        return self._upstream_breakers

    @upstream_breakers.setter
    def upstream_breakers(self, value: Dict[str, Dict[str, float]]) -> None:
        self._upstream_breakers = value

    @property
    def global_breaker(self) -> Dict[str, float]:
        return self._global_breaker

    @global_breaker.setter
    def global_breaker(self, value: Dict[str, float]) -> None:
        self._global_breaker = value

    async def record_health(self, upstream_id: str, method: str, success: bool) -> None:
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
        healthy_upstream_ids = {
            upstream_id
            for upstream_id, status in self._warmup_status.items()
            if status.get("initialize_success") and status.get("tools_list_success")
        }
        total_upstreams = len(self._config.upstreams)
        readiness_mode = self._config.gateway.readiness_mode

        if readiness_mode == READINESS_MODE_REQUIRED:
            required = set(self._config.gateway.required_ready_upstreams)
            return bool(required) and required.issubset(healthy_upstream_ids)
        if readiness_mode == READINESS_MODE_THRESHOLD:
            healthy_count = len(healthy_upstream_ids)
            minimum_upstreams = self._config.gateway.readiness_min_healthy_upstreams
            if minimum_upstreams is not None and healthy_count < minimum_upstreams:
                return False
            minimum_percent = self._config.gateway.readiness_min_healthy_percent
            if minimum_percent is not None:
                required_count = math.ceil((minimum_percent / 100) * total_upstreams) if total_upstreams > 0 else 0
                if healthy_count < required_count:
                    return False
            return healthy_count > 0
        return bool(healthy_upstream_ids)

    def is_global_breaker_open(self) -> bool:
        return time.monotonic() < float(self._global_breaker.get("open_until", 0.0))

    def is_upstream_breaker_open(self, upstream_id: str) -> bool:
        state = self._upstream_breakers.get(upstream_id)
        if not state:
            return False
        return time.monotonic() < float(state.get("open_until", 0.0))

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
