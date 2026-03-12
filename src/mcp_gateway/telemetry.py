from __future__ import annotations

from typing import Optional

from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Counter, Histogram, generate_latest


class GatewayTelemetry:
    def __init__(self) -> None:
        self._prom_registry = CollectorRegistry()
        self._prom_requests_total = Counter(
            "mcp_gateway_requests_total",
            "Total MCP requests received by method.",
            ["method"],
            registry=self._prom_registry,
        )
        self._prom_responses_total = Counter(
            "mcp_gateway_responses_total",
            "Total MCP responses emitted.",
            ["method", "success", "cache_hit", "upstream_id", "tool_name"],
            registry=self._prom_registry,
        )
        self._prom_response_latency_ms = Histogram(
            "mcp_gateway_response_latency_ms",
            "MCP response latency in milliseconds.",
            ["method", "success", "cache_hit", "upstream_id", "tool_name"],
            registry=self._prom_registry,
        )
        self._prom_denials_total = Counter(
            "mcp_gateway_denials_total",
            "Total denied tool calls.",
            ["upstream_id", "tool_name"],
            registry=self._prom_registry,
        )
        self._prom_upstream_calls_total = Counter(
            "mcp_gateway_upstream_calls_total",
            "Total upstream method outcomes.",
            ["upstream_id", "method", "success"],
            registry=self._prom_registry,
        )

    def record_request(self, method: str) -> None:
        self._prom_requests_total.labels(method=method).inc()

    def record_response(
        self,
        method: str,
        success: bool,
        cache_hit: bool,
        latency_ms: int,
        upstream_id: Optional[str],
        tool_name: Optional[str],
    ) -> None:
        attrs = {
            "method": method,
            "success": str(success).lower(),
            "cache_hit": str(cache_hit).lower(),
            "upstream_id": upstream_id or "none",
            "tool_name": tool_name or "none",
        }
        self._prom_responses_total.labels(**attrs).inc()
        self._prom_response_latency_ms.labels(**attrs).observe(max(0, latency_ms))

    def record_denial(self, upstream_id: Optional[str], tool_name: Optional[str]) -> None:
        attrs = {
            "upstream_id": upstream_id or "none",
            "tool_name": tool_name or "none",
        }
        self._prom_denials_total.labels(**attrs).inc()

    def record_upstream_outcome(self, upstream_id: str, method: str, success: bool) -> None:
        attrs = {
            "upstream_id": upstream_id,
            "method": method,
            "success": str(success).lower(),
        }
        self._prom_upstream_calls_total.labels(**attrs).inc()

    def render_prometheus(self) -> bytes:
        return generate_latest(self._prom_registry)

    @property
    def prometheus_content_type(self) -> str:
        return CONTENT_TYPE_LATEST

    async def close(self) -> None:
        return
