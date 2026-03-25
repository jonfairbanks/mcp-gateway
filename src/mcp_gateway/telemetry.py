from __future__ import annotations

import os
from typing import Optional

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import extract, inject
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace import SpanKind, Status, StatusCode
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Counter, Histogram, generate_latest

from .__init__ import __version__

_CONFIGURED_TRACER_PROVIDER: Optional[TracerProvider] = None


class GatewayTelemetry:
    def __init__(self, tracer_provider: Optional[TracerProvider] = None, *, enabled: bool = False) -> None:
        self._prom_registry = CollectorRegistry()
        self._owns_tracer_provider = False
        self._tracer_provider = tracer_provider
        self._enabled = enabled
        self._prom_requests_total = Counter(
            "mcp_gateway_requests_total",
            "Total MCP requests received by method.",
            ["method"],
            registry=self._prom_registry,
        )
        self._prom_responses_total = Counter(
            "mcp_gateway_responses_total",
            "Total MCP responses emitted.",
            ["method", "success", "cache_hit", "upstream_id"],
            registry=self._prom_registry,
        )
        self._prom_response_latency_ms = Histogram(
            "mcp_gateway_response_latency_ms",
            "MCP response latency in milliseconds.",
            ["method", "success", "cache_hit", "upstream_id"],
            registry=self._prom_registry,
        )
        self._prom_denials_total = Counter(
            "mcp_gateway_denials_total",
            "Total denied tool calls.",
            ["upstream_id"],
            registry=self._prom_registry,
        )
        self._prom_tool_calls_total = Counter(
            "mcp_gateway_tool_calls_total",
            "Total tool call outcomes by integration.",
            ["upstream_id", "success", "cache_hit"],
            registry=self._prom_registry,
        )
        self._prom_upstream_calls_total = Counter(
            "mcp_gateway_upstream_calls_total",
            "Total upstream method outcomes.",
            ["upstream_id", "method", "success"],
            registry=self._prom_registry,
        )
        self._configure_tracing()
        self._tracer = (self._tracer_provider or trace.get_tracer_provider()).get_tracer("mcp_gateway", __version__)

    def _configure_tracing(self) -> None:
        global _CONFIGURED_TRACER_PROVIDER

        if self._tracer_provider is not None:
            return
        if not self._enabled:
            return

        exporter = self._resolve_trace_exporter()
        if exporter is None:
            return

        if _CONFIGURED_TRACER_PROVIDER is None:
            provider = TracerProvider(
                resource=Resource.create(
                    {
                        SERVICE_NAME: os.getenv("OTEL_SERVICE_NAME", "mcp-gateway"),
                        "service.version": __version__,
                    }
                )
            )
            if exporter == "console":
                span_exporter = ConsoleSpanExporter()
            else:
                span_exporter = OTLPSpanExporter()
            provider.add_span_processor(BatchSpanProcessor(span_exporter))
            trace.set_tracer_provider(provider)
            _CONFIGURED_TRACER_PROVIDER = provider
            self._owns_tracer_provider = True

        self._tracer_provider = _CONFIGURED_TRACER_PROVIDER

    @staticmethod
    def _resolve_trace_exporter() -> Optional[str]:
        exporters = [item.strip().lower() for item in os.getenv("OTEL_TRACES_EXPORTER", "").split(",") if item.strip()]
        if "otlp" in exporters:
            return "otlp"
        if "console" in exporters:
            return "console"
        if os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT") or os.getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"):
            return "otlp"
        return None

    @property
    def tracing_enabled(self) -> bool:
        return self._tracer_provider is not None

    def extract_context(self, carrier) -> object:
        return extract(carrier)

    def inject_context(self, carrier: dict[str, str]) -> None:
        inject(carrier)

    def start_http_server_span(self, method: str, path: str, *, context: object = None):
        attributes = {
            "http.request.method": method,
            "url.path": path,
        }
        return self._tracer.start_as_current_span(
            f"HTTP {method} {path}",
            context=context,
            kind=SpanKind.SERVER,
            attributes=attributes,
        )

    def start_mcp_span(self, method: str, request_id: str, *, client_id: str, auth_subject: Optional[str]):
        attributes = {
            "mcp.method": method,
            "mcp.request_id": request_id,
            "mcp.client_id": client_id,
        }
        if auth_subject:
            attributes["enduser.id"] = auth_subject
        return self._tracer.start_as_current_span(f"mcp.{method}", kind=SpanKind.INTERNAL, attributes=attributes)

    def start_upstream_span(self, upstream_id: str, transport: str, method: str, *, notification: bool):
        return self._tracer.start_as_current_span(
            f"upstream.{upstream_id}.{method}",
            kind=SpanKind.CLIENT,
            attributes={
                "mcp.upstream.id": upstream_id,
                "mcp.upstream.transport": transport,
                "mcp.method": method,
                "mcp.notification": notification,
            },
        )

    def annotate_http_response(self, status_code: int) -> None:
        span = trace.get_current_span()
        if not span.is_recording():
            return
        span.set_attribute("http.response.status_code", status_code)
        if status_code >= 500:
            span.set_status(Status(StatusCode.ERROR))
        else:
            span.set_status(Status(StatusCode.OK))

    def annotate_exception(self, exc: BaseException) -> None:
        span = trace.get_current_span()
        if not span.is_recording():
            return
        span.record_exception(exc)
        span.set_status(Status(StatusCode.ERROR, str(exc) or type(exc).__name__))

    def annotate_mcp_result(
        self,
        *,
        request_id: str,
        method: str,
        success: bool,
        cache_hit: bool,
        latency_ms: int,
        upstream_id: Optional[str],
        tool_name: Optional[str],
        error: Optional[dict[str, object]] = None,
    ) -> None:
        span = trace.get_current_span()
        if not span.is_recording():
            return
        span.set_attribute("mcp.request_id", request_id)
        span.set_attribute("mcp.method", method)
        span.set_attribute("mcp.success", success)
        span.set_attribute("mcp.cache_hit", cache_hit)
        span.set_attribute("mcp.latency_ms", latency_ms)
        if upstream_id is not None:
            span.set_attribute("mcp.upstream.id", upstream_id)
        if tool_name is not None:
            span.set_attribute("mcp.tool_name", tool_name)
        if error:
            message = error.get("message")
            span.set_status(Status(StatusCode.ERROR, str(message) if message is not None else "MCP request failed"))
        else:
            span.set_status(Status(StatusCode.OK))

    def annotate_upstream_result(
        self,
        *,
        success: bool,
        error: Optional[dict[str, object]] = None,
        expected_unsupported: bool = False,
    ) -> None:
        span = trace.get_current_span()
        if not span.is_recording():
            return
        span.set_attribute("mcp.success", success)
        if expected_unsupported:
            span.set_attribute("mcp.expected_unsupported", True)
            span.set_status(Status(StatusCode.OK))
            return
        if error:
            message = error.get("message")
            span.set_status(Status(StatusCode.ERROR, str(message) if message is not None else "Upstream request failed"))
        else:
            span.set_status(Status(StatusCode.OK))

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
        }
        self._prom_responses_total.labels(**attrs).inc()
        self._prom_response_latency_ms.labels(**attrs).observe(max(0, latency_ms))
        if method == "tools/call":
            self._prom_tool_calls_total.labels(
                upstream_id=upstream_id or "none",
                success=str(success).lower(),
                cache_hit=str(cache_hit).lower(),
            ).inc()

    def record_denial(self, upstream_id: Optional[str], tool_name: Optional[str]) -> None:
        self._prom_denials_total.labels(upstream_id=upstream_id or "none").inc()

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
        if self._owns_tracer_provider and self._tracer_provider is not None:
            self._tracer_provider.shutdown()
        return
