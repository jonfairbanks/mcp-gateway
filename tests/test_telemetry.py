from __future__ import annotations

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from mcp_gateway.telemetry import GatewayTelemetry


def test_prometheus_metrics_drop_tool_name_labels_but_keep_upstream_counts() -> None:
    telemetry = GatewayTelemetry()

    telemetry.record_response(
        method="tools/call",
        success=True,
        cache_hit=False,
        latency_ms=12,
        upstream_id="context7",
        tool_name="query-docs",
    )
    telemetry.record_response(
        method="tools/call",
        success=False,
        cache_hit=True,
        latency_ms=3,
        upstream_id="grafana",
        tool_name="query_loki_logs",
    )
    telemetry.record_denial("context7", "query-docs")

    rendered = telemetry.render_prometheus().decode("utf-8")

    assert 'mcp_gateway_responses_total{cache_hit="false",method="tools/call",success="true",upstream_id="context7"} 1.0' in rendered
    assert 'mcp_gateway_tool_calls_total{cache_hit="false",success="true",upstream_id="context7"} 1.0' in rendered
    assert 'mcp_gateway_tool_calls_total{cache_hit="true",success="false",upstream_id="grafana"} 1.0' in rendered
    assert 'mcp_gateway_denials_total{upstream_id="context7"} 1.0' in rendered
    assert 'tool_name=' not in rendered


def test_tracing_stays_disabled_without_otel_env(monkeypatch) -> None:
    monkeypatch.delenv("OTEL_TRACES_EXPORTER", raising=False)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", raising=False)

    telemetry = GatewayTelemetry()

    assert telemetry.tracing_enabled is False


def test_inject_context_adds_traceparent_for_active_span() -> None:
    telemetry = GatewayTelemetry(tracer_provider=TracerProvider())
    carrier: dict[str, str] = {}

    with telemetry.start_mcp_span("tools/call", "req-123", client_id="client-1", auth_subject=None):
        telemetry.inject_context(carrier)

    assert "traceparent" in carrier


def test_extract_context_round_trips_traceparent() -> None:
    propagator = TraceContextTextMapPropagator()
    carrier = {
        "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
    }

    context = GatewayTelemetry(tracer_provider=TracerProvider()).extract_context(carrier)
    extracted = propagator.extract(carrier=carrier, context=context)

    assert extracted is not None
