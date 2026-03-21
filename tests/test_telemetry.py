from __future__ import annotations

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
