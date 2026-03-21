from __future__ import annotations

import asyncio

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from mcp_gateway.postgres import PostgresStore


def test_postgres_store_emits_span_for_fast_return_without_database() -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    store = PostgresStore("")
    store._tracer = provider.get_tracer("test.postgres")

    result = asyncio.run(store.cache_get("missing"))

    assert result is None
    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "postgres.cache_get"
    assert spans[0].attributes["db.system"] == "postgresql"
    assert spans[0].attributes["db.operation.name"] == "cache_get"
    assert spans[0].status.status_code == StatusCode.OK


def test_postgres_store_records_error_status_when_database_is_required() -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    store = PostgresStore("")
    store._tracer = provider.get_tracer("test.postgres")

    try:
        asyncio.run(store.consume_rate_limit(scope_key="client:test", limit=1))
    except RuntimeError as exc:
        assert str(exc) == "Postgres is not available"
    else:
        raise AssertionError("expected consume_rate_limit to require Postgres")

    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].name == "postgres.consume_rate_limit"
    assert spans[0].status.status_code == StatusCode.ERROR
