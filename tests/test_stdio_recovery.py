from __future__ import annotations

import asyncio
import json
import sys

import pytest

from mcp_gateway.gateway import Gateway
from mcp_gateway.logging import Logger
from mcp_gateway.postgres import PostgresStore
from mcp_gateway.telemetry import GatewayTelemetry
from tests.gateway_test_support import _config_with_upstreams, _upstream
from tests.test_upstreams import FIXTURE_STDIO_UPSTREAM


def _gateway(journal):
    upstream = _upstream("stdio-fixture")
    upstream.transport = "stdio"
    upstream.command = [sys.executable, str(FIXTURE_STDIO_UPSTREAM)]
    upstream.env = {"FAKE_STDIO_REQUIRE_INITIALIZED": "1", "FAKE_STDIO_JOURNAL": str(journal)}
    upstream.timeout_ms = 2000
    gateway = Gateway(_config_with_upstreams([upstream]), PostgresStore(""), Logger(False), GatewayTelemetry())
    return gateway, upstream


def _call(request_id, **arguments):
    return {"jsonrpc": "2.0", "id": request_id, "method": "tools/call",
            "params": {"name": "stdio.echo", "arguments": arguments}}


def _journal(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


@pytest.mark.parametrize("interruption", ["timeout", "cancel", "exit"])
def test_gateway_recovers_initialized_stdio_session_without_replaying_calls(tmp_path, interruption):
    journal = tmp_path / "requests.jsonl"
    gateway, upstream = _gateway(journal)

    async def run():
        await gateway.warmup()
        client = await gateway._get_upstream_client(upstream)
        try:
            assert gateway.is_ready()
            if interruption == "exit":
                client._process.terminate()
                await client._process.wait()
            else:
                client._timeout = 0.1 if interruption == "timeout" else 2
                operation = asyncio.create_task(gateway._execute_upstream_operation(
                    upstream, "tools/call", _call("interrupted", value="slow", delay_seconds=5)))
                if interruption == "cancel":
                    async def wait_for_request():
                        while not any(row["request"].get("id") == "interrupted" for row in _journal(journal)):
                            await asyncio.sleep(0.01)
                    await asyncio.wait_for(wait_for_request(), 2)
                    operation.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await operation
                else:
                    failed = await operation
                    assert failed.error["code"] == -32002
                assert not client._initialized
                # Keep routing traffic so the next request can restore the session.
                assert gateway.is_ready()
            client._timeout = 2
            responses = await asyncio.gather(*[
                gateway._execute_upstream_operation(upstream, "tools/call", _call(name, value=name))
                for name in ("first", "second")
            ])
            assert all(response.success for response in responses)
            assert [response.payload["result"]["content"][0]["text"] for response in responses] == [
                "stdio.echo:first", "stdio.echo:second"]
            assert gateway.is_ready()
        finally:
            await client.close()

    asyncio.run(run())
    rows = _journal(journal)
    starts = [row for row in rows if row["request"]["method"] == "initialize"]
    assert len(starts) == 2
    assert starts[0]["request"] == starts[1]["request"]
    assert starts[0]["pid"] != starts[1]["pid"]
    replacement = [row["request"] for row in rows if row["pid"] == starts[1]["pid"]]
    assert [request["method"] for request in replacement] == [
        "initialize", "notifications/initialized", "tools/call", "tools/call"]
    assert sum(row["request"].get("id") == "interrupted" for row in rows) == (interruption != "exit")


def test_failed_reinitialization_blocks_new_call_and_later_recovers(tmp_path):
    journal = tmp_path / "requests.jsonl"
    gateway, upstream = _gateway(journal)

    async def run():
        await gateway.warmup()
        client = await gateway._get_upstream_client(upstream)
        try:
            await client.close()
            client._env["FAKE_STDIO_REJECT_INITIALIZE"] = "1"
            response = await gateway._execute_upstream_operation(upstream, "tools/call", _call("blocked"))
            assert not response.success
            assert not client._initialized
            assert client._process is None
            del client._env["FAKE_STDIO_REJECT_INITIALIZE"]
            response = await gateway._execute_upstream_operation(upstream, "tools/call", _call("recovered"))
            assert response.success
            assert gateway.is_ready()
        finally:
            await client.close()

    asyncio.run(run())
    assert not any(row["request"].get("id") == "blocked" for row in _journal(journal))


def test_notification_reinitializes_replacement_without_duplicate_initialized(tmp_path):
    journal = tmp_path / "requests.jsonl"
    gateway, upstream = _gateway(journal)

    async def run():
        await gateway.warmup()
        client = await gateway._get_upstream_client(upstream)
        try:
            await client.close()
            await client.notify({"jsonrpc": "2.0", "method": "notifications/initialized"})
            assert (await client.call(_call("after-notification"))).success
            assert gateway.is_ready()
        finally:
            await client.close()

    asyncio.run(run())
    rows = _journal(journal)
    pid = rows[-1]["pid"]
    assert [row["request"]["method"] for row in rows if row["pid"] == pid] == [
        "initialize", "notifications/initialized", "tools/call"]
