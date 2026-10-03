from __future__ import annotations

import asyncio
import copy
import json

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from mcp_gateway.discovery import CALL_TOOL, INSTRUCTIONS, MAX_RESULT_BYTES, SEARCH_TOOL, ToolDiscoveryIndex
from mcp_gateway.gateway import Gateway
from mcp_gateway.request_context import AuthenticatedPrincipal, RequestContext
from mcp_gateway.server_http import HttpServer
from mcp_gateway.telemetry import GatewayTelemetry
from mcp_gateway.upstreams import UpstreamResponse
from tests.gateway_test_support import RecordingLogger, RecordingStore, _config_with_upstreams, _upstream

READ_TOOL = {
    "name": "read_fixture", "description": "Read a fixture value", "inputSchema": {
        "type": "object", "properties": {"value": {"type": "string"}}, "required": ["value"], "additionalProperties": False,
    }, "annotations": {"readOnlyHint": True}, "icons": [{"src": "data:image/png;base64,unused"}],
}
RESULT = {"content": [{"type": "text", "text": "fixture result"}], "structuredContent": {"id": "application-id"}}


class AuditStore(RecordingStore):
    def __init__(self):
        super().__init__()
        self.requests = []
        self.responses = []
        self.cached = {}

    def is_available(self):
        return False

    async def log_request(self, **kwargs):
        self.requests.append(kwargs)

    async def log_response(self, **kwargs):
        self.responses.append(kwargs)

    async def cache_get(self, cache_key):
        return self.cached.get(cache_key)

    async def cache_set(self, cache_key, response, ttl_seconds):
        self.cached[cache_key] = response


def payload(name=SEARCH_TOOL, arguments=None, request_id=1):
    return {"jsonrpc": "2.0", "id": request_id, "method": "tools/call", "params": {
        "name": name, "arguments": arguments if arguments is not None else {"query": "read fixture"},
    }}


def wrapped(arguments=None, name="read_fixture", request_id=1):
    return payload(CALL_TOOL, {"name": name, "arguments": arguments if arguments is not None else {"value": "same"}}, request_id)


def context(discovery=True, key=None):
    return RequestContext("test-client", AuthenticatedPrincipal("gateway", "shared_bearer", api_key_id=key), discovery)


async def fixture(tools=None, warm=True):
    upstream = _upstream("fixture", deny_tools=["denied_fixture"])
    config = _config_with_upstreams([upstream])
    config.gateway.rate_limit_per_minute = 10000
    config.cache.allowed_tools = ["read_fixture"]
    store, logger = AuditStore(), RecordingLogger()
    gateway = Gateway(config, store, logger, GatewayTelemetry())
    calls = []
    discovered = tools if tools is not None else [READ_TOOL, {**READ_TOOL, "name": "denied_fixture"}]

    async def call(upstream, request):
        calls.append((upstream.id, copy.deepcopy(request)))
        if request["method"] == "initialize":
            result = {"protocolVersion": "2025-11-25", "capabilities": {"tools": {}}, "serverInfo": {"name": "fixture", "version": "1"}}
        elif request["method"] == "tools/list":
            result = {"tools": discovered}
        else:
            result = RESULT
        return UpstreamResponse({"jsonrpc": "2.0", "id": request.get("id"), "result": result}, True)

    async def notify(upstream, request):
        calls.append((upstream.id, copy.deepcopy(request)))

    gateway._call_upstream = call
    gateway._notify_upstream = notify
    if warm:
        await gateway.warmup()
        calls.clear()
    return gateway, config, store, logger, calls


def test_compact_catalog_cold_start_and_single_flight_warmup():
    async def run():
        gateway, config, store, _, calls = await fixture(warm=False)
        try:
            async with TestClient(TestServer(HttpServer(config, gateway, RecordingLogger(), gateway._telemetry).build_app())) as client:
                headers = {"Authorization": "Bearer secret"}
                async with client.post("/mcp", json={"jsonrpc": "2.0", "id": "init", "method": "initialize", "params": {"protocolVersion": "2025-03-26"}}, headers=headers) as response:
                    body = await response.json()
                    assert body["result"]["protocolVersion"] == "2025-03-26"
                    assert body["result"]["instructions"] == INSTRUCTIONS
                    assert "tools" in body["result"]["capabilities"]
                async with client.post("/mcp", json={"jsonrpc": "2.0", "id": 0, "method": "tools/list"}, headers=headers) as response:
                    body = await response.json()
                    assert body["id"] == 0
                    assert {x["name"] for x in body["result"]["tools"]} == {SEARCH_TOOL, CALL_TOOL}
                assert not calls  # Cold compact discovery never downloads the full catalog for the client.
                replies = await asyncio.gather(*(gateway.handle(payload(request_id=i), context()) for i in range(5)))
                assert all(json.loads(x.payload["result"]["content"][0]["text"])["tools"][0]["name"] == "read_fixture" for x in replies)
                assert [x[1]["method"] for x in calls].count("tools/list") == 1
                assert len(store.requests) == len(store.responses) == 7
        finally:
            await gateway.close()
    asyncio.run(run())


def test_both_endpoints_are_available_without_discovery_configuration():
    async def run():
        gateway, config, _, _, calls = await fixture()
        try:
            async with TestClient(TestServer(HttpServer(config, gateway, RecordingLogger(), gateway._telemetry).build_app())) as client:
                p = {"jsonrpc": "2.0", "id": 9, "method": "tools/list"}
                async with client.post("/mcp/full", json=p, headers={"Authorization": "Bearer secret"}) as response:
                    assert (await response.json())["result"]["tools"] == [READ_TOOL]
                async with client.post("/mcp", json=p, headers={"Authorization": "Bearer secret"}) as response:
                    assert response.status == 200
                    assert {tool["name"] for tool in (await response.json())["result"]["tools"]} == {SEARCH_TOOL, CALL_TOOL}
                assert calls == []
        finally:
            await gateway.close()
    asyncio.run(run())


def test_endpoint_modes_preserve_direct_and_wrapped_execution():
    async def run():
        gateway, config, _, _, calls = await fixture()
        try:
            async with TestClient(TestServer(HttpServer(config, gateway, RecordingLogger(), gateway._telemetry).build_app())) as client:
                headers = {"Authorization": "Bearer secret"}
                direct = payload("read_fixture", {"value": "direct"}, 31)
                async with client.post("/mcp", json=direct, headers=headers) as response:
                    body = await response.json()
                    assert body["id"] == 31 and body["error"]["code"] == -32602
                assert calls == []
                async with client.post("/mcp/full", json=direct, headers=headers) as response:
                    body = await response.json()
                    assert body["id"] == 31 and body["result"] == RESULT
                async with client.post("/mcp", json=wrapped({"value": "wrapped"}, request_id=32), headers=headers) as response:
                    body = await response.json()
                    assert body["id"] == 32 and body["result"] == RESULT
                assert [call[1]["params"]["name"] for call in calls] == ["read_fixture", "read_fixture"]
        finally:
            await gateway.close()
    asyncio.run(run())


@pytest.mark.parametrize("method", ["GET", "POST", "DELETE", "OPTIONS"])
def test_removed_discovery_endpoint_returns_404(method):
    async def run():
        gateway, config, _, _, calls = await fixture()
        try:
            async with TestClient(TestServer(HttpServer(config, gateway, RecordingLogger(), gateway._telemetry).build_app())) as client:
                async with client.request(method, "/mcp/discovery", json=wrapped(), headers={"Authorization": "Bearer secret"}) as response:
                    assert response.status == 404
                assert calls == []
        finally:
            await gateway.close()
    asyncio.run(run())


def test_discovery_http_security_cors_rate_limits_and_notifications():
    async def run():
        gateway, config, _, _, calls = await fixture()
        config.gateway.allowed_origins = ["https://client.example"]
        config.gateway.rate_limit_per_minute = 4
        try:
            async with TestClient(TestServer(HttpServer(config, gateway, RecordingLogger(), gateway._telemetry).build_app())) as client:
                async with client.post("/mcp", json=wrapped()) as response:
                    assert response.status == 401
                async with client.post("/mcp", json=wrapped(), headers={"Authorization": "Bearer secret", "Origin": "https://evil.example"}) as response:
                    assert response.status == 403
                headers = {"Authorization": "Bearer secret", "Origin": "https://client.example"}
                async with client.options("/mcp", headers=headers) as response:
                    assert response.status == 204 and response.headers["Access-Control-Allow-Origin"] == "https://client.example"
                async with client.post("/mcp", data="not json", headers=headers) as response:
                    assert response.status == 400 and response.headers["Access-Control-Allow-Origin"] == "https://client.example"
                notification = {"jsonrpc": "2.0", "method": "notifications/initialized"}
                async with client.post("/mcp", json=notification, headers=headers) as response:
                    assert response.status == 202
                async with client.post("/mcp", json=payload(), headers=headers) as response:
                    assert response.status == 200
                async with client.post("/mcp", json=payload(), headers=headers) as response:
                    assert response.status == 429
                assert all(x[1]["method"] != "tools/call" for x in calls)
        finally:
            await gateway.close()
    asyncio.run(run())


def test_wrapped_execution_shares_native_cache_ids_metadata_and_redacted_audit():
    async def run():
        gateway, config, store, logger, calls = await fixture()
        config.logging.store_request_bodies = True
        config.logging.store_response_bodies = True
        try:
            direct = payload("read_fixture", {"value": "same"}, 99)
            direct["params"]["_meta"] = {"progressToken": 1}
            first = await gateway.handle(direct, context(discovery=False))
            request = wrapped(request_id=0)
            request["params"]["_meta"] = {"progressToken": 2}
            second = await gateway.handle(request, context())
            assert first.payload["id"] == 99 and second.payload["id"] == 0
            assert first.payload["result"] == second.payload["result"] == RESULT
            assert second.cache_hit and len(calls) == 1
            assert len(store.requests) == len(store.responses) == 2
            assert store.requests[1]["tool_name"] == "read_fixture"
            assert store.requests[1]["raw_request"]["params"]["name"] == CALL_TOOL
            assert store.requests[0]["cache_key"] == store.requests[1]["cache_key"]
            assert [fields for event, fields in logger.infos if event == "mcp_request"][-1]["tool_discovery"]
            other_key = await gateway.handle(wrapped(request_id="other-key"), context(key="different"))
            assert not other_key.cache_hit and len(calls) == 2
            fresh = wrapped({"value": "fresh"}, name="fixture___read_fixture", request_id="alias")
            fresh["params"]["_meta"] = {"progressToken": "retained"}
            await gateway.handle(fresh, context())
            assert calls[-1][1]["params"]["name"] == "read_fixture"
            assert calls[-1][1]["params"]["_meta"] == {"progressToken": "retained"}
        finally:
            await gateway.close()
    asyncio.run(run())


@pytest.mark.parametrize("rpc_payload", [
    wrapped(name=SEARCH_TOOL), wrapped(name="fixture___" + CALL_TOOL), wrapped(name="unknown_tool"),
    wrapped({"value": 42}), wrapped({}), wrapped({"value": "same", "extra": "not allowed"}),
    payload("read_fixture", {"value": "bypass"}), payload(CALL_TOOL, {"name": "read_fixture"}),
    payload(SEARCH_TOOL, {"query": ""}), payload(SEARCH_TOOL, {"query": "x" * 513}),
    payload(SEARCH_TOOL, {"query": "read", "limit": True}), payload(SEARCH_TOOL, {"query": "read", "limit": 11}),
    payload(SEARCH_TOOL, {"query": "read", "upstream": "missing"}), payload(SEARCH_TOOL, {"query": "read", "upstream": None}),
    payload(SEARCH_TOOL, {"query": "read", "extra": 1}),
])
def test_invalid_compact_requests_are_audited_without_upstream_execution(rpc_payload):
    async def run():
        gateway, _, store, _, calls = await fixture()
        try:
            response = await gateway.handle(rpc_payload, context())
            assert response.payload["error"]["code"] == -32602
            assert calls == [] and len(store.requests) == len(store.responses) == 1
        finally:
            await gateway.close()
    asyncio.run(run())


def test_denied_tools_are_hidden_and_wrapper_cannot_bypass_policy_or_auth():
    async def run():
        gateway, _, store, logger, calls = await fixture()
        try:
            search = await gateway.handle(payload(arguments={"query": "denied_fixture"}), context())
            assert "denied_fixture" not in search.payload["result"]["content"][0]["text"]
            denied = await gateway.handle(wrapped(name="fixture___denied_fixture"), context())
            assert denied.payload["error"]["code"] == -32001
            assert any(event == "mcp_denied" for event, _ in logger.infos)
            unauthorized = await gateway.handle(wrapped(), RequestContext("test-client", tool_discovery=True))
            assert unauthorized.payload["error"]["code"] == -32010
            # Policy denials use the existing denial audit rather than a response row.
            assert calls == [] and len(store.requests) == 3 and len(store.responses) == 2
        finally:
            await gateway.close()
    asyncio.run(run())


def test_registry_refresh_uses_a_coherent_snapshot_for_in_flight_calls():
    async def run():
        gateway, _, store, _, calls = await fixture()
        new_upstream = _upstream("replacement")
        gateway._upstream_by_id[new_upstream.id] = new_upstream
        waiting, resume = asyncio.Event(), asyncio.Event()
        original_log = store.log_request

        async def paused_log(**kwargs):
            await original_log(**kwargs)
            if kwargs["tool_name"] == "read_fixture":
                waiting.set()
                await resume.wait()

        store.log_request = paused_log
        try:
            pending = asyncio.create_task(gateway.handle(wrapped(), context()))
            await waiting.wait()
            new_tool = copy.deepcopy(READ_TOOL)
            new_tool["inputSchema"]["properties"]["value"] = {"type": "integer"}
            await gateway._apply_tool_registry_state(gateway._build_tool_registry_state([(new_upstream, [new_tool])]))
            assert gateway._tool_registry["read_fixture"] == "replacement"
            resume.set()
            response = await pending
            assert response.success and calls[-1][0] == "fixture"  # Original schema and original route stay together.
            store.log_request = original_log
            assert (await gateway.handle(wrapped({"value": "stale"}), context())).payload["error"]["code"] == -32602
            response = await gateway.handle(wrapped({"value": 1}), context())
            assert response.success and calls[-1][0] == "replacement"
            await gateway._apply_tool_registry_state(gateway._build_tool_registry_state([(new_upstream, [])]))
            assert (await gateway.handle(wrapped(), context())).payload["error"]["code"] == -32602
        finally:
            resume.set()
            await gateway.close()
    asyncio.run(run())


def test_reserved_name_collision_preserves_previous_registry():
    async def run():
        gateway, config, _, _, _ = await fixture()
        try:
            previous = gateway._discovery_index
            state = gateway._build_tool_registry_state([(config.upstreams[0], [{**READ_TOOL, "name": CALL_TOOL}])])
            with pytest.raises(ValueError, match="reserved"):
                await gateway._apply_tool_registry_state(state)
            assert gateway._tool_registry == {"read_fixture": "fixture", "denied_fixture": "fixture"}
            assert gateway._discovery_index is previous
        finally:
            await gateway.close()
    asyncio.run(run())


def test_search_is_bounded_preserves_schema_hints_and_filters_upstreams():
    tools = [READ_TOOL, {**READ_TOOL, "name": "other_read"}, {
        "name": "pr_read", "description": "Read pull request details", "inputSchema": {
            "type": "object", "properties": {"method": {"enum": ["get_diff", "get_comments"]}},
        },
    }]
    index = ToolDiscoveryIndex(tools, {"read_fixture": "fixture", "other_read": "other", "pr_read": "github"}, ["fixture", "other", "github"])
    result = json.loads(index.search({"query": "read", "upstream": "fixture"}))
    assert [x["name"] for x in result["tools"]] == ["read_fixture"]
    assert result["tools"][0]["inputSchema"] == READ_TOOL["inputSchema"]
    assert result["tools"][0]["annotations"] == READ_TOOL["annotations"]
    assert "icons" not in result["tools"][0]
    assert json.loads(index.search({"query": "PR diff", "limit": 1}))["tools"][0]["name"] == "pr_read"
    assert json.loads(index.search({"query": "zzzznonexistent"}))["tools"] == []
    huge = copy.deepcopy(READ_TOOL)
    huge["inputSchema"]["description"] = "x" * MAX_RESULT_BYTES
    bounded = ToolDiscoveryIndex([huge], {"read_fixture": "fixture"}, ["fixture"]).search({"query": "read_fixture"})
    assert len(bounded.encode()) <= MAX_RESULT_BYTES
    assert json.loads(bounded)["tools"] == [] and json.loads(bounded)["omitted_count"] == 1


def test_local_schema_refs_work_and_remote_refs_do_not_fetch():
    async def run():
        fetched = []

        async def remote(request):
            fetched.append(request.path)
            return web.json_response({"type": "string"})

        app = web.Application()
        app.router.add_get("/schema", remote)
        async with TestServer(app) as remote_server:
            local_tool = copy.deepcopy(READ_TOOL)
            local_tool["inputSchema"] = {
                "type": "object", "properties": {"value": {"$ref": "#/$defs/value"}},
                "$defs": {"value": {"type": "string"}}, "required": ["value"],
            }
            index = ToolDiscoveryIndex([local_tool], {"read_fixture": "fixture"}, ["fixture"])
            index.validate_arguments("read_fixture", {"value": "works"})
            remote_tool = copy.deepcopy(local_tool)
            remote_tool["inputSchema"]["properties"]["value"] = {"$ref": str(remote_server.make_url("/schema"))}
            index = ToolDiscoveryIndex([remote_tool], {"read_fixture": "fixture"}, ["fixture"])
            with pytest.raises(ValueError, match="cannot be validated"):
                index.validate_arguments("read_fixture", {"value": "never send this"})
            assert fetched == []
    asyncio.run(run())


@pytest.mark.parametrize("schema", [None, {"properties": None}, {"enum": 1}])
def test_invalid_upstream_schemas_do_not_break_index_publication(schema):
    index = ToolDiscoveryIndex([{**READ_TOOL, "inputSchema": schema}], {"read_fixture": "fixture"}, ["fixture"])
    assert json.loads(index.search({"query": "read_fixture"}))["tools"][0]["inputSchema"] == schema
    with pytest.raises(ValueError, match="cannot be validated"):
        index.validate_arguments("read_fixture", {"value": "private argument"})
