from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from mcp_gateway.config import AppConfig, CacheConfig, GatewayConfig, LoggingConfig, UpstreamConfig
from mcp_gateway.gateway import Gateway
from mcp_gateway.logging import Logger
from mcp_gateway.postgres import PostgresStore
from mcp_gateway.protocol import CURRENT_PROTOCOL_VERSION
from mcp_gateway.server_http import HttpServer
from mcp_gateway.telemetry import GatewayTelemetry

FIXTURE_STDIO_UPSTREAM = Path(__file__).resolve().parent / "fixtures" / "fake_stdio_upstream.py"
SCHEMA_SQL = Path(__file__).resolve().parents[1] / "schema.sql"
TEST_DATABASE_DSN_ENV = "MCP_GATEWAY_TEST_DATABASE_URL"

pytestmark = pytest.mark.skipif(
    not os.getenv(TEST_DATABASE_DSN_ENV),
    reason=f"set {TEST_DATABASE_DSN_ENV} to run Postgres integration tests",
)


def _gateway_config(http_endpoint: str) -> AppConfig:
    return AppConfig(
        gateway=GatewayConfig(
            listen_host="127.0.0.1",
            listen_port=0,
            auth_mode="single_shared",
            api_key="phase-one-secret",
            bootstrap_admin_api_key="",
            allow_unauthenticated=False,
            public_tools_catalog=False,
            trusted_proxies=["127.0.0.1", "::1"],
            request_max_bytes=2 * 1024 * 1024,
            rate_limit_per_minute=120,
            circuit_breaker_fail_threshold=10,
            circuit_breaker_open_seconds=30,
        ),
        logging=LoggingConfig(stdout_json=False, extra_redact_fields=[]),
        cache=CacheConfig(enabled=True, max_entries=100, default_ttl_minutes=60, client_scoped_tools=[]),
        upstreams=[
            UpstreamConfig(
                id="http-fixture",
                name="http-fixture",
                transport="streamable_http",
                endpoint=http_endpoint,
                http_headers={},
                bearer_token_env_var=None,
                http_serialize_requests=False,
                command=None,
                env={},
                cwd=None,
                timeout_ms=2000,
                stdio_read_limit_bytes=1024 * 1024,
                max_in_flight=10,
                deny_tools=[],
                cache_ttl_minutes=None,
                circuit_breaker_fail_threshold=None,
                circuit_breaker_open_seconds=None,
                tool_routes=[],
            ),
            UpstreamConfig(
                id="stdio-fixture",
                name="stdio-fixture",
                transport="stdio",
                endpoint=None,
                http_headers={},
                bearer_token_env_var=None,
                http_serialize_requests=False,
                command=[sys.executable, str(FIXTURE_STDIO_UPSTREAM)],
                env={"FAKE_STDIO_TOOL_NAME": "stdio.echo"},
                cwd=None,
                timeout_ms=2000,
                stdio_read_limit_bytes=1024 * 1024,
                max_in_flight=10,
                deny_tools=[],
                cache_ttl_minutes=None,
                circuit_breaker_fail_threshold=None,
                circuit_breaker_open_seconds=None,
                tool_routes=[],
            ),
        ],
    )


async def _prepare_database(store: PostgresStore) -> None:
    assert store._pool is not None
    async with store._pool.connection() as conn:
        await conn.execute(SCHEMA_SQL.read_text(encoding="utf-8"))
        await conn.execute(
            """
            TRUNCATE TABLE
                mcp_responses,
                mcp_denials,
                mcp_requests,
                mcp_cache,
                gateway_rate_limits,
                gateway_api_keys,
                gateway_group_memberships,
                gateway_group_integration_grants,
                gateway_group_platform_grants,
                gateway_groups,
                gateway_users,
                gateway_policy_state
            CASCADE
            """
        )
        await conn.execute(
            """
            INSERT INTO gateway_policy_state (singleton_key, policy_revision)
            VALUES ('default', 0)
            """
        )


async def _logged_tool_call_upstreams(store: PostgresStore) -> list[str]:
    assert store._pool is not None
    async with store._pool.connection() as conn:
        cur = await conn.execute(
            """
            SELECT upstream_id
            FROM mcp_requests
            WHERE method = 'tools/call'
            ORDER BY upstream_id
            """
        )
        rows = await cur.fetchall()
    return [str(row["upstream_id"]) for row in rows]


async def _logged_counts(store: PostgresStore) -> tuple[int, int]:
    assert store._pool is not None
    async with store._pool.connection() as conn:
        requests_cur = await conn.execute("SELECT COUNT(*) AS count FROM mcp_requests")
        responses_cur = await conn.execute("SELECT COUNT(*) AS count FROM mcp_responses")
        request_row = await requests_cur.fetchone()
        response_row = await responses_cur.fetchone()
    return int(request_row["count"]), int(response_row["count"])


def test_gateway_app_integrates_http_and_stdio_upstreams_with_postgres_logging() -> None:
    async def run_test() -> None:
        http_app = web.Application()
        seen_methods: list[str] = []

        async def upstream_handler(request: web.Request) -> web.StreamResponse:
            payload = await request.json()
            method = payload.get("method")
            seen_methods.append(str(method))

            if method == "notifications/initialized":
                return web.Response(status=202)
            if method == "initialize":
                return web.json_response(
                    {
                        "jsonrpc": "2.0",
                        "id": payload.get("id"),
                        "result": {
                            "protocolVersion": payload.get("params", {}).get("protocolVersion", CURRENT_PROTOCOL_VERSION),
                            "capabilities": {"tools": {}},
                            "serverInfo": {"name": "fake-http-upstream", "version": "0.1.0"},
                        },
                    }
                )
            if method == "tools/list":
                return web.json_response(
                    {
                        "jsonrpc": "2.0",
                        "id": payload.get("id"),
                        "result": {
                            "tools": [
                                {
                                    "name": "http.echo",
                                    "description": "Echoes the provided value.",
                                    "inputSchema": {
                                        "type": "object",
                                        "properties": {"value": {"type": "string"}},
                                        "required": ["value"],
                                    },
                                }
                            ]
                        },
                    }
                )
            if method == "tools/call":
                args = (payload.get("params") or {}).get("arguments") or {}
                return web.json_response(
                    {
                        "jsonrpc": "2.0",
                        "id": payload.get("id"),
                        "result": {
                            "content": [{"type": "text", "text": f"http.echo:{args.get('value', '')}"}],
                            "isError": False,
                        },
                    }
                )
            return web.json_response(
                {
                    "jsonrpc": "2.0",
                    "id": payload.get("id"),
                    "error": {"code": -32601, "message": f"Unknown method: {method}"},
                }
            )

        http_app.router.add_post("/mcp", upstream_handler)

        async with TestServer(http_app) as upstream_server:
            store = PostgresStore(os.environ[TEST_DATABASE_DSN_ENV])
            await store.start()
            await _prepare_database(store)

            config = _gateway_config(str(upstream_server.make_url("/mcp")))
            logger = Logger(stdout_json=False)
            telemetry = GatewayTelemetry()
            gateway = Gateway(config, store, logger, telemetry)
            server = HttpServer(config, gateway, logger, telemetry)

            try:
                await gateway.warmup()
                async with TestServer(server.build_app()) as gateway_server:
                    async with TestClient(gateway_server) as client:
                        headers = {"Authorization": "Bearer phase-one-secret"}

                        ready_response = await client.get("/readyz")
                        ready_payload = await ready_response.json()
                        assert ready_response.status == 200
                        assert ready_payload["ready"] is True

                        initialize_response = await client.post(
                            "/mcp",
                            headers=headers,
                            json={
                                "jsonrpc": "2.0",
                                "id": "init-1",
                                "method": "initialize",
                                "params": {
                                    "protocolVersion": CURRENT_PROTOCOL_VERSION,
                                    "capabilities": {},
                                    "clientInfo": {"name": "phase-1-test", "version": "0.1.0"},
                                },
                            },
                        )
                        initialize_payload = await initialize_response.json()
                        assert initialize_response.status == 200
                        assert initialize_payload["result"]["protocolVersion"] == CURRENT_PROTOCOL_VERSION
                        assert initialize_response.headers["MCP-Protocol-Version"] == CURRENT_PROTOCOL_VERSION

                        initialized_response = await client.post(
                            "/mcp",
                            headers=headers,
                            json={
                                "jsonrpc": "2.0",
                                "method": "notifications/initialized",
                                "params": {},
                            },
                        )
                        assert initialized_response.status == 202

                        list_response = await client.post(
                            "/mcp",
                            headers=headers,
                            json={"jsonrpc": "2.0", "id": "list-1", "method": "tools/list", "params": {}},
                        )
                        list_payload = await list_response.json()
                        tool_names = sorted(tool["name"] for tool in list_payload["result"]["tools"])
                        assert list_response.status == 200
                        assert tool_names == ["http.echo", "stdio.echo"]

                        http_call_response = await client.post(
                            "/mcp",
                            headers=headers,
                            json={
                                "jsonrpc": "2.0",
                                "id": "call-http",
                                "method": "tools/call",
                                "params": {"name": "http.echo", "arguments": {"value": "from-http"}},
                            },
                        )
                        http_call_payload = await http_call_response.json()
                        assert http_call_response.status == 200
                        assert http_call_payload["result"]["content"][0]["text"] == "http.echo:from-http"

                        stdio_call_response = await client.post(
                            "/mcp",
                            headers=headers,
                            json={
                                "jsonrpc": "2.0",
                                "id": "call-stdio",
                                "method": "tools/call",
                                "params": {"name": "stdio.echo", "arguments": {"value": "from-stdio"}},
                            },
                        )
                        stdio_call_payload = await stdio_call_response.json()
                        assert stdio_call_response.status == 200
                        assert stdio_call_payload["result"]["content"][0]["text"] == "stdio.echo:from-stdio"

                        request_count, response_count = await _logged_counts(store)
                        assert request_count >= 4
                        assert response_count >= 4
                        assert await _logged_tool_call_upstreams(store) == ["http-fixture", "stdio-fixture"]
                        assert seen_methods.count("initialize") >= 1
                        assert seen_methods.count("tools/list") >= 1
            finally:
                await gateway.close()
                await telemetry.close()
                await store.close()

    asyncio.run(run_test())


def test_rate_limits_apply_across_two_gateway_instances_with_shared_postgres() -> None:
    async def run_test() -> None:
        http_app = web.Application()

        async def upstream_handler(request: web.Request) -> web.StreamResponse:
            payload = await request.json()
            method = payload.get("method")
            if method == "notifications/initialized":
                return web.Response(status=202)
            if method == "initialize":
                return web.json_response(
                    {
                        "jsonrpc": "2.0",
                        "id": payload.get("id"),
                        "result": {
                            "protocolVersion": payload.get("params", {}).get("protocolVersion", CURRENT_PROTOCOL_VERSION),
                            "capabilities": {"tools": {}},
                            "serverInfo": {"name": "fake-http-upstream", "version": "0.1.0"},
                        },
                    }
                )
            if method == "tools/list":
                return web.json_response(
                    {
                        "jsonrpc": "2.0",
                        "id": payload.get("id"),
                        "result": {
                            "tools": [
                                {
                                    "name": "http.echo",
                                    "description": "Echoes the provided value.",
                                    "inputSchema": {"type": "object"},
                                }
                            ]
                        },
                    }
                )
            return web.json_response({"jsonrpc": "2.0", "id": payload.get("id"), "result": {}})

        http_app.router.add_post("/mcp", upstream_handler)

        async with TestServer(http_app) as upstream_server:
            dsn = os.environ[TEST_DATABASE_DSN_ENV]
            store_a = PostgresStore(dsn)
            store_b = PostgresStore(dsn)
            await store_a.start()
            await store_b.start()
            await _prepare_database(store_a)

            config = _gateway_config(str(upstream_server.make_url("/mcp")))
            config.gateway.public_tools_catalog = True
            config.gateway.rate_limit_per_minute = 1

            logger = Logger(stdout_json=False)
            telemetry_a = GatewayTelemetry()
            telemetry_b = GatewayTelemetry()
            gateway_a = Gateway(config, store_a, logger, telemetry_a)
            gateway_b = Gateway(config, store_b, logger, telemetry_b)
            server_a = HttpServer(config, gateway_a, logger, telemetry_a)
            server_b = HttpServer(config, gateway_b, logger, telemetry_b)

            try:
                await gateway_a.warmup()
                await gateway_b.warmup()

                async with TestServer(server_a.build_app()) as gateway_server_a:
                    async with TestServer(server_b.build_app()) as gateway_server_b:
                        async with TestClient(gateway_server_a) as client_a:
                            async with TestClient(gateway_server_b) as client_b:
                                headers = {"Accept": "application/json", "X-Client-Id": "tenant-shared"}
                                first = await client_a.get("/tools", headers=headers)
                                second = await client_b.get("/tools", headers=headers)

                                assert first.status == 200
                                assert second.status == 429
                                assert second.headers["Retry-After"]
            finally:
                await gateway_a.close()
                await gateway_b.close()
                await telemetry_a.close()
                await telemetry_b.close()
                await store_a.close()
                await store_b.close()

    asyncio.run(run_test())
