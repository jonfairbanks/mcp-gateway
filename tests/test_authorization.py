from __future__ import annotations

import asyncio

from mcp_gateway.authorization import (
    PLATFORM_PERMISSION_IDENTITIES_WRITE,
    PLATFORM_PERMISSION_USAGE_READ,
    AuthorizationService,
)
from mcp_gateway.config import (
    AppConfig,
    CacheConfig,
    GatewayConfig,
    LoggingConfig,
    UpstreamConfig,
)
from mcp_gateway.request_context import AuthenticatedPrincipal


class FakePolicyStore:
    def __init__(self) -> None:
        self.revision = 0
        self.integration_policies: list[dict[str, str]] = []
        self.platform_policies: list[dict[str, str]] = []

    async def get_policy_revision(self) -> int:
        return self.revision

    async def list_group_integration_policies(self):
        return list(self.integration_policies)

    async def list_group_platform_policies(self):
        return list(self.platform_policies)


def _config() -> AppConfig:
    return AppConfig(
        gateway=GatewayConfig(
            listen_host="0.0.0.0",
            listen_port=8080,
            auth_mode="postgres_api_keys",
            api_key="",
            bootstrap_admin_api_key="",
            allow_unauthenticated=False,
            public_tools_catalog=False,
            trusted_proxies=["127.0.0.1", "::1"],
            request_max_bytes=2 * 1024 * 1024,
            rate_limit_per_minute=120,
            circuit_breaker_fail_threshold=10,
            circuit_breaker_open_seconds=30,
            sse_queue_max_messages=100,
            max_sse_sessions=1000,
        ),
        logging=LoggingConfig(stdout_json=False, extra_redact_fields=[]),
        cache=CacheConfig(enabled=True, max_entries=100, default_ttl_minutes=60, client_scoped_tools=[]),
        upstreams=[
            UpstreamConfig(
                id="jira",
                name="jira",
                transport="http_sse",
                endpoint="https://example.com/jira",
                http_headers={},
                bearer_token_env_var=None,
                http_serialize_requests=False,
                command=None,
                env={},
                cwd=None,
                timeout_ms=1000,
                stdio_read_limit_bytes=1024 * 1024,
                max_in_flight=10,
                deny_tools=[],
                cache_ttl_minutes=None,
                circuit_breaker_fail_threshold=None,
                circuit_breaker_open_seconds=None,
                tool_routes=[],
            ),
            UpstreamConfig(
                id="github",
                name="github",
                transport="http_sse",
                endpoint="https://example.com/github",
                http_headers={},
                bearer_token_env_var=None,
                http_serialize_requests=False,
                command=None,
                env={},
                cwd=None,
                timeout_ms=1000,
                stdio_read_limit_bytes=1024 * 1024,
                max_in_flight=10,
                deny_tools=[],
                cache_ttl_minutes=None,
                circuit_breaker_fail_threshold=None,
                circuit_breaker_open_seconds=None,
                tool_routes=[],
            ),
            UpstreamConfig(
                id="aws",
                name="aws",
                transport="http_sse",
                endpoint="https://example.com/aws",
                http_headers={},
                bearer_token_env_var=None,
                http_serialize_requests=False,
                command=None,
                env={},
                cwd=None,
                timeout_ms=1000,
                stdio_read_limit_bytes=1024 * 1024,
                max_in_flight=10,
                deny_tools=[],
                cache_ttl_minutes=None,
                circuit_breaker_fail_threshold=None,
                circuit_breaker_open_seconds=None,
                tool_routes=[],
            ),
            UpstreamConfig(
                id="context7",
                name="context7",
                transport="http_sse",
                endpoint="https://example.com/context7",
                http_headers={},
                bearer_token_env_var=None,
                http_serialize_requests=False,
                command=None,
                env={},
                cwd=None,
                timeout_ms=1000,
                stdio_read_limit_bytes=1024 * 1024,
                max_in_flight=10,
                deny_tools=[],
                cache_ttl_minutes=None,
                circuit_breaker_fail_threshold=None,
                circuit_breaker_open_seconds=None,
                tool_routes=[],
            ),
            UpstreamConfig(
                id="notion",
                name="notion",
                transport="http_sse",
                endpoint="https://example.com/notion",
                http_headers={},
                bearer_token_env_var=None,
                http_serialize_requests=False,
                command=None,
                env={},
                cwd=None,
                timeout_ms=1000,
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


def test_authorization_service_enforces_group_integration_matrix() -> None:
    store = FakePolicyStore()
    store.integration_policies = [
        {"group_name": "sales", "upstream_id": "jira"},
        {"group_name": "developers", "upstream_id": "jira"},
        {"group_name": "developers", "upstream_id": "github"},
        {"group_name": "developers", "upstream_id": "aws"},
        {"group_name": "developers", "upstream_id": "context7"},
        {"group_name": "developers", "upstream_id": "notion"},
    ]
    authorization = AuthorizationService(_config(), store)

    sales = AuthenticatedPrincipal(subject="sally", auth_scheme="postgres_api_key", group_names=("sales",))
    developers = AuthenticatedPrincipal(subject="devon", auth_scheme="postgres_api_key", group_names=("developers",))

    assert asyncio.run(authorization.authorize_integration(sales, "jira")) is True
    assert asyncio.run(authorization.authorize_integration(sales, "aws")) is False
    assert asyncio.run(authorization.authorize_integration(developers, "jira")) is True
    assert asyncio.run(authorization.authorize_integration(developers, "github")) is True
    assert asyncio.run(authorization.authorize_integration(developers, "aws")) is True
    assert asyncio.run(authorization.authorize_integration(developers, "context7")) is True
    assert asyncio.run(authorization.authorize_integration(developers, "notion")) is True


def test_authorization_service_reloads_group_grants_on_revision_change() -> None:
    store = FakePolicyStore()
    authorization = AuthorizationService(_config(), store)
    principal = AuthenticatedPrincipal(subject="sally", auth_scheme="postgres_api_key", group_names=("sales",))

    assert asyncio.run(authorization.authorize_integration(principal, "jira")) is False

    store.integration_policies = [{"group_name": "sales", "upstream_id": "jira"}]
    store.revision = 1

    assert asyncio.run(authorization.authorize_integration(principal, "jira")) is True


def test_authorization_service_honors_platform_grants_and_legacy_admin_role() -> None:
    store = FakePolicyStore()
    store.platform_policies = [{"group_name": "auditor", "permission": PLATFORM_PERMISSION_USAGE_READ}]
    authorization = AuthorizationService(_config(), store)

    auditor = AuthenticatedPrincipal(subject="amy", auth_scheme="postgres_api_key", group_names=("auditor",))
    legacy_admin = AuthenticatedPrincipal(subject="root", auth_scheme="postgres_api_key", role="admin")

    assert asyncio.run(authorization.authorize_platform(auditor, PLATFORM_PERMISSION_USAGE_READ)) is True
    assert asyncio.run(authorization.authorize_platform(auditor, PLATFORM_PERMISSION_IDENTITIES_WRITE)) is False
    assert asyncio.run(authorization.authorize_platform(legacy_admin, PLATFORM_PERMISSION_IDENTITIES_WRITE)) is True
