from __future__ import annotations

import asyncio

import pytest

from mcp_gateway.auth import (
    AUTH_MODE_POSTGRES_API_KEYS,
    AuthService,
    extract_api_key_prefix,
    generate_api_key,
    hash_api_key,
)
from mcp_gateway.config import AppConfig, CacheConfig, GatewayConfig, LoggingConfig
from mcp_gateway.logging import Logger


class FakeStore:
    def __init__(self) -> None:
        self.identity = None
        self.touched: list[str] = []
        self.issued = None
        self.group_names: list[str] = []

    async def find_api_key_identity(self, key_prefix: str):
        if self.identity and self.identity["key_prefix"] == key_prefix:
            return self.identity
        return None

    async def touch_api_key_last_used(self, api_key_id: str) -> None:
        self.touched.append(api_key_id)

    async def issue_api_key(self, **kwargs):
        self.issued = kwargs
        return {
            "user_id": "user-1",
            "subject": kwargs["subject"],
            "display_name": kwargs["display_name"],
            "role": kwargs["role"],
            "api_key_id": "key-1",
            "key_name": kwargs["key_name"],
            "expires_at": kwargs["expires_at"].isoformat() if kwargs["expires_at"] is not None else None,
        }

    async def list_group_names_for_subject(self, subject: str):
        return list(self.group_names)


def _config(*, auth_mode: str, api_key: str = "", bootstrap_admin_api_key: str = "", allow_unauthenticated: bool = False):
    return AppConfig(
        gateway=GatewayConfig(
            listen_host="0.0.0.0",
            listen_port=8080,
            auth_mode=auth_mode,
            api_key=api_key,
            bootstrap_admin_api_key=bootstrap_admin_api_key,
            allow_unauthenticated=allow_unauthenticated,
            public_tools_catalog=False,
            trusted_proxies=["127.0.0.1", "::1"],
            request_max_bytes=2 * 1024 * 1024,
            rate_limit_per_minute=120,
            circuit_breaker_fail_threshold=10,
            circuit_breaker_open_seconds=30,
        ),
        logging=LoggingConfig(stdout_json=False, extra_redact_fields=[]),
        cache=CacheConfig(enabled=True, max_entries=100, default_ttl_minutes=60),
        upstreams=[],
    )


def test_single_shared_authenticates_configured_api_key() -> None:
    auth = AuthService(_config(auth_mode="single_shared", api_key="secret"), FakeStore(), Logger(stdout_json=False))

    principal = asyncio.run(auth.authenticate_token("secret"))

    assert principal is not None
    assert principal.auth_scheme == "shared_bearer"
    assert principal.role == "admin"


def test_postgres_authenticates_bootstrap_admin_key() -> None:
    auth = AuthService(
        _config(auth_mode=AUTH_MODE_POSTGRES_API_KEYS, bootstrap_admin_api_key="bootstrap-secret"),
        FakeStore(),
        Logger(stdout_json=False),
    )

    principal = asyncio.run(auth.authenticate_token("bootstrap-secret"))

    assert principal is not None
    assert principal.is_bootstrap_admin is True
    assert principal.role == "admin"


def test_postgres_authenticates_database_api_key_and_touches_last_used() -> None:
    api_key, key_prefix, key_hash = generate_api_key()
    store = FakeStore()
    store.group_names = ["sales"]
    store.identity = {
        "api_key_id": "key-1",
        "key_prefix": key_prefix,
        "key_hash": key_hash,
        "user_id": "user-1",
        "subject": "alice",
        "display_name": "Alice",
        "role": "member",
    }
    auth = AuthService(_config(auth_mode=AUTH_MODE_POSTGRES_API_KEYS), store, Logger(stdout_json=False))

    principal = asyncio.run(auth.authenticate_token(api_key))

    assert principal is not None
    assert principal.user_id == "user-1"
    assert principal.api_key_id == "key-1"
    assert principal.role is None
    assert principal.group_names == ("sales",)
    assert store.touched == ["key-1"]


def test_issue_api_key_generates_hash_and_prefix_without_storing_plaintext() -> None:
    store = FakeStore()
    auth = AuthService(_config(auth_mode=AUTH_MODE_POSTGRES_API_KEYS), store, Logger(stdout_json=False))

    issued = asyncio.run(
        auth.issue_api_key(
            subject="alice",
            display_name="Alice",
            role=None,
            key_name="default",
            expires_days=7,
        )
    )

    assert issued["api_key"].startswith("mgw_")
    assert store.issued is not None
    assert store.issued["subject"] == "alice"
    assert store.issued["role"] is None
    assert store.issued["key_prefix"] == extract_api_key_prefix(issued["api_key"])
    assert store.issued["key_hash"] == hash_api_key(issued["api_key"])


def test_issue_api_key_rejects_unknown_role() -> None:
    auth = AuthService(_config(auth_mode=AUTH_MODE_POSTGRES_API_KEYS), FakeStore(), Logger(stdout_json=False))

    with pytest.raises(ValueError, match="must be admin"):
        asyncio.run(
            auth.issue_api_key(
                subject="alice",
                display_name="Alice",
                role="owner",
                key_name="default",
            )
        )
