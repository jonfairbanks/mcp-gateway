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
        self.api_key = None
        self.touched: list[str] = []
        self.issued = None
        self.keys: list[dict[str, object]] = []

    async def find_api_key(self, key_prefix: str):
        if self.api_key and self.api_key["key_prefix"] == key_prefix:
            return self.api_key
        return None

    async def touch_api_key_last_used(self, api_key_id: str) -> None:
        self.touched.append(api_key_id)

    async def issue_api_key(self, **kwargs):
        self.issued = kwargs
        return {
            "api_key_id": str(kwargs["api_key_id"]),
            "key_name": kwargs["key_name"],
            "expires_at": kwargs["expires_at"].isoformat() if kwargs["expires_at"] is not None else None,
        }

    async def list_api_keys(self):
        return self.keys

    async def revoke_api_key(self, api_key_id: str):
        return {"id": api_key_id, "revoked": True}


def _config(*, auth_mode: str, api_key: str = "", bootstrap_api_key: str = "", allow_unauthenticated: bool = False):
    return AppConfig(
        gateway=GatewayConfig(
            listen_host="0.0.0.0",
            listen_port=8080,
            auth_mode=auth_mode,
            api_key=api_key,
            bootstrap_api_key=bootstrap_api_key,
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
    assert principal.subject == "gateway"
    assert principal.auth_scheme == "shared_bearer"
    assert not hasattr(principal, "role")


def test_single_shared_rejects_wrong_or_missing_key_even_when_auth_is_required() -> None:
    auth = AuthService(_config(auth_mode="single_shared", api_key="secret"), FakeStore(), Logger(stdout_json=False))

    assert asyncio.run(auth.authenticate_token("wrong")) is None
    assert asyncio.run(auth.authenticate_token(None)) is None
    assert auth.auth_required() is True


def test_missing_shared_key_requires_auth_unless_unauthenticated_is_explicit() -> None:
    auth = AuthService(_config(auth_mode="single_shared"), FakeStore(), Logger(stdout_json=False))
    anonymous = AuthService(
        _config(auth_mode="single_shared", allow_unauthenticated=True), FakeStore(), Logger(stdout_json=False)
    )

    assert auth.auth_required() is True
    assert asyncio.run(auth.authenticate_token(None)) is None
    assert anonymous.auth_required() is False


def test_configured_shared_key_requires_auth_even_when_unauthenticated_is_enabled() -> None:
    auth = AuthService(
        _config(auth_mode="single_shared", api_key="secret", allow_unauthenticated=True),
        FakeStore(),
        Logger(stdout_json=False),
    )

    assert auth.auth_required() is True


def test_postgres_authenticates_bootstrap_key() -> None:
    auth = AuthService(
        _config(auth_mode=AUTH_MODE_POSTGRES_API_KEYS, bootstrap_api_key="bootstrap-secret"),
        FakeStore(),
        Logger(stdout_json=False),
    )

    principal = asyncio.run(auth.authenticate_token("bootstrap-secret"))

    assert principal is not None
    assert principal.subject == "gateway"
    assert principal.auth_scheme == "bootstrap_key"


def test_postgres_authenticates_database_api_key_and_touches_last_used() -> None:
    api_key, key_prefix, key_hash = generate_api_key()
    store = FakeStore()
    store.api_key = {"id": "key-1", "key_prefix": key_prefix, "key_hash": key_hash, "key_name": "laptop"}
    auth = AuthService(_config(auth_mode=AUTH_MODE_POSTGRES_API_KEYS), store, Logger(stdout_json=False))

    principal = asyncio.run(auth.authenticate_token(api_key))

    assert principal is not None
    assert principal.subject == "api_key:key-1"
    assert principal.auth_scheme == "postgres_api_key"
    assert principal.api_key_id == "key-1"
    assert principal.key_name == "laptop"
    assert store.touched == ["key-1"]


def test_issue_api_key_generates_hash_and_prefix_without_storing_plaintext() -> None:
    store = FakeStore()
    auth = AuthService(_config(auth_mode=AUTH_MODE_POSTGRES_API_KEYS), store, Logger(stdout_json=False))

    issued = asyncio.run(auth.issue_api_key(key_name="laptop", expires_days=7))

    assert issued["api_key"].startswith("mgw_")
    assert store.issued is not None
    assert store.issued["key_prefix"] == extract_api_key_prefix(issued["api_key"])
    assert store.issued["key_hash"] == hash_api_key(issued["api_key"])
    assert store.issued["key_name"] == "laptop"


@pytest.mark.parametrize("expires_days", [0, -1])
def test_issue_api_key_rejects_nonpositive_expiry(expires_days: int) -> None:
    auth = AuthService(_config(auth_mode=AUTH_MODE_POSTGRES_API_KEYS), FakeStore(), Logger(stdout_json=False))

    with pytest.raises(ValueError, match="greater than 0"):
        asyncio.run(auth.issue_api_key(key_name="laptop", expires_days=expires_days))


def test_list_and_revoke_api_keys_delegate_to_store() -> None:
    store = FakeStore()
    store.keys = [{"id": "key-1", "key_name": "laptop"}]
    auth = AuthService(_config(auth_mode=AUTH_MODE_POSTGRES_API_KEYS), store, Logger(stdout_json=False))

    assert asyncio.run(auth.list_api_keys()) == [{"id": "key-1", "key_name": "laptop"}]
    assert asyncio.run(auth.revoke_api_key("key-1")) == {"id": "key-1", "revoked": True}



def test_database_key_with_correct_prefix_but_wrong_secret_is_rejected() -> None:
    api_key, prefix, key_hash = generate_api_key()
    store = FakeStore()
    store.api_key = {"id": "key-1", "key_prefix": prefix, "key_hash": key_hash, "key_name": "laptop"}
    auth = AuthService(_config(auth_mode=AUTH_MODE_POSTGRES_API_KEYS), store, Logger(stdout_json=False))
    assert asyncio.run(auth.authenticate_token(api_key + "wrong")) is None
    assert store.touched == []
