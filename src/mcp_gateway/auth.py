from __future__ import annotations

import hashlib
import hmac
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional
from uuid import UUID, uuid4

from .config import AUTH_MODE_POSTGRES_API_KEYS, AUTH_MODE_SINGLE_SHARED, AppConfig
from .logging import Logger
from .postgres import PostgresStore
from .request_context import AuthenticatedPrincipal

API_KEY_PREFIX_LENGTH = 12


class AuthUnavailableError(RuntimeError):
    pass


def hash_api_key(api_key: str) -> str:
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()


def extract_api_key_prefix(api_key: str) -> str:
    if api_key.startswith("mgw_"):
        parts = api_key.split("_", 2)
        if len(parts) == 3 and parts[1]:
            return parts[1]
    return api_key[:API_KEY_PREFIX_LENGTH]


def generate_api_key() -> tuple[str, str, str]:
    prefix = secrets.token_hex(API_KEY_PREFIX_LENGTH // 2)
    secret = secrets.token_urlsafe(24)
    api_key = f"mgw_{prefix}_{secret}"
    return api_key, prefix, hash_api_key(api_key)


class AuthService:
    def __init__(self, config: AppConfig, store: PostgresStore, logger: Logger) -> None:
        self._config = config
        self._store = store
        self._logger = logger

    def auth_required(self) -> bool:
        if self._config.gateway.auth_mode == AUTH_MODE_SINGLE_SHARED and self._config.gateway.api_key:
            return True
        return not self._config.gateway.allow_unauthenticated

    async def authenticate_token(self, token: Optional[str]) -> Optional[AuthenticatedPrincipal]:
        auth_mode = self._config.gateway.auth_mode
        if auth_mode == AUTH_MODE_SINGLE_SHARED:
            return self._authenticate_single_shared(token)
        if auth_mode == AUTH_MODE_POSTGRES_API_KEYS:
            return await self._authenticate_postgres_api_key(token)
        raise AuthUnavailableError(f"Unsupported auth mode: {auth_mode}")

    def _authenticate_single_shared(self, token: Optional[str]) -> Optional[AuthenticatedPrincipal]:
        expected = self._config.gateway.api_key
        if not expected or not token or not hmac.compare_digest(token, expected):
            return None
        return AuthenticatedPrincipal(subject="gateway", auth_scheme="shared_bearer")

    async def _authenticate_postgres_api_key(self, token: Optional[str]) -> Optional[AuthenticatedPrincipal]:
        if not token:
            return None

        bootstrap = self._config.gateway.bootstrap_api_key
        if bootstrap and hmac.compare_digest(token, bootstrap):
            return AuthenticatedPrincipal(subject="gateway", auth_scheme="bootstrap_key")

        try:
            row = await self._store.find_api_key(extract_api_key_prefix(token))
        except Exception as exc:  # noqa: BLE001
            self._logger.error("auth_backend_unavailable", auth_mode=self._config.gateway.auth_mode, error=str(exc))
            raise AuthUnavailableError("Auth backend unavailable") from exc

        if not row or not hmac.compare_digest(hash_api_key(token), str(row["key_hash"])):
            return None

        api_key_id = str(row["id"])
        try:
            await self._store.touch_api_key_last_used(api_key_id)
        except Exception as exc:  # noqa: BLE001
            self._logger.warn("auth_backend_touch_failed", api_key_id=api_key_id, error=str(exc))

        return AuthenticatedPrincipal(
            subject=f"api_key:{api_key_id}",
            auth_scheme="postgres_api_key",
            api_key_id=api_key_id,
            key_name=str(row["key_name"]),
        )

    async def issue_api_key(self, *, key_name: str, expires_days: Optional[int] = None) -> dict[str, object]:
        normalized_key_name = key_name.strip()
        if not normalized_key_name:
            raise ValueError("key_name is required")
        if expires_days is not None and expires_days <= 0:
            raise ValueError("expires_days must be greater than 0 when provided")

        expires_at: Optional[datetime] = None
        if expires_days is not None:
            expires_at = datetime.now(timezone.utc) + timedelta(days=expires_days)

        api_key, key_prefix, key_hash = generate_api_key()
        api_key_id: UUID = uuid4()
        issued = await self._store.issue_api_key(
            api_key_id=api_key_id,
            key_name=normalized_key_name,
            key_prefix=key_prefix,
            key_hash=key_hash,
            expires_at=expires_at,
        )
        issued["api_key"] = api_key
        return issued

    async def list_api_keys(self) -> list[dict[str, object]]:
        return await self._store.list_api_keys()

    async def revoke_api_key(self, api_key_id: str) -> Optional[dict[str, object]]:
        return await self._store.revoke_api_key(api_key_id)
