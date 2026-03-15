from __future__ import annotations

import hashlib
import hmac
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from uuid import uuid4

from .config import AppConfig
from .logging import Logger
from .postgres import PostgresStore
from .request_context import AuthenticatedPrincipal

AUTH_MODE_SINGLE_SHARED = "single_shared"
AUTH_MODE_POSTGRES_API_KEYS = "postgres_api_keys"
ROLE_ADMIN = "admin"
ROLE_MEMBER = "member"
ROLE_VIEWER = "viewer"
VALID_ROLES = frozenset({ROLE_ADMIN, ROLE_MEMBER, ROLE_VIEWER})
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


def normalize_role(role: str) -> str:
    normalized_role = role.strip().lower()
    if normalized_role not in VALID_ROLES:
        raise ValueError("role must be one of: admin, member, viewer")
    return normalized_role


class AuthService:
    def __init__(self, config: AppConfig, store: PostgresStore, logger: Logger) -> None:
        self._config = config
        self._store = store
        self._logger = logger

    def auth_mode_requires_database(self) -> bool:
        return self._config.gateway.auth_mode == AUTH_MODE_POSTGRES_API_KEYS

    def auth_required(self) -> bool:
        if self._config.gateway.auth_mode == AUTH_MODE_SINGLE_SHARED:
            return bool(self._config.gateway.api_key)
        return not self._config.gateway.allow_unauthenticated

    async def authenticate_token(self, token: Optional[str]) -> Optional[AuthenticatedPrincipal]:
        if self._config.gateway.auth_mode == AUTH_MODE_SINGLE_SHARED:
            return self._authenticate_single_shared(token)
        return await self._authenticate_postgres_api_key(token)

    def _authenticate_single_shared(self, token: Optional[str]) -> Optional[AuthenticatedPrincipal]:
        expected = self._config.gateway.api_key
        if not expected or not token:
            return None
        if not hmac.compare_digest(token, expected):
            return None
        return AuthenticatedPrincipal(subject="gateway", auth_scheme="shared_bearer", role=ROLE_ADMIN)

    async def _authenticate_postgres_api_key(self, token: Optional[str]) -> Optional[AuthenticatedPrincipal]:
        if not token:
            return None
        bootstrap = self._config.gateway.bootstrap_admin_api_key
        if bootstrap and hmac.compare_digest(token, bootstrap):
            return AuthenticatedPrincipal(
                subject="bootstrap-admin",
                auth_scheme="bootstrap_admin",
                role=ROLE_ADMIN,
                is_bootstrap_admin=True,
            )
        try:
            row = await self._store.find_api_key_identity(extract_api_key_prefix(token))
        except Exception as exc:  # noqa: BLE001
            self._logger.error("auth_backend_unavailable", auth_mode=self._config.gateway.auth_mode, error=str(exc))
            raise AuthUnavailableError("Auth backend unavailable") from exc
        if not row:
            return None
        if not hmac.compare_digest(hash_api_key(token), row["key_hash"]):
            return None
        try:
            await self._store.touch_api_key_last_used(str(row["api_key_id"]))
        except Exception as exc:  # noqa: BLE001
            self._logger.warn("auth_backend_touch_failed", api_key_id=str(row["api_key_id"]), error=str(exc))
        return AuthenticatedPrincipal(
            subject=row["subject"],
            auth_scheme="postgres_api_key",
            role=row["role"],
            user_id=str(row["user_id"]),
            api_key_id=str(row["api_key_id"]),
        )

    async def issue_api_key(
        self,
        *,
        subject: str,
        display_name: Optional[str],
        role: str,
        key_name: str,
        expires_days: Optional[int] = None,
    ) -> dict[str, Optional[str]]:
        normalized_role = normalize_role(role)
        if not subject.strip():
            raise ValueError("subject is required")
        if not key_name.strip():
            raise ValueError("key_name is required")

        expires_at: Optional[datetime] = None
        if expires_days is not None:
            expires_at = datetime.now(timezone.utc) + timedelta(days=max(1, expires_days))

        return await self._issue_api_key(
            subject=subject.strip(),
            display_name=(display_name or subject).strip(),
            role=normalized_role,
            key_name=key_name.strip(),
            expires_at=expires_at,
        )

    async def _issue_api_key(
        self,
        *,
        subject: str,
        display_name: str,
        role: str,
        key_name: str,
        expires_at: Optional[datetime],
    ) -> dict[str, Optional[str]]:
        api_key, key_prefix, key_hash = generate_api_key()
        issued = await self._store.issue_api_key(
            user_id=uuid4(),
            subject=subject,
            display_name=display_name,
            role=role,
            api_key_id=uuid4(),
            key_name=key_name,
            key_prefix=key_prefix,
            key_hash=key_hash,
            expires_at=expires_at,
        )
        issued["api_key"] = api_key
        return issued

    async def issue_api_key_for_user(
        self,
        *,
        user_id: str,
        key_name: str,
        expires_at: Optional[datetime] = None,
    ) -> dict[str, Optional[str]]:
        if not user_id.strip():
            raise ValueError("user_id is required")
        if not key_name.strip():
            raise ValueError("key_name is required")

        api_key, key_prefix, key_hash = generate_api_key()
        issued = await self._store.issue_api_key_for_user(
            user_id=user_id.strip(),
            api_key_id=uuid4(),
            key_name=key_name.strip(),
            key_prefix=key_prefix,
            key_hash=key_hash,
            expires_at=expires_at,
        )
        issued["api_key"] = api_key
        return issued

    async def get_user_by_id(self, user_id: str) -> Optional[Dict[str, Any]]:
        return await self._store.get_user_by_id(user_id)

    async def get_user_by_subject(self, subject: str) -> Optional[Dict[str, Any]]:
        return await self._store.get_user_by_subject(subject)

    async def create_user(self, *, subject: str, display_name: Optional[str], role: str) -> Optional[Dict[str, Any]]:
        normalized_role = normalize_role(role)
        normalized_subject = subject.strip()
        if not normalized_subject:
            raise ValueError("subject is required")
        normalized_display_name = (display_name or normalized_subject).strip()
        if not normalized_display_name:
            raise ValueError("display_name is required")
        return await self._store.create_user(
            subject=normalized_subject,
            display_name=normalized_display_name,
            role=normalized_role,
        )

    async def list_users(self) -> list[Dict[str, Any]]:
        return await self._store.list_users()

    async def update_user(
        self,
        user_id: str,
        *,
        display_name: Optional[str] = None,
        role: Optional[str] = None,
        is_active: Optional[bool] = None,
    ) -> Optional[Dict[str, Any]]:
        normalized_role = normalize_role(role) if role is not None else None
        normalized_display_name = display_name.strip() if isinstance(display_name, str) else None
        if isinstance(display_name, str) and not normalized_display_name:
            raise ValueError("display_name must not be empty")
        return await self._store.update_user(
            user_id,
            display_name=normalized_display_name,
            role=normalized_role,
            is_active=is_active,
        )

    async def list_api_keys(self, *, user_id: str) -> list[Dict[str, Any]]:
        return await self._store.list_api_keys(user_id=user_id)

    async def revoke_api_key(self, api_key_id: str, *, user_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
        return await self._store.revoke_api_key(api_key_id, user_id=user_id)

    async def usage_summary(
        self,
        *,
        group_by: str,
        from_timestamp: Optional[datetime] = None,
        to_timestamp: Optional[datetime] = None,
    ) -> list[Dict[str, Any]]:
        return await self._store.usage_summary(
            group_by=group_by,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
        )
