from __future__ import annotations

import asyncio
from functools import wraps
from typing import Optional

from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from .postgres_audit import PostgresAuditMixin
from .postgres_identities import PostgresIdentityMixin
from .postgres_rbac import PostgresRBACMixin
from .postgres_reporting import PostgresReportingMixin


class PostgresStore(PostgresAuditMixin, PostgresIdentityMixin, PostgresRBACMixin, PostgresReportingMixin):
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._pool: Optional[AsyncConnectionPool] = None
        self._lock = asyncio.Lock()
        self._tracer = trace.get_tracer("mcp_gateway.postgres")

    async def start(self) -> None:
        async with self._lock:
            if not self._dsn:
                return
            if self._pool is None:
                self._pool = AsyncConnectionPool(conninfo=self._dsn, open=False, kwargs={"row_factory": dict_row})
                await self._pool.open()

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()

    def is_available(self) -> bool:
        return self._pool is not None


def _instrument_postgres_method(name: str) -> None:
    original = getattr(PostgresStore, name)

    @wraps(original)
    async def wrapped(self: PostgresStore, *args, **kwargs):
        with self._tracer.start_as_current_span(
            f"postgres.{name}",
            kind=SpanKind.CLIENT,
            attributes={
                "db.system": "postgresql",
                "db.namespace": "mcp_gateway",
                "db.operation.name": name,
            },
        ) as span:
            try:
                result = await original(self, *args, **kwargs)
            except Exception as exc:
                if span.is_recording():
                    span.record_exception(exc)
                    span.set_status(Status(StatusCode.ERROR, str(exc) or type(exc).__name__))
                raise
            if span.is_recording():
                span.set_status(Status(StatusCode.OK))
            return result

    setattr(PostgresStore, name, wrapped)


for _method_name in (
    "log_request",
    "log_response",
    "log_denial",
    "cache_get",
    "cache_set",
    "cleanup_expired_cache",
    "consume_rate_limit",
    "cleanup_expired_rate_limits",
    "get_policy_revision",
    "find_api_key_identity",
    "touch_api_key_last_used",
    "get_user_by_id",
    "get_user_by_subject",
    "create_user",
    "list_users",
    "update_user",
    "put_identity",
    "patch_identity",
    "list_identities",
    "list_group_names_for_subject",
    "list_groups",
    "create_group",
    "update_group",
    "delete_group",
    "add_group_member",
    "remove_group_member",
    "list_group_integration_grants",
    "add_group_integration_grant",
    "remove_group_integration_grant",
    "list_group_platform_grants",
    "add_group_platform_grant",
    "remove_group_platform_grant",
    "list_group_integration_policies",
    "list_group_platform_policies",
    "list_api_keys",
    "issue_api_key_for_user",
    "revoke_api_key",
    "usage_summary",
    "issue_api_key",
):
    _instrument_postgres_method(_method_name)

del _method_name
