from __future__ import annotations

from datetime import datetime
from typing import Dict, Optional

from .postgres_serialization import serialize_usage_row


class PostgresReportingMixin:
    async def usage_summary(
        self,
        *,
        group_by: str,
        from_timestamp: Optional[datetime] = None,
        to_timestamp: Optional[datetime] = None,
    ) -> list[Dict[str, object]]:
        if not self._pool:
            raise RuntimeError("Postgres is not available")
        normalized_group_by = "subject" if group_by == "user" else group_by
        if normalized_group_by not in {"subject", "integration", "api_key"}:
            raise ValueError("group_by must be one of: subject, integration, api_key")

        if normalized_group_by == "subject":
            select_identity = """
                summary.auth_subject AS subject,
                users.display_name,
                users.email,
                summary.auth_scheme
            """
            join_identity = "LEFT JOIN gateway_users AS users ON users.subject = summary.auth_subject"
            group_identity = "summary.auth_subject, users.display_name, users.email, summary.auth_scheme"
            order_identity = "request_count DESC, last_used_at DESC, summary.auth_subject ASC"
            null_filter = "summary.auth_subject IS NOT NULL"
        elif normalized_group_by == "integration":
            select_identity = """
                summary.authorized_upstream_id
            """
            join_identity = ""
            group_identity = "summary.authorized_upstream_id"
            order_identity = "request_count DESC, last_used_at DESC, summary.authorized_upstream_id ASC"
            null_filter = "summary.authorized_upstream_id IS NOT NULL"
        else:
            select_identity = """
                summary.auth_api_key_id AS api_key_id,
                summary.auth_user_id AS user_id,
                summary.auth_subject AS subject,
                users.display_name,
                summary.auth_role AS role,
                keys.key_name,
                keys.key_prefix
            """
            join_identity = """
                LEFT JOIN gateway_users AS users ON users.id = summary.auth_user_id
                LEFT JOIN gateway_api_keys AS keys ON keys.id = summary.auth_api_key_id
            """
            group_identity = """
                summary.auth_api_key_id,
                summary.auth_user_id,
                summary.auth_subject,
                users.display_name,
                summary.auth_role,
                keys.key_name,
                keys.key_prefix
            """
            order_identity = "request_count DESC, last_used_at DESC, keys.key_name ASC"
            null_filter = "summary.auth_api_key_id IS NOT NULL"

        async with self._pool.connection() as conn:
            cur = await conn.execute(
                f"""
                WITH summary AS (
                    SELECT
                        req.id,
                        req.timestamp,
                        req.method,
                        req.auth_user_id,
                        req.auth_api_key_id,
                        req.auth_role,
                        req.auth_subject,
                        req.auth_scheme,
                        req.authorized_upstream_id,
                        EXISTS (
                            SELECT 1
                            FROM mcp_responses AS resp
                            WHERE resp.request_id = req.id
                              AND resp.success = TRUE
                        ) AS success_hit,
                        EXISTS (
                            SELECT 1
                            FROM mcp_responses AS resp
                            WHERE resp.request_id = req.id
                              AND resp.cache_hit = TRUE
                        ) AS cache_hit,
                        EXISTS (
                            SELECT 1
                            FROM mcp_denials AS denial
                            WHERE denial.request_id = req.id
                        ) AS denial_hit
                    FROM mcp_requests AS req
                    WHERE (%s::timestamptz IS NULL OR req.timestamp >= %s)
                      AND (%s::timestamptz IS NULL OR req.timestamp <= %s)
                )
                SELECT
                    {select_identity},
                    COUNT(*)::bigint AS request_count,
                    SUM(CASE WHEN summary.method = 'tools/call' THEN 1 ELSE 0 END)::bigint AS tool_call_count,
                    SUM(CASE WHEN summary.success_hit THEN 1 ELSE 0 END)::bigint AS success_count,
                    SUM(CASE WHEN summary.denial_hit THEN 1 ELSE 0 END)::bigint AS denial_count,
                    SUM(CASE WHEN summary.cache_hit THEN 1 ELSE 0 END)::bigint AS cache_hit_count,
                    MAX(summary.timestamp) AS last_used_at
                FROM summary
                {join_identity}
                WHERE {null_filter}
                GROUP BY {group_identity}
                ORDER BY {order_identity}
                """,
                (from_timestamp, from_timestamp, to_timestamp, to_timestamp),
            )
            rows = await cur.fetchall()
            return [serialize_usage_row(row, normalized_group_by) for row in rows]
