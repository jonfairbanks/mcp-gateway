from __future__ import annotations

from typing import Any, Dict, Optional


def sanitize_role(role: Any) -> Optional[str]:
    if not isinstance(role, str):
        return None
    normalized = role.strip().lower()
    return "admin" if normalized == "admin" else None


def serialize_user_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": str(row["id"]),
        "subject": row["subject"],
        "display_name": row.get("display_name"),
        "role": sanitize_role(row.get("role")),
        "issuer": row.get("issuer"),
        "email": row.get("email"),
        "auth_source": row.get("auth_source"),
        "is_active": row["is_active"],
        "last_seen_at": row["last_seen_at"].isoformat() if row.get("last_seen_at") is not None else None,
        "created_at": row["created_at"].isoformat() if row.get("created_at") is not None else None,
        "updated_at": row["updated_at"].isoformat() if row.get("updated_at") is not None else None,
    }


def serialize_api_key_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "api_key_id": str(row["id"]),
        "user_id": str(row["user_id"]),
        "key_name": row["key_name"],
        "key_prefix": row["key_prefix"],
        "is_active": row["is_active"],
        "created_at": row["created_at"].isoformat() if row.get("created_at") is not None else None,
        "last_used_at": row["last_used_at"].isoformat() if row.get("last_used_at") is not None else None,
        "expires_at": row["expires_at"].isoformat() if row.get("expires_at") is not None else None,
        "revoked_at": row["revoked_at"].isoformat() if row.get("revoked_at") is not None else None,
    }


def serialize_group_row(row: Dict[str, Any], *, is_reserved: bool = False) -> Dict[str, Any]:
    return {
        "id": str(row["id"]),
        "name": row["name"],
        "description": row.get("description"),
        "is_reserved": is_reserved,
        "created_at": row["created_at"].isoformat() if row.get("created_at") is not None else None,
        "updated_at": row["updated_at"].isoformat() if row.get("updated_at") is not None else None,
    }


def serialize_usage_row(row: Dict[str, Any], group_by: str) -> Dict[str, Any]:
    payload = {
        "request_count": row["request_count"],
        "tool_call_count": row["tool_call_count"],
        "success_count": row["success_count"],
        "denial_count": row["denial_count"],
        "cache_hit_count": row["cache_hit_count"],
        "last_used_at": row["last_used_at"].isoformat() if row.get("last_used_at") is not None else None,
    }
    if group_by == "subject":
        payload.update(
            {
                "subject": row["subject"],
                "display_name": row.get("display_name"),
                "email": row.get("email"),
                "auth_scheme": row.get("auth_scheme"),
            }
        )
    elif group_by == "integration":
        payload.update({"upstream_id": row["authorized_upstream_id"]})
    else:
        payload.update(
            {
                "api_key_id": str(row["api_key_id"]),
                "user_id": str(row["user_id"]) if row.get("user_id") is not None else None,
                "subject": row.get("subject"),
                "display_name": row.get("display_name"),
                "role": row.get("role"),
                "key_name": row.get("key_name"),
                "key_prefix": row.get("key_prefix"),
            }
        )
    return payload
