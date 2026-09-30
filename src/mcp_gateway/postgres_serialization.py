from __future__ import annotations

from typing import Any, Dict


def serialize_api_key_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "api_key_id": str(row["id"]),
        "key_name": row["key_name"],
        "key_prefix": row["key_prefix"],
        "is_active": row["is_active"],
        "created_at": row["created_at"].isoformat() if row.get("created_at") is not None else None,
        "last_used_at": row["last_used_at"].isoformat() if row.get("last_used_at") is not None else None,
        "expires_at": row["expires_at"].isoformat() if row.get("expires_at") is not None else None,
        "revoked_at": row["revoked_at"].isoformat() if row.get("revoked_at") is not None else None,
    }
