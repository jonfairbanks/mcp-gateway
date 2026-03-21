from __future__ import annotations

from typing import Any, Dict, Optional


class GatewayHTTPError(Exception):
    def __init__(
        self,
        status: int,
        error: str,
        message: str,
        *,
        fields: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.error = error
        self.message = message
        self.fields = fields or {}


class NotFoundError(GatewayHTTPError):
    def __init__(self, message: str, **fields: Any) -> None:
        super().__init__(404, "NotFound", message, fields=fields)


class ConflictError(GatewayHTTPError):
    def __init__(self, message: str, **fields: Any) -> None:
        super().__init__(409, "Conflict", message, fields=fields)
