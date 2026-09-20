from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class AuthenticatedPrincipal:
    subject: str
    auth_scheme: str
    api_key_id: Optional[str] = None
    key_name: Optional[str] = None


@dataclass(frozen=True)
class RequestContext:
    client_id: Optional[str]
    principal: Optional[AuthenticatedPrincipal] = None
