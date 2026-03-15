from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class AuthenticatedPrincipal:
    subject: str
    auth_scheme: str
    role: str = "admin"
    user_id: Optional[str] = None
    api_key_id: Optional[str] = None
    is_bootstrap_admin: bool = False


@dataclass(frozen=True)
class RequestContext:
    client_id: Optional[str]
    principal: Optional[AuthenticatedPrincipal] = None

    @property
    def is_authenticated(self) -> bool:
        return self.principal is not None

    @property
    def role(self) -> Optional[str]:
        if not self.principal:
            return None
        return self.principal.role
