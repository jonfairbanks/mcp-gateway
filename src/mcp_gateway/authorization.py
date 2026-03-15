from __future__ import annotations

import asyncio
from typing import Any, Iterable, Optional

try:
    import casbin  # type: ignore[import-not-found]
except Exception:  # noqa: BLE001
    casbin = None

from .config import AppConfig
from .request_context import AuthenticatedPrincipal

GROUP_LEGACY_ADMIN = "legacy_admin"
GROUP_LEGACY_MEMBER = "legacy_member"
GROUP_LEGACY_VIEWER = "legacy_viewer"
RESERVED_GROUP_NAMES = frozenset({GROUP_LEGACY_ADMIN, GROUP_LEGACY_MEMBER, GROUP_LEGACY_VIEWER})

PLATFORM_PERMISSION_IDENTITIES_READ = "admin.identities.read"
PLATFORM_PERMISSION_IDENTITIES_WRITE = "admin.identities.write"
PLATFORM_PERMISSION_GROUPS_READ = "admin.groups.read"
PLATFORM_PERMISSION_GROUPS_WRITE = "admin.groups.write"
PLATFORM_PERMISSION_USAGE_READ = "admin.usage.read"
ALL_PLATFORM_PERMISSIONS = (
    PLATFORM_PERMISSION_IDENTITIES_READ,
    PLATFORM_PERMISSION_IDENTITIES_WRITE,
    PLATFORM_PERMISSION_GROUPS_READ,
    PLATFORM_PERMISSION_GROUPS_WRITE,
    PLATFORM_PERMISSION_USAGE_READ,
)


class _FallbackEnforcer:
    def __init__(self) -> None:
        self._policies: set[tuple[str, str, str]] = set()

    def clear_policy(self) -> None:
        self._policies.clear()

    def add_policy(self, sub: str, obj: str, act: str) -> None:
        self._policies.add((sub, obj, act))

    def enforce(self, sub: str, obj: str, act: str) -> bool:
        return (sub, obj, act) in self._policies


MODEL_TEXT = """
[request_definition]
r = sub, obj, act

[policy_definition]
p = sub, obj, act

[policy_effect]
e = some(where (p.eft == allow))

[matchers]
m = r.sub == p.sub && r.obj == p.obj && r.act == p.act
""".strip()


def _platform_action(permission: str) -> str:
    if permission.endswith(".read"):
        return "read"
    if permission.endswith(".write"):
        return "write"
    return "write"


class AuthorizationService:
    def __init__(self, config: AppConfig, store: Any) -> None:
        self._config = config
        self._store = store
        self._lock = asyncio.Lock()
        self._policy_revision = -1
        self._enforcer = self._new_enforcer()

    def _new_enforcer(self):
        if casbin is None:
            return _FallbackEnforcer()
        model = casbin.Model()
        model.load_model_from_text(MODEL_TEXT)
        return casbin.Enforcer(model, adapter=None)

    async def ensure_loaded(self) -> None:
        revision = await self._policy_revision_value()
        if revision == self._policy_revision:
            return
        async with self._lock:
            revision = await self._policy_revision_value()
            if revision == self._policy_revision:
                return
            await self._reload_policies(revision)

    async def _policy_revision_value(self) -> int:
        getter = getattr(self._store, "get_policy_revision", None)
        if getter is None:
            return 0
        return int(await getter())

    async def _reload_policies(self, revision: int) -> None:
        self._enforcer = self._new_enforcer()
        for policy in self._static_policies():
            self._enforcer.add_policy(*policy)

        integration_loader = getattr(self._store, "list_group_integration_policies", None)
        if integration_loader is not None:
            for row in await integration_loader():
                self._enforcer.add_policy(
                    f"group:{row['group_name']}",
                    f"integration:{row['upstream_id']}",
                    "call",
                )

        platform_loader = getattr(self._store, "list_group_platform_policies", None)
        if platform_loader is not None:
            for row in await platform_loader():
                permission = row["permission"]
                self._enforcer.add_policy(
                    f"group:{row['group_name']}",
                    f"platform:{permission}",
                    _platform_action(permission),
                )

        self._policy_revision = revision

    def _static_policies(self) -> Iterable[tuple[str, str, str]]:
        upstream_ids = [upstream.id for upstream in self._config.upstreams]
        for upstream_id in upstream_ids:
            yield (f"group:{GROUP_LEGACY_ADMIN}", f"integration:{upstream_id}", "call")
        for permission in ALL_PLATFORM_PERMISSIONS:
            action = _platform_action(permission)
            yield (f"group:{GROUP_LEGACY_ADMIN}", f"platform:{permission}", action)

    async def authorize_integration(self, principal: Optional[AuthenticatedPrincipal], upstream_id: str) -> bool:
        if principal is None:
            return False
        await self.ensure_loaded()
        target = f"integration:{upstream_id}"
        for subject in self._subjects_for_principal(principal):
            if self._enforcer.enforce(subject, target, "call"):
                return True
        return False

    async def authorize_platform(self, principal: Optional[AuthenticatedPrincipal], permission: str) -> bool:
        if principal is None:
            return False
        await self.ensure_loaded()
        target = f"platform:{permission}"
        action = _platform_action(permission)
        for subject in self._subjects_for_principal(principal):
            if self._enforcer.enforce(subject, target, action):
                return True
        return False

    def _subjects_for_principal(self, principal: AuthenticatedPrincipal) -> list[str]:
        subjects = [f"subject:{principal.subject}"]
        group_names = set(principal.group_names)
        if principal.role == "admin":
            group_names.add(GROUP_LEGACY_ADMIN)
        elif principal.role == "member":
            group_names.add(GROUP_LEGACY_MEMBER)
        elif principal.role == "viewer":
            group_names.add(GROUP_LEGACY_VIEWER)
        subjects.extend(f"group:{group_name}" for group_name in sorted(group_names))
        return subjects
