from __future__ import annotations

import asyncio
import html
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from aiohttp import web

from .auth import (
    AUTH_MODE_POSTGRES_API_KEYS,
    VALID_ROLES,
    AuthUnavailableError,
)
from .authorization import (
    PLATFORM_PERMISSION_GROUPS_READ,
    PLATFORM_PERMISSION_GROUPS_WRITE,
    PLATFORM_PERMISSION_IDENTITIES_READ,
    PLATFORM_PERMISSION_IDENTITIES_WRITE,
    PLATFORM_PERMISSION_USAGE_READ,
)
from .config import AppConfig
from .errors import GatewayHTTPError
from .gateway import Gateway
from .jsonrpc import make_error_response
from .logging import Logger
from .protocol import DEFAULT_HTTP_PROTOCOL_VERSION, is_supported_protocol_version
from .request_context import AuthenticatedPrincipal, RequestContext
from .telemetry import GatewayTelemetry


class HttpServer:
    def __init__(self, config: AppConfig, gateway: Gateway, logger: Logger, telemetry: GatewayTelemetry) -> None:
        self._config = config
        self._gateway = gateway
        self._logger = logger
        self._telemetry = telemetry
        self._rate_limit_state: Dict[str, tuple[float, int]] = {}
        self._rate_limit_lock = asyncio.Lock()
        self._rate_limit_gc_interval_seconds = 60.0
        self._rate_limit_entry_ttl_seconds = 300.0
        self._rate_limit_max_clients = 10000
        self._next_rate_limit_gc_at = 0.0

    def _rest_error(self, status: int, error: str, message: str, **fields: Any) -> web.Response:
        payload: Dict[str, Any] = {"error": error, "message": message}
        payload.update(fields)
        return web.json_response(payload, status=status)

    def _management_unavailable_response(self) -> web.Response:
        return self._rest_error(
            400,
            "Unavailable",
            "Management APIs require gateway.auth_mode to be postgres_api_keys.",
        )

    def _legacy_management_unavailable_response(self) -> web.Response:
        return self._rest_error(
            400,
            "Unavailable",
            "API-key management requires gateway.auth_mode to be postgres_api_keys.",
        )

    def _require_postgres_management(self) -> Optional[web.Response]:
        if self._gateway.auth_mode_requires_database():
            return None
        return self._management_unavailable_response()

    def _require_legacy_api_key_management(self) -> Optional[web.Response]:
        if self._config.gateway.auth_mode == AUTH_MODE_POSTGRES_API_KEYS:
            return None
        return self._legacy_management_unavailable_response()

    def _rest_forbidden(self, message: str) -> web.Response:
        return self._rest_error(403, "Forbidden", message)

    def _invalid_request_response(
        self,
        endpoint: str,
        message: str,
        **fields: Any,
    ) -> web.Response:
        return self._rest_error(400, "InvalidRequest", message)

    def _log_invalid_request_exception(self, endpoint: str, exc: BaseException, **fields: Any) -> None:
        self._logger.warn(
            "http_invalid_request",
            endpoint=endpoint,
            error_type=type(exc).__name__,
            error=str(exc) or type(exc).__name__,
            **fields,
        )

    def _log_http_error_exception(self, endpoint: str, exc: GatewayHTTPError) -> None:
        log_method = self._logger.warn if exc.status < 500 else self._logger.error
        log_method(
            "http_service_error",
            endpoint=endpoint,
            status=exc.status,
            error=exc.error,
            message=exc.message,
            **exc.fields,
        )

    def _gateway_http_error_response(self, endpoint: str, exc: GatewayHTTPError) -> web.Response:
        self._log_http_error_exception(endpoint, exc)
        return self._rest_error(exc.status, exc.error, exc.message, **exc.fields)

    @web.middleware
    async def _error_middleware(self, request: web.Request, handler):
        try:
            return await handler(request)
        except GatewayHTTPError as exc:
            return self._gateway_http_error_response(request.path, exc)
        except Exception as exc:  # noqa: BLE001
            self._logger.error(
                "http_unhandled_exception",
                endpoint=request.path,
                error_type=type(exc).__name__,
                error=str(exc) or type(exc).__name__,
            )
            if request.path == "/mcp":
                return web.json_response(make_error_response(None, -32603, "Internal error"), status=500)
            return self._rest_error(500, "InternalError", "Unexpected server error.")

    def _normalize_role(self, role: str) -> Optional[str]:
        normalized_role = role.strip().lower()
        if normalized_role not in VALID_ROLES:
            return None
        return normalized_role

    async def _require_platform_permission(
        self,
        request_context: RequestContext,
        endpoint: str,
        permission: str,
    ) -> Optional[web.Response]:
        allowed = await self._gateway.authorize_platform(request_context.principal, permission)
        if allowed:
            return None
        return self._forbidden_response(endpoint, permission)

    async def _parse_rest_json(self, request: web.Request) -> tuple[Optional[Dict[str, Any]], Optional[web.Response]]:
        payload, invalid_json = await self._parse_json_request(request)
        if invalid_json:
            return None, invalid_json
        if not isinstance(payload, dict):
            return None, self._rest_error(400, "InvalidRequest", "Expected a JSON object body.")
        return payload, None

    def _parse_iso_datetime(self, value: Any, field_name: str) -> Optional[datetime]:
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError(f"{field_name} must be an ISO-8601 string or null")
        normalized = value.strip()
        if not normalized:
            return None
        try:
            parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(f"{field_name} must be a valid ISO-8601 timestamp") from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    def _principal_profile(
        self,
        request_context: RequestContext,
        *,
        user: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        principal = request_context.principal
        assert principal is not None
        payload = {
            "subject": principal.subject,
            "issuer": principal.issuer,
            "display_name": principal.display_name,
            "email": principal.email,
            "groups": list(principal.group_names),
            "role": principal.role,
            "auth_scheme": principal.auth_scheme,
            "user_id": principal.user_id,
            "api_key_id": principal.api_key_id,
            "legacy_api_key_id": principal.legacy_api_key_id,
            "is_bootstrap_admin": principal.is_bootstrap_admin,
        }
        if user is not None:
            payload["user"] = user
        return payload

    async def _authenticate(
        self,
        request: web.Request,
        *,
        require_principal: bool = False,
    ) -> tuple[Optional[AuthenticatedPrincipal], Optional[web.Response]]:
        auth = request.headers.get("Authorization", "")
        token = auth.split(" ", 1)[1] if auth.startswith("Bearer ") else None
        try:
            principal = await self._gateway.authenticate_token(token)
        except AuthUnavailableError:
            return None, web.json_response(make_error_response(None, -32012, "Authentication backend unavailable"), status=503)
        if principal is not None:
            return principal, None
        if not require_principal and not self._gateway.auth_required():
            return None, None
        return None, web.json_response(make_error_response(None, -32010, "Unauthorized"), status=401)

    async def _authenticate_http_endpoint(
        self,
        request: web.Request,
        endpoint: str,
        *,
        require_principal: bool = False,
    ) -> tuple[Optional[AuthenticatedPrincipal], Optional[web.Response]]:
        principal, unauthorized = await self._authenticate(request, require_principal=require_principal)
        if not unauthorized:
            return principal, None
        accept = request.headers.get("Accept", "")
        if unauthorized.status != 401:
            message = "Authentication backend unavailable."
            if "text/html" in accept.lower():
                body = (
                    "<!doctype html>"
                    "<html><head><title>Authentication Unavailable</title></head>"
                    "<body>"
                    "<h1>Authentication Unavailable</h1>"
                    f"<p>{html.escape(message)}</p>"
                    "</body></html>"
                )
                return None, web.Response(text=body, status=unauthorized.status, content_type="text/html")
            return None, web.json_response(
                {
                    "error": "Unavailable",
                    "message": message,
                },
                status=unauthorized.status,
            )
        headers = {"WWW-Authenticate": 'Bearer realm="mcp-gateway"'}
        message = f"{endpoint} requires Authorization: Bearer <token>."
        hint = "Browsers do not send this header automatically. Use curl or an API client."
        if "text/html" in accept.lower():
            body = (
                "<!doctype html>"
                "<html><head><title>Authorization Required</title></head>"
                "<body>"
                "<h1>Authorization Required</h1>"
                f"<p>{html.escape(message)}</p>"
                f"<p>{html.escape(hint)}</p>"
                "</body></html>"
            )
            return None, web.Response(text=body, status=unauthorized.status, content_type="text/html", headers=headers)
        return None, web.json_response(
            {
                "error": "Unauthorized" if unauthorized.status == 401 else "Unavailable",
                "message": message,
                "hint": hint,
            },
            status=unauthorized.status,
            headers=headers,
        )

    def _forbidden_response(self, endpoint: str, permission: Optional[str] = None) -> web.Response:
        message = "Blocked by gateway policy: permission not allowed"
        data = {"category": "policy_denied", "endpoint": endpoint, "retryable": False}
        if permission is not None:
            data["permission"] = permission
            message = f"Blocked by gateway policy: permission '{permission}' not allowed"
        return web.json_response(
            make_error_response(
                None,
                -32001,
                message,
                data=data,
            ),
            status=403,
        )

    def _trusted_proxy(self, request: web.Request) -> bool:
        trusted = set(self._config.gateway.trusted_proxies)
        remote = request.remote or ""
        return remote in trusted

    def _client_id(self, request: web.Request) -> str:
        trusted_proxy = self._trusted_proxy(request)
        explicit = request.headers.get("X-Client-Id")
        if explicit and trusted_proxy:
            return explicit
        if trusted_proxy:
            forwarded_for = request.headers.get("X-Forwarded-For", "")
            if forwarded_for:
                return forwarded_for.split(",")[0].strip()
        return request.remote or "unknown"

    def _prune_rate_limit_state(self, now: float) -> None:
        if now < self._next_rate_limit_gc_at and len(self._rate_limit_state) <= self._rate_limit_max_clients:
            return
        cutoff = now - self._rate_limit_entry_ttl_seconds
        stale_clients = [client_id for client_id, (window_start, _) in self._rate_limit_state.items() if window_start < cutoff]
        for client_id in stale_clients:
            self._rate_limit_state.pop(client_id, None)
        if len(self._rate_limit_state) > self._rate_limit_max_clients:
            overflow = len(self._rate_limit_state) - self._rate_limit_max_clients
            oldest = sorted(self._rate_limit_state.items(), key=lambda item: item[1][0])[:overflow]
            for client_id, _ in oldest:
                self._rate_limit_state.pop(client_id, None)
        self._next_rate_limit_gc_at = now + self._rate_limit_gc_interval_seconds

    def _rate_limit_scope_key(self, request_context: RequestContext) -> str:
        principal = request_context.principal
        if principal is not None:
            if principal.api_key_id:
                return f"api_key:{principal.api_key_id}"
            if principal.user_id:
                return f"user:{principal.user_id}"
            return f"subject:{principal.auth_scheme}:{principal.subject}"
        return f"client:{request_context.client_id or 'anonymous'}"

    async def _fallback_rate_limit(self, scope_key: str) -> Optional[web.Response]:
        limit = max(1, self._config.gateway.rate_limit_per_minute)
        now = time.monotonic()
        async with self._rate_limit_lock:
            self._prune_rate_limit_state(now)
            window_start, count = self._rate_limit_state.get(scope_key, (now, 0))
            if now - window_start >= 60:
                window_start = now
                count = 0
            count += 1
            self._rate_limit_state[scope_key] = (window_start, count)
            if count <= limit:
                return None
        retry_after = max(1, int(60 - (now - window_start)))
        return web.json_response(
            make_error_response(None, -32029, "Rate limit exceeded"),
            status=429,
            headers={"Retry-After": str(retry_after)},
        )

    async def _rate_limit(self, request_context: RequestContext) -> Optional[web.Response]:
        scope_key = self._rate_limit_scope_key(request_context)
        limit = max(1, self._config.gateway.rate_limit_per_minute)
        if self._gateway.store_available():
            try:
                result = await self._gateway.consume_rate_limit(scope_key=scope_key, limit=limit, window_seconds=60)
            except Exception as exc:  # noqa: BLE001
                self._logger.warn("rate_limit_store_unavailable", scope_key=scope_key, error=str(exc))
            else:
                if bool(result["allowed"]):
                    return None
                return web.json_response(
                    make_error_response(None, -32029, "Rate limit exceeded"),
                    status=429,
                    headers={"Retry-After": str(int(result["retry_after_seconds"]))},
                )
        return await self._fallback_rate_limit(scope_key)

    async def _preflight_request(
        self,
        request: web.Request,
        endpoint: Optional[str] = None,
        *,
        require_auth: bool = True,
        strict_auth: bool = False,
    ) -> tuple[Optional[RequestContext], Optional[web.Response]]:
        principal: Optional[AuthenticatedPrincipal] = None
        if require_auth:
            if endpoint:
                principal, unauthorized = await self._authenticate_http_endpoint(
                    request,
                    endpoint,
                    require_principal=strict_auth,
                )
            else:
                principal, unauthorized = await self._authenticate(request, require_principal=strict_auth)
            if unauthorized:
                return None, unauthorized
        client_id = self._client_id(request)
        request_context = RequestContext(client_id=client_id, principal=principal)
        blocked = await self._rate_limit(request_context)
        if blocked:
            return None, blocked
        return request_context, None

    async def _parse_json_request(self, request: web.Request) -> tuple[Optional[Any], Optional[web.Response]]:
        try:
            return await request.json(), None
        except json.JSONDecodeError:
            return None, web.json_response(make_error_response(None, -32700, "Invalid JSON"), status=400)

    @staticmethod
    def _jsonrpc_http_response(
        payload: Any,
        status: int = 200,
        *,
        protocol_version: Optional[str] = None,
    ) -> web.Response:
        headers = {}
        if protocol_version is not None:
            headers["MCP-Protocol-Version"] = protocol_version
        return web.json_response(payload, status=status, headers=headers)

    @staticmethod
    def _is_jsonrpc_response_message(payload: dict[str, Any]) -> bool:
        if payload.get("jsonrpc") != "2.0":
            return False
        return "method" not in payload and ("result" in payload or "error" in payload)

    async def _handle_single_message(
        self,
        payload: Any,
        request_context: RequestContext,
    ) -> tuple[Optional[dict[str, Any]], Optional[web.Response]]:
        if not isinstance(payload, dict):
            return None, self._jsonrpc_http_response(make_error_response(None, -32600, "Invalid Request"), status=400)
        if self._is_jsonrpc_response_message(payload):
            return None, None
        result = await self._gateway.handle(payload, request_context)
        if payload.get("id") is None:
            return None, None
        return result.payload, None

    async def _handle_batch_message(
        self,
        payloads: list[Any],
        request_context: RequestContext,
        *,
        protocol_version: Optional[str] = None,
    ) -> web.Response:
        if not payloads:
            return self._jsonrpc_http_response(make_error_response(None, -32600, "Invalid Request"), status=400)
        responses: list[dict[str, Any]] = []
        for item in payloads:
            payload, invalid = await self._handle_single_message(item, request_context)
            if invalid is not None:
                body = invalid.body
                assert body is not None
                responses.append(json.loads(body.decode("utf-8")))
                continue
            if payload is not None:
                responses.append(payload)
        # JSON-RPC notifications do not produce response bodies, so a batch that is
        # entirely notifications is surfaced as HTTP 202 with no JSON payload.
        if not responses:
            if protocol_version is None:
                return web.Response(status=202)
            return web.Response(status=202, headers={"MCP-Protocol-Version": protocol_version})
        return self._jsonrpc_http_response(responses, protocol_version=protocol_version)

    def _effective_protocol_version(self, request: web.Request, payload: Any) -> tuple[Optional[str], Optional[web.Response]]:
        header_version = request.headers.get("MCP-Protocol-Version") or request.headers.get("mcp-protocol-version")
        if header_version is not None and not is_supported_protocol_version(header_version):
            return None, web.Response(status=400, text="Unsupported MCP-Protocol-Version header.")

        if isinstance(payload, dict) and payload.get("method") == "initialize":
            params = payload.get("params")
            requested_version = params.get("protocolVersion") if isinstance(params, dict) else None
            # `initialize` negotiates protocol version from the JSON-RPC payload.
            # Later requests rely on the transport header instead.
            if is_supported_protocol_version(requested_version):
                return requested_version, None
            return None, None

        return header_version or DEFAULT_HTTP_PROTOCOL_VERSION, None

    async def health_handler(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok", "service": "mcp-gateway", **self._gateway.status_snapshot()})

    async def ready_handler(self, request: web.Request) -> web.Response:
        ready = self._gateway.is_ready()
        status = 200 if ready else 503
        return web.json_response({"ready": ready, **self._gateway.status_snapshot()}, status=status)

    async def tools_handler(self, request: web.Request) -> web.Response:
        _, blocked = await self._preflight_request(
            request,
            endpoint="/tools",
            require_auth=not self._config.gateway.public_tools_catalog,
        )
        if blocked:
            return blocked
        payload = await self._gateway.tools_catalog()
        return web.json_response(payload)

    async def metrics_handler(self, request: web.Request) -> web.Response:
        body = self._telemetry.render_prometheus()
        return web.Response(body=body, headers={"Content-Type": self._telemetry.prometheus_content_type})

    async def me_handler(self, request: web.Request) -> web.Response:
        unavailable = self._require_postgres_management()
        if unavailable:
            return unavailable
        request_context, blocked = await self._preflight_request(request, endpoint="/v1/me", strict_auth=True)
        if blocked:
            return blocked
        assert request_context is not None
        user = None
        if request_context.principal and request_context.principal.user_id:
            user = await self._gateway.get_user_by_id(request_context.principal.user_id)
        return web.json_response(self._principal_profile(request_context, user=user))

    async def my_api_keys_list_handler(self, request: web.Request) -> web.Response:
        unavailable = self._require_legacy_api_key_management()
        if unavailable:
            return unavailable
        request_context, blocked = await self._preflight_request(request, endpoint="/v1/me/api-keys", strict_auth=True)
        if blocked:
            return blocked
        assert request_context is not None
        principal = request_context.principal
        assert principal is not None
        if not principal.user_id:
            return self._rest_error(400, "InvalidPrincipal", "This principal is not linked to a managed user.")
        items = await self._gateway.list_api_keys(user_id=principal.user_id)
        return web.json_response({"items": items})

    async def my_api_keys_create_handler(self, request: web.Request) -> web.Response:
        unavailable = self._require_legacy_api_key_management()
        if unavailable:
            return unavailable
        request_context, blocked = await self._preflight_request(request, endpoint="/v1/me/api-keys", strict_auth=True)
        if blocked:
            return blocked
        assert request_context is not None
        principal = request_context.principal
        assert principal is not None
        if not principal.user_id:
            return self._rest_error(400, "InvalidPrincipal", "This principal is not linked to a managed user.")
        body, invalid_body = await self._parse_rest_json(request)
        if invalid_body:
            return invalid_body
        assert body is not None
        label = body.get("label", "default")
        if not isinstance(label, str) or not label.strip():
            return self._rest_error(400, "InvalidRequest", "label must be a non-empty string.")
        try:
            expires_at = self._parse_iso_datetime(body.get("expires_at"), "expires_at")
        except ValueError as exc:
            self._log_invalid_request_exception("/v1/me/api-keys", exc, field="expires_at")
            return self._invalid_request_response(
                "/v1/me/api-keys",
                "expires_at must be a valid ISO-8601 timestamp",
                field="expires_at",
            )
        try:
            issued = await self._gateway.issue_api_key_for_user(
                user_id=principal.user_id,
                key_name=label.strip(),
                expires_at=expires_at,
            )
        except ValueError as exc:
            self._log_invalid_request_exception("/v1/me/api-keys", exc, operation="issue_api_key_for_user")
            return self._invalid_request_response(
                "/v1/me/api-keys",
                "Unable to issue API key for this request.",
                operation="issue_api_key_for_user",
            )
        return web.json_response(issued, status=201)

    async def my_api_keys_revoke_handler(self, request: web.Request) -> web.Response:
        unavailable = self._require_legacy_api_key_management()
        if unavailable:
            return unavailable
        request_context, blocked = await self._preflight_request(request, endpoint="/v1/me/api-keys", strict_auth=True)
        if blocked:
            return blocked
        assert request_context is not None
        principal = request_context.principal
        assert principal is not None
        if not principal.user_id:
            return self._rest_error(400, "InvalidPrincipal", "This principal is not linked to a managed user.")
        api_key_id = request.match_info.get("key_id", "").strip()
        if not api_key_id:
            return self._rest_error(400, "InvalidRequest", "key_id is required.")
        revoked = await self._gateway.revoke_api_key(api_key_id, user_id=principal.user_id)
        if revoked is None:
            return self._rest_error(404, "NotFound", "API key not found.")
        return web.json_response(revoked)

    async def admin_users_list_handler(self, request: web.Request) -> web.Response:
        unavailable = self._require_legacy_api_key_management()
        if unavailable:
            return unavailable
        request_context, blocked = await self._preflight_request(request, endpoint="/v1/admin/users", strict_auth=True)
        if blocked:
            return blocked
        assert request_context is not None
        forbidden = await self._require_platform_permission(request_context, "/v1/admin/users", PLATFORM_PERMISSION_IDENTITIES_READ)
        if forbidden:
            return forbidden
        items = await self._gateway.list_users()
        return web.json_response({"items": items})

    async def admin_users_create_handler(self, request: web.Request) -> web.Response:
        unavailable = self._require_legacy_api_key_management()
        if unavailable:
            return unavailable
        request_context, blocked = await self._preflight_request(request, endpoint="/v1/admin/users", strict_auth=True)
        if blocked:
            return blocked
        assert request_context is not None
        forbidden = await self._require_platform_permission(
            request_context,
            "/v1/admin/users",
            PLATFORM_PERMISSION_IDENTITIES_WRITE,
        )
        if forbidden:
            return forbidden
        body, invalid_body = await self._parse_rest_json(request)
        if invalid_body:
            return invalid_body
        assert body is not None
        subject = body.get("subject")
        display_name = body.get("display_name")
        role = body.get("role")
        issue_api_key = body.get("issue_api_key", False)
        key_label = body.get("key_label") or "default"
        if not isinstance(subject, str) or not subject.strip():
            return self._rest_error(400, "InvalidRequest", "subject must be a non-empty string.")
        if display_name is not None and (not isinstance(display_name, str) or not display_name.strip()):
            return self._rest_error(400, "InvalidRequest", "display_name must be a non-empty string when provided.")
        normalized_role = None
        if role is not None:
            if not isinstance(role, str) or not role.strip():
                return self._rest_error(400, "InvalidRequest", "role must be a non-empty string when provided.")
            normalized_role = self._normalize_role(role)
            if normalized_role is None:
                return self._invalid_request_response(
                    "/v1/admin/users",
                    "role, when provided, must be admin",
                    field="role",
                )
        if not isinstance(issue_api_key, bool):
            return self._rest_error(400, "InvalidRequest", "issue_api_key must be a boolean.")
        if not isinstance(key_label, str) or not key_label.strip():
            return self._rest_error(400, "InvalidRequest", "key_label must be a non-empty string when provided.")
        try:
            expires_at = self._parse_iso_datetime(body.get("expires_at"), "expires_at")
        except ValueError as exc:
            self._log_invalid_request_exception("/v1/admin/users", exc, field="expires_at")
            return self._invalid_request_response(
                "/v1/admin/users",
                "expires_at must be a valid ISO-8601 timestamp",
                field="expires_at",
            )
        try:
            user = await self._gateway.create_user(subject=subject, display_name=display_name, role=normalized_role)
        except ValueError as exc:
            self._log_invalid_request_exception("/v1/admin/users", exc, operation="create_user")
            return self._invalid_request_response(
                "/v1/admin/users",
                "Invalid user creation request.",
                operation="create_user",
            )
        if user is None:
            return self._rest_error(409, "Conflict", "A user with that subject already exists.")
        response_payload: Dict[str, Any] = {"user": user}
        if issue_api_key:
            issued = await self._gateway.issue_api_key_for_user(
                user_id=user["id"],
                key_name=key_label.strip(),
                expires_at=expires_at,
            )
            response_payload["issued_api_key"] = issued
        return web.json_response(response_payload, status=201)

    async def admin_users_update_handler(self, request: web.Request) -> web.Response:
        unavailable = self._require_legacy_api_key_management()
        if unavailable:
            return unavailable
        request_context, blocked = await self._preflight_request(request, endpoint="/v1/admin/users", strict_auth=True)
        if blocked:
            return blocked
        assert request_context is not None
        forbidden = await self._require_platform_permission(
            request_context,
            "/v1/admin/users",
            PLATFORM_PERMISSION_IDENTITIES_WRITE,
        )
        if forbidden:
            return forbidden
        user_id = request.match_info.get("user_id", "").strip()
        if not user_id:
            return self._rest_error(400, "InvalidRequest", "user_id is required.")
        body, invalid_body = await self._parse_rest_json(request)
        if invalid_body:
            return invalid_body
        assert body is not None
        display_name = body.get("display_name")
        role_present = "role" in body
        role = body.get("role")
        is_active = body.get("is_active")
        if display_name is not None and (not isinstance(display_name, str) or not display_name.strip()):
            return self._rest_error(400, "InvalidRequest", "display_name must be a non-empty string when provided.")
        normalized_role = None
        if role_present and role is not None:
            if not isinstance(role, str) or not role.strip():
                return self._rest_error(400, "InvalidRequest", "role must be a non-empty string when provided.")
            normalized_role = self._normalize_role(role)
            if normalized_role is None:
                return self._invalid_request_response(
                    "/v1/admin/users",
                    "role, when provided, must be admin",
                    field="role",
                )
        if is_active is not None and not isinstance(is_active, bool):
            return self._rest_error(400, "InvalidRequest", "is_active must be a boolean when provided.")
        if display_name is None and not role_present and is_active is None:
            return self._rest_error(400, "InvalidRequest", "At least one updatable field is required.")
        try:
            user = await self._gateway.update_user(
                user_id,
                display_name=display_name,
                role=normalized_role,
                role_provided=role_present,
                is_active=is_active,
            )
        except ValueError as exc:
            self._log_invalid_request_exception("/v1/admin/users", exc, operation="update_user")
            return self._invalid_request_response(
                "/v1/admin/users",
                "Invalid user update request.",
                operation="update_user",
            )
        if user is None:
            return self._rest_error(404, "NotFound", "User not found.")
        return web.json_response(user)

    async def admin_identities_list_handler(self, request: web.Request) -> web.Response:
        unavailable = self._require_postgres_management()
        if unavailable:
            return unavailable
        request_context, blocked = await self._preflight_request(request, endpoint="/v1/admin/identities", strict_auth=True)
        if blocked:
            return blocked
        assert request_context is not None
        forbidden = await self._require_platform_permission(
            request_context,
            "/v1/admin/identities",
            PLATFORM_PERMISSION_IDENTITIES_READ,
        )
        if forbidden:
            return forbidden
        items = await self._gateway.list_identities()
        return web.json_response({"items": items})

    async def admin_identity_put_handler(self, request: web.Request) -> web.Response:
        unavailable = self._require_postgres_management()
        if unavailable:
            return unavailable
        request_context, blocked = await self._preflight_request(
            request,
            endpoint="/v1/admin/identities",
            strict_auth=True,
        )
        if blocked:
            return blocked
        assert request_context is not None
        forbidden = await self._require_platform_permission(
            request_context,
            "/v1/admin/identities",
            PLATFORM_PERMISSION_IDENTITIES_WRITE,
        )
        if forbidden:
            return forbidden
        subject = request.match_info.get("subject", "").strip()
        if not subject:
            return self._rest_error(400, "InvalidRequest", "subject is required.")
        body, invalid_body = await self._parse_rest_json(request)
        if invalid_body:
            return invalid_body
        assert body is not None
        display_name = body.get("display_name")
        email = body.get("email")
        is_active = body.get("is_active", True)
        if display_name is not None and (not isinstance(display_name, str) or not display_name.strip()):
            return self._rest_error(400, "InvalidRequest", "display_name must be a non-empty string when provided.")
        if email is not None and (not isinstance(email, str) or not email.strip()):
            return self._rest_error(400, "InvalidRequest", "email must be a non-empty string when provided.")
        if not isinstance(is_active, bool):
            return self._rest_error(400, "InvalidRequest", "is_active must be a boolean.")
        identity = await self._gateway.put_identity(
            subject=subject,
            display_name=display_name,
            email=email,
            is_active=is_active,
        )
        return web.json_response(identity)

    async def admin_identity_patch_handler(self, request: web.Request) -> web.Response:
        unavailable = self._require_postgres_management()
        if unavailable:
            return unavailable
        request_context, blocked = await self._preflight_request(
            request,
            endpoint="/v1/admin/identities",
            strict_auth=True,
        )
        if blocked:
            return blocked
        assert request_context is not None
        forbidden = await self._require_platform_permission(
            request_context,
            "/v1/admin/identities",
            PLATFORM_PERMISSION_IDENTITIES_WRITE,
        )
        if forbidden:
            return forbidden
        subject = request.match_info.get("subject", "").strip()
        if not subject:
            return self._rest_error(400, "InvalidRequest", "subject is required.")
        body, invalid_body = await self._parse_rest_json(request)
        if invalid_body:
            return invalid_body
        assert body is not None
        display_name = body.get("display_name")
        email = body.get("email")
        is_active = body.get("is_active")
        if display_name is not None and (not isinstance(display_name, str) or not display_name.strip()):
            return self._rest_error(400, "InvalidRequest", "display_name must be a non-empty string when provided.")
        if email is not None and (not isinstance(email, str) or not email.strip()):
            return self._rest_error(400, "InvalidRequest", "email must be a non-empty string when provided.")
        if is_active is not None and not isinstance(is_active, bool):
            return self._rest_error(400, "InvalidRequest", "is_active must be a boolean when provided.")
        identity = await self._gateway.patch_identity(
            subject,
            display_name=display_name,
            email=email,
            is_active=is_active,
        )
        if identity is None:
            return self._rest_error(404, "NotFound", "Identity not found.")
        return web.json_response(identity)

    async def admin_integrations_list_handler(self, request: web.Request) -> web.Response:
        unavailable = self._require_postgres_management()
        if unavailable:
            return unavailable
        request_context, blocked = await self._preflight_request(
            request,
            endpoint="/v1/admin/integrations",
            strict_auth=True,
        )
        if blocked:
            return blocked
        assert request_context is not None
        forbidden = await self._require_platform_permission(
            request_context,
            "/v1/admin/integrations",
            PLATFORM_PERMISSION_GROUPS_READ,
        )
        if forbidden:
            return forbidden
        items = await self._gateway.list_integrations()
        return web.json_response({"items": items})

    async def admin_groups_list_handler(self, request: web.Request) -> web.Response:
        unavailable = self._require_postgres_management()
        if unavailable:
            return unavailable
        request_context, blocked = await self._preflight_request(request, endpoint="/v1/admin/groups", strict_auth=True)
        if blocked:
            return blocked
        assert request_context is not None
        forbidden = await self._require_platform_permission(request_context, "/v1/admin/groups", PLATFORM_PERMISSION_GROUPS_READ)
        if forbidden:
            return forbidden
        items = await self._gateway.list_groups()
        return web.json_response({"items": items})

    async def admin_groups_create_handler(self, request: web.Request) -> web.Response:
        unavailable = self._require_postgres_management()
        if unavailable:
            return unavailable
        request_context, blocked = await self._preflight_request(request, endpoint="/v1/admin/groups", strict_auth=True)
        if blocked:
            return blocked
        assert request_context is not None
        forbidden = await self._require_platform_permission(request_context, "/v1/admin/groups", PLATFORM_PERMISSION_GROUPS_WRITE)
        if forbidden:
            return forbidden
        body, invalid_body = await self._parse_rest_json(request)
        if invalid_body:
            return invalid_body
        assert body is not None
        name = body.get("name")
        description = body.get("description")
        if not isinstance(name, str) or not name.strip():
            return self._rest_error(400, "InvalidRequest", "name must be a non-empty string.")
        if description is not None and not isinstance(description, str):
            return self._rest_error(400, "InvalidRequest", "description must be a string when provided.")
        try:
            group = await self._gateway.create_group(name=name.strip(), description=description)
        except ValueError as exc:
            self._log_invalid_request_exception("/v1/admin/groups", exc, operation="create_group")
            return self._invalid_request_response("/v1/admin/groups", str(exc), operation="create_group")
        if group is None:
            return self._rest_error(409, "Conflict", "A group with that name already exists.")
        return web.json_response(group, status=201)

    async def admin_groups_update_handler(self, request: web.Request) -> web.Response:
        unavailable = self._require_postgres_management()
        if unavailable:
            return unavailable
        request_context, blocked = await self._preflight_request(request, endpoint="/v1/admin/groups", strict_auth=True)
        if blocked:
            return blocked
        assert request_context is not None
        forbidden = await self._require_platform_permission(request_context, "/v1/admin/groups", PLATFORM_PERMISSION_GROUPS_WRITE)
        if forbidden:
            return forbidden
        group_id = request.match_info.get("group_id", "").strip()
        if not group_id:
            return self._rest_error(400, "InvalidRequest", "group_id is required.")
        body, invalid_body = await self._parse_rest_json(request)
        if invalid_body:
            return invalid_body
        assert body is not None
        name = body.get("name")
        description = body.get("description")
        if name is not None and (not isinstance(name, str) or not name.strip()):
            return self._rest_error(400, "InvalidRequest", "name must be a non-empty string when provided.")
        if description is not None and not isinstance(description, str):
            return self._rest_error(400, "InvalidRequest", "description must be a string when provided.")
        try:
            group = await self._gateway.update_group(group_id, name=name, description=description)
        except ValueError as exc:
            self._log_invalid_request_exception("/v1/admin/groups", exc, operation="update_group")
            return self._invalid_request_response("/v1/admin/groups", str(exc), operation="update_group")
        if group is None:
            return self._rest_error(404, "NotFound", "Group not found.")
        return web.json_response(group)

    async def admin_groups_delete_handler(self, request: web.Request) -> web.Response:
        unavailable = self._require_postgres_management()
        if unavailable:
            return unavailable
        request_context, blocked = await self._preflight_request(request, endpoint="/v1/admin/groups", strict_auth=True)
        if blocked:
            return blocked
        assert request_context is not None
        forbidden = await self._require_platform_permission(request_context, "/v1/admin/groups", PLATFORM_PERMISSION_GROUPS_WRITE)
        if forbidden:
            return forbidden
        group_id = request.match_info.get("group_id", "").strip()
        if not group_id:
            return self._rest_error(400, "InvalidRequest", "group_id is required.")
        deleted = await self._gateway.delete_group(group_id)
        if not deleted:
            return self._rest_error(404, "NotFound", "Group not found.")
        return web.json_response({"deleted": True})

    async def admin_group_members_add_handler(self, request: web.Request) -> web.Response:
        unavailable = self._require_postgres_management()
        if unavailable:
            return unavailable
        request_context, blocked = await self._preflight_request(
            request,
            endpoint="/v1/admin/groups/members",
            strict_auth=True,
        )
        if blocked:
            return blocked
        assert request_context is not None
        forbidden = await self._require_platform_permission(
            request_context,
            "/v1/admin/groups/members",
            PLATFORM_PERMISSION_GROUPS_WRITE,
        )
        if forbidden:
            return forbidden
        group_id = request.match_info.get("group_id", "").strip()
        if not group_id:
            return self._rest_error(400, "InvalidRequest", "group_id is required.")
        body, invalid_body = await self._parse_rest_json(request)
        if invalid_body:
            return invalid_body
        assert body is not None
        subject = body.get("subject")
        if not isinstance(subject, str) or not subject.strip():
            return self._rest_error(400, "InvalidRequest", "subject must be a non-empty string.")
        membership = await self._gateway.add_group_member(group_id, subject=subject.strip())
        return web.json_response(membership, status=201)

    async def admin_group_members_delete_handler(self, request: web.Request) -> web.Response:
        unavailable = self._require_postgres_management()
        if unavailable:
            return unavailable
        request_context, blocked = await self._preflight_request(
            request,
            endpoint="/v1/admin/groups/members",
            strict_auth=True,
        )
        if blocked:
            return blocked
        assert request_context is not None
        forbidden = await self._require_platform_permission(
            request_context,
            "/v1/admin/groups/members",
            PLATFORM_PERMISSION_GROUPS_WRITE,
        )
        if forbidden:
            return forbidden
        group_id = request.match_info.get("group_id", "").strip()
        subject = request.match_info.get("subject", "").strip()
        if not group_id or not subject:
            return self._rest_error(400, "InvalidRequest", "group_id and subject are required.")
        deleted = await self._gateway.remove_group_member(group_id, subject=subject)
        if not deleted:
            return self._rest_error(404, "NotFound", "Group membership not found.")
        return web.json_response({"deleted": True})

    async def admin_group_integration_grants_list_handler(self, request: web.Request) -> web.Response:
        unavailable = self._require_postgres_management()
        if unavailable:
            return unavailable
        request_context, blocked = await self._preflight_request(
            request,
            endpoint="/v1/admin/groups/integration-grants",
            strict_auth=True,
        )
        if blocked:
            return blocked
        assert request_context is not None
        forbidden = await self._require_platform_permission(
            request_context,
            "/v1/admin/groups/integration-grants",
            PLATFORM_PERMISSION_GROUPS_READ,
        )
        if forbidden:
            return forbidden
        group_id = request.match_info.get("group_id", "").strip()
        if not group_id:
            return self._rest_error(400, "InvalidRequest", "group_id is required.")
        items = await self._gateway.list_group_integration_grants(group_id)
        return web.json_response({"items": items})

    async def admin_group_integration_grants_create_handler(self, request: web.Request) -> web.Response:
        unavailable = self._require_postgres_management()
        if unavailable:
            return unavailable
        request_context, blocked = await self._preflight_request(
            request,
            endpoint="/v1/admin/groups/integration-grants",
            strict_auth=True,
        )
        if blocked:
            return blocked
        assert request_context is not None
        forbidden = await self._require_platform_permission(
            request_context,
            "/v1/admin/groups/integration-grants",
            PLATFORM_PERMISSION_GROUPS_WRITE,
        )
        if forbidden:
            return forbidden
        group_id = request.match_info.get("group_id", "").strip()
        if not group_id:
            return self._rest_error(400, "InvalidRequest", "group_id is required.")
        body, invalid_body = await self._parse_rest_json(request)
        if invalid_body:
            return invalid_body
        assert body is not None
        upstream_id = body.get("upstream_id")
        if not isinstance(upstream_id, str) or not upstream_id.strip():
            return self._rest_error(400, "InvalidRequest", "upstream_id must be a non-empty string.")
        try:
            grant = await self._gateway.add_group_integration_grant(group_id, upstream_id=upstream_id.strip())
        except ValueError as exc:
            self._log_invalid_request_exception(
                "/v1/admin/groups/integration-grants",
                exc,
                operation="add_group_integration_grant",
            )
            return self._invalid_request_response(
                "/v1/admin/groups/integration-grants",
                str(exc),
                operation="add_group_integration_grant",
            )
        return web.json_response(grant, status=201)

    async def admin_group_integration_grants_delete_handler(self, request: web.Request) -> web.Response:
        unavailable = self._require_postgres_management()
        if unavailable:
            return unavailable
        request_context, blocked = await self._preflight_request(
            request,
            endpoint="/v1/admin/groups/integration-grants",
            strict_auth=True,
        )
        if blocked:
            return blocked
        assert request_context is not None
        forbidden = await self._require_platform_permission(
            request_context,
            "/v1/admin/groups/integration-grants",
            PLATFORM_PERMISSION_GROUPS_WRITE,
        )
        if forbidden:
            return forbidden
        group_id = request.match_info.get("group_id", "").strip()
        upstream_id = request.match_info.get("upstream_id", "").strip()
        if not group_id or not upstream_id:
            return self._rest_error(400, "InvalidRequest", "group_id and upstream_id are required.")
        deleted = await self._gateway.remove_group_integration_grant(group_id, upstream_id=upstream_id)
        if not deleted:
            return self._rest_error(404, "NotFound", "Integration grant not found.")
        return web.json_response({"deleted": True})

    async def admin_group_platform_grants_list_handler(self, request: web.Request) -> web.Response:
        unavailable = self._require_postgres_management()
        if unavailable:
            return unavailable
        request_context, blocked = await self._preflight_request(
            request,
            endpoint="/v1/admin/groups/platform-grants",
            strict_auth=True,
        )
        if blocked:
            return blocked
        assert request_context is not None
        forbidden = await self._require_platform_permission(
            request_context,
            "/v1/admin/groups/platform-grants",
            PLATFORM_PERMISSION_GROUPS_READ,
        )
        if forbidden:
            return forbidden
        group_id = request.match_info.get("group_id", "").strip()
        if not group_id:
            return self._rest_error(400, "InvalidRequest", "group_id is required.")
        items = await self._gateway.list_group_platform_grants(group_id)
        return web.json_response({"items": items})

    async def admin_group_platform_grants_create_handler(self, request: web.Request) -> web.Response:
        unavailable = self._require_postgres_management()
        if unavailable:
            return unavailable
        request_context, blocked = await self._preflight_request(
            request,
            endpoint="/v1/admin/groups/platform-grants",
            strict_auth=True,
        )
        if blocked:
            return blocked
        assert request_context is not None
        forbidden = await self._require_platform_permission(
            request_context,
            "/v1/admin/groups/platform-grants",
            PLATFORM_PERMISSION_GROUPS_WRITE,
        )
        if forbidden:
            return forbidden
        group_id = request.match_info.get("group_id", "").strip()
        if not group_id:
            return self._rest_error(400, "InvalidRequest", "group_id is required.")
        body, invalid_body = await self._parse_rest_json(request)
        if invalid_body:
            return invalid_body
        assert body is not None
        permission = body.get("permission")
        if not isinstance(permission, str) or not permission.strip():
            return self._rest_error(400, "InvalidRequest", "permission must be a non-empty string.")
        try:
            grant = await self._gateway.add_group_platform_grant(group_id, permission=permission.strip())
        except ValueError as exc:
            self._log_invalid_request_exception(
                "/v1/admin/groups/platform-grants",
                exc,
                operation="add_group_platform_grant",
            )
            return self._invalid_request_response(
                "/v1/admin/groups/platform-grants",
                str(exc),
                operation="add_group_platform_grant",
            )
        return web.json_response(grant, status=201)

    async def admin_group_platform_grants_delete_handler(self, request: web.Request) -> web.Response:
        unavailable = self._require_postgres_management()
        if unavailable:
            return unavailable
        request_context, blocked = await self._preflight_request(
            request,
            endpoint="/v1/admin/groups/platform-grants",
            strict_auth=True,
        )
        if blocked:
            return blocked
        assert request_context is not None
        forbidden = await self._require_platform_permission(
            request_context,
            "/v1/admin/groups/platform-grants",
            PLATFORM_PERMISSION_GROUPS_WRITE,
        )
        if forbidden:
            return forbidden
        group_id = request.match_info.get("group_id", "").strip()
        permission = request.match_info.get("permission", "").strip()
        if not group_id or not permission:
            return self._rest_error(400, "InvalidRequest", "group_id and permission are required.")
        deleted = await self._gateway.remove_group_platform_grant(group_id, permission=permission)
        if not deleted:
            return self._rest_error(404, "NotFound", "Platform grant not found.")
        return web.json_response({"deleted": True})

    async def admin_usage_handler(self, request: web.Request) -> web.Response:
        unavailable = self._require_postgres_management()
        if unavailable:
            return unavailable
        request_context, blocked = await self._preflight_request(request, endpoint="/v1/admin/usage", strict_auth=True)
        if blocked:
            return blocked
        assert request_context is not None
        forbidden = await self._require_platform_permission(request_context, "/v1/admin/usage", PLATFORM_PERMISSION_USAGE_READ)
        if forbidden:
            return forbidden
        group_by = request.query.get("group_by", "subject")
        if group_by not in {"subject", "integration", "api_key", "user"}:
            return self._rest_error(400, "InvalidRequest", "group_by must be one of: subject, integration, api_key.")
        try:
            from_timestamp = self._parse_iso_datetime(request.query.get("from"), "from")
        except ValueError as exc:
            self._log_invalid_request_exception("/v1/admin/usage", exc, field="from")
            return self._invalid_request_response(
                "/v1/admin/usage",
                "from must be a valid ISO-8601 timestamp",
                field="from",
            )
        try:
            to_timestamp = self._parse_iso_datetime(request.query.get("to"), "to")
        except ValueError as exc:
            self._log_invalid_request_exception("/v1/admin/usage", exc, field="to")
            return self._invalid_request_response(
                "/v1/admin/usage",
                "to must be a valid ISO-8601 timestamp",
                field="to",
            )
        try:
            items = await self._gateway.usage_summary(
                group_by=group_by,
                from_timestamp=from_timestamp,
                to_timestamp=to_timestamp,
            )
        except ValueError as exc:
            self._log_invalid_request_exception("/v1/admin/usage", exc, field="group_by")
            return self._invalid_request_response("/v1/admin/usage", str(exc), field="group_by")
        normalized_group_by = "subject" if group_by == "user" else group_by
        return web.json_response(
            {
                "group_by": normalized_group_by,
                "from": from_timestamp.isoformat() if from_timestamp is not None else None,
                "to": to_timestamp.isoformat() if to_timestamp is not None else None,
                "items": items,
            }
        )

    async def mcp_get_handler(self, request: web.Request) -> web.Response:
        return web.Response(status=405, headers={"Allow": "POST"}, text="This endpoint does not offer GET SSE streams.")

    async def mcp_delete_handler(self, request: web.Request) -> web.Response:
        return web.Response(status=405, headers={"Allow": "POST"}, text="This endpoint does not support session deletion.")

    async def mcp_post_handler(self, request: web.Request) -> web.Response:
        request_context, blocked = await self._preflight_request(request)
        if blocked:
            return blocked
        assert request_context is not None
        payload, invalid_json = await self._parse_json_request(request)
        if invalid_json:
            return invalid_json
        protocol_version, invalid_protocol = self._effective_protocol_version(request, payload)
        if invalid_protocol is not None:
            return invalid_protocol
        if isinstance(payload, list):
            return await self._handle_batch_message(payload, request_context, protocol_version=protocol_version)
        single_payload, invalid = await self._handle_single_message(payload, request_context)
        if invalid is not None:
            return invalid
        if single_payload is None:
            if protocol_version is None:
                return web.Response(status=202)
            return web.Response(status=202, headers={"MCP-Protocol-Version": protocol_version})
        if protocol_version is None and isinstance(single_payload, dict):
            result = single_payload.get("result")
            if isinstance(result, dict) and is_supported_protocol_version(result.get("protocolVersion")):
                protocol_version = result["protocolVersion"]
        return self._jsonrpc_http_response(single_payload, protocol_version=protocol_version)

    def build_app(self) -> web.Application:
        app = web.Application(
            client_max_size=self._config.gateway.request_max_bytes,
            middlewares=[self._error_middleware],
        )
        app.add_routes(
            [
                web.get("/healthz", self.health_handler),
                web.get("/readyz", self.ready_handler),
                web.get("/tools", self.tools_handler),
                web.get("/metrics", self.metrics_handler),
                web.get("/v1/me", self.me_handler),
                web.get("/v1/me/api-keys", self.my_api_keys_list_handler),
                web.post("/v1/me/api-keys", self.my_api_keys_create_handler),
                web.delete("/v1/me/api-keys/{key_id}", self.my_api_keys_revoke_handler),
                web.get("/v1/admin/users", self.admin_users_list_handler),
                web.post("/v1/admin/users", self.admin_users_create_handler),
                web.patch("/v1/admin/users/{user_id}", self.admin_users_update_handler),
                web.get("/v1/admin/identities", self.admin_identities_list_handler),
                web.put("/v1/admin/identities/{subject}", self.admin_identity_put_handler),
                web.patch("/v1/admin/identities/{subject}", self.admin_identity_patch_handler),
                web.get("/v1/admin/integrations", self.admin_integrations_list_handler),
                web.get("/v1/admin/groups", self.admin_groups_list_handler),
                web.post("/v1/admin/groups", self.admin_groups_create_handler),
                web.patch("/v1/admin/groups/{group_id}", self.admin_groups_update_handler),
                web.delete("/v1/admin/groups/{group_id}", self.admin_groups_delete_handler),
                web.post("/v1/admin/groups/{group_id}/members", self.admin_group_members_add_handler),
                web.delete("/v1/admin/groups/{group_id}/members/{subject}", self.admin_group_members_delete_handler),
                web.get("/v1/admin/groups/{group_id}/integration-grants", self.admin_group_integration_grants_list_handler),
                web.post("/v1/admin/groups/{group_id}/integration-grants", self.admin_group_integration_grants_create_handler),
                web.delete(
                    "/v1/admin/groups/{group_id}/integration-grants/{upstream_id}",
                    self.admin_group_integration_grants_delete_handler,
                ),
                web.get("/v1/admin/groups/{group_id}/platform-grants", self.admin_group_platform_grants_list_handler),
                web.post("/v1/admin/groups/{group_id}/platform-grants", self.admin_group_platform_grants_create_handler),
                web.delete(
                    "/v1/admin/groups/{group_id}/platform-grants/{permission}",
                    self.admin_group_platform_grants_delete_handler,
                ),
                web.get("/v1/admin/usage", self.admin_usage_handler),
                web.get("/mcp", self.mcp_get_handler),
                web.post("/mcp", self.mcp_post_handler),
                web.delete("/mcp", self.mcp_delete_handler),
            ]
        )
        return app

    async def run(self) -> None:
        app = self.build_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, self._config.gateway.listen_host, self._config.gateway.listen_port)
        await site.start()
        self._logger.info(
            "http_server_started",
            host=self._config.gateway.listen_host,
            port=self._config.gateway.listen_port,
        )
        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            pass
        finally:
            await runner.cleanup()
