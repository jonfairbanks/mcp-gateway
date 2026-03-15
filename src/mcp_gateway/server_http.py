from __future__ import annotations

import asyncio
import html
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import uuid4

from aiohttp import web

from .auth import AuthUnavailableError, ROLE_ADMIN, ROLE_VIEWER, VALID_ROLES
from .config import AppConfig
from .gateway import Gateway
from .jsonrpc import json_dumps, make_error_response
from .logging import Logger
from .request_context import AuthenticatedPrincipal, RequestContext
from .telemetry import GatewayTelemetry


class SseSession:
    def __init__(self, response: web.StreamResponse, max_messages: int) -> None:
        self.response = response
        self.queue: asyncio.Queue[str] = asyncio.Queue(maxsize=max_messages)
        self.closed = False


class HttpServer:
    def __init__(self, config: AppConfig, gateway: Gateway, logger: Logger, telemetry: GatewayTelemetry) -> None:
        self._config = config
        self._gateway = gateway
        self._logger = logger
        self._telemetry = telemetry
        self._sessions: Dict[str, SseSession] = {}
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

    def _require_postgres_management(self) -> Optional[web.Response]:
        if self._gateway.auth_mode_requires_database():
            return None
        return self._management_unavailable_response()

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

    def _normalize_role(self, role: str) -> Optional[str]:
        normalized_role = role.strip().lower()
        if normalized_role not in VALID_ROLES:
            return None
        return normalized_role

    def _require_admin(self, request_context: RequestContext) -> Optional[web.Response]:
        if request_context.role == ROLE_ADMIN:
            return None
        return self._rest_forbidden("Admin role required.")

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
            "role": principal.role,
            "auth_scheme": principal.auth_scheme,
            "user_id": principal.user_id,
            "api_key_id": principal.api_key_id,
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
        message = f"{endpoint} requires Authorization: Bearer <api_key>."
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

    def _forbidden_response(self, endpoint: str) -> web.Response:
        return web.json_response(
            make_error_response(
                None,
                -32001,
                "Blocked by gateway policy: role not allowed",
                data={"category": "role_denied", "endpoint": endpoint, "retryable": False},
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
            # If still over limit, evict the oldest windows first.
            overflow = len(self._rate_limit_state) - self._rate_limit_max_clients
            oldest = sorted(self._rate_limit_state.items(), key=lambda item: item[1][0])[:overflow]
            for client_id, _ in oldest:
                self._rate_limit_state.pop(client_id, None)
        self._next_rate_limit_gc_at = now + self._rate_limit_gc_interval_seconds

    async def _rate_limit(self, client_id: str) -> Optional[web.Response]:
        limit = max(1, self._config.gateway.rate_limit_per_minute)
        now = time.monotonic()
        async with self._rate_limit_lock:
            self._prune_rate_limit_state(now)
            window_start, count = self._rate_limit_state.get(client_id, (now, 0))
            if now - window_start >= 60:
                window_start = now
                count = 0
            count += 1
            self._rate_limit_state[client_id] = (window_start, count)
            if count <= limit:
                return None
        retry_after = max(1, int(60 - (now - window_start)))
        return web.json_response(
            make_error_response(None, -32029, "Rate limit exceeded"),
            status=429,
            headers={"Retry-After": str(retry_after)},
        )

    def _session_capacity_exceeded(self) -> bool:
        return len(self._sessions) >= max(1, self._config.gateway.max_sse_sessions)

    async def _close_session(self, session_id: str) -> None:
        session = self._sessions.pop(session_id, None)
        if not session:
            return
        session.closed = True
        try:
            await session.response.write_eof()
        except RuntimeError:
            pass
        except ConnectionResetError:
            pass

    async def _enqueue_session_payload(self, session_id: str, data: str) -> bool:
        session = self._sessions.get(session_id)
        if not session or session.closed:
            return False
        try:
            session.queue.put_nowait(data)
            return True
        except asyncio.QueueFull:
            await self._close_session(session_id)
            return False

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
        blocked = await self._rate_limit(client_id)
        if blocked:
            return None, blocked
        # Keep auth and request attribution together so later RBAC layers can
        # build on the same path instead of re-reading headers in each handler.
        return RequestContext(client_id=client_id, principal=principal), None

    async def _parse_json_request(self, request: web.Request) -> tuple[Optional[Dict[str, Any]], Optional[web.Response]]:
        try:
            return await request.json(), None
        except json.JSONDecodeError:
            return None, web.json_response(make_error_response(None, -32700, "Invalid JSON"), status=400)

    async def _dispatch_gateway_request(
        self,
        request: web.Request,
        endpoint: Optional[str] = None,
    ) -> tuple[Optional[Dict[str, Any]], Optional[web.Response], Optional[Any]]:
        request_context, blocked = await self._preflight_request(request, endpoint=endpoint)
        if blocked:
            return None, blocked, None
        assert request_context is not None
        payload, invalid_json = await self._parse_json_request(request)
        if invalid_json:
            return None, invalid_json, None
        result = await self._gateway.handle(payload, request_context)
        return payload, None, result

    async def health_handler(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok", "service": "mcp-gateway", **self._gateway.status_snapshot()})

    async def ready_handler(self, request: web.Request) -> web.Response:
        ready = self._gateway.is_ready()
        status = 200 if ready else 503
        return web.json_response({"ready": ready, **self._gateway.status_snapshot()}, status=status)

    async def tools_handler(self, request: web.Request) -> web.Response:
        # The tool catalog may be intentionally public for discovery, but the
        # execution endpoints still go through the authenticated preflight path.
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
        unavailable = self._require_postgres_management()
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
        unavailable = self._require_postgres_management()
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
        unavailable = self._require_postgres_management()
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
        unavailable = self._require_postgres_management()
        if unavailable:
            return unavailable
        request_context, blocked = await self._preflight_request(request, endpoint="/v1/admin/users", strict_auth=True)
        if blocked:
            return blocked
        assert request_context is not None
        forbidden = self._require_admin(request_context)
        if forbidden:
            return forbidden
        items = await self._gateway.list_users()
        return web.json_response({"items": items})

    async def admin_users_create_handler(self, request: web.Request) -> web.Response:
        unavailable = self._require_postgres_management()
        if unavailable:
            return unavailable
        request_context, blocked = await self._preflight_request(request, endpoint="/v1/admin/users", strict_auth=True)
        if blocked:
            return blocked
        assert request_context is not None
        forbidden = self._require_admin(request_context)
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
        if not isinstance(role, str) or not role.strip():
            return self._rest_error(400, "InvalidRequest", "role must be a non-empty string.")
        normalized_role = self._normalize_role(role)
        if normalized_role is None:
            return self._invalid_request_response(
                "/v1/admin/users",
                "role must be one of: admin, member, viewer",
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
        unavailable = self._require_postgres_management()
        if unavailable:
            return unavailable
        request_context, blocked = await self._preflight_request(request, endpoint="/v1/admin/users", strict_auth=True)
        if blocked:
            return blocked
        assert request_context is not None
        forbidden = self._require_admin(request_context)
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
        role = body.get("role")
        is_active = body.get("is_active")
        if display_name is not None and (not isinstance(display_name, str) or not display_name.strip()):
            return self._rest_error(400, "InvalidRequest", "display_name must be a non-empty string when provided.")
        if role is not None and (not isinstance(role, str) or not role.strip()):
            return self._rest_error(400, "InvalidRequest", "role must be a non-empty string when provided.")
        normalized_role = None
        if role is not None:
            normalized_role = self._normalize_role(role)
            if normalized_role is None:
                return self._invalid_request_response(
                    "/v1/admin/users",
                    "role must be one of: admin, member, viewer",
                    field="role",
                )
        if is_active is not None and not isinstance(is_active, bool):
            return self._rest_error(400, "InvalidRequest", "is_active must be a boolean when provided.")
        if display_name is None and role is None and is_active is None:
            return self._rest_error(400, "InvalidRequest", "At least one updatable field is required.")
        try:
            user = await self._gateway.update_user(
                user_id,
                display_name=display_name,
                role=normalized_role,
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

    async def admin_usage_handler(self, request: web.Request) -> web.Response:
        unavailable = self._require_postgres_management()
        if unavailable:
            return unavailable
        request_context, blocked = await self._preflight_request(request, endpoint="/v1/admin/usage", strict_auth=True)
        if blocked:
            return blocked
        assert request_context is not None
        forbidden = self._require_admin(request_context)
        if forbidden:
            return forbidden
        group_by = request.query.get("group_by", "user")
        if group_by not in {"user", "api_key"}:
            return self._rest_error(400, "InvalidRequest", "group_by must be one of: user, api_key.")
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
        items = await self._gateway.usage_summary(
            group_by=group_by,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
        )
        return web.json_response(
            {
                "group_by": group_by,
                "from": from_timestamp.isoformat() if from_timestamp is not None else None,
                "to": to_timestamp.isoformat() if to_timestamp is not None else None,
                "items": items,
            }
        )

    async def sse_handler(self, request: web.Request) -> web.StreamResponse:
        request_context, blocked = await self._preflight_request(request)
        if blocked:
            return blocked
        assert request_context is not None
        if request_context.role == ROLE_VIEWER:
            return self._forbidden_response("/sse")
        if self._session_capacity_exceeded():
            return web.json_response(
                make_error_response(
                    None,
                    -32031,
                    "SSE session capacity exceeded",
                    data={"category": "capacity_exhausted", "retryable": True},
                ),
                status=503,
            )

        response = web.StreamResponse(
            status=200,
            reason="OK",
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
            },
        )
        await response.prepare(request)
        session_id = str(uuid4())
        session = SseSession(response, max(1, self._config.gateway.sse_queue_max_messages))
        self._sessions[session_id] = session

        await response.write(f"event: ready\ndata: {session_id}\n\n".encode("utf-8"))
        await response.drain()

        try:
            while not session.closed:
                data = await session.queue.get()
                await response.write(f"data: {data}\n\n".encode("utf-8"))
                await response.drain()
        except asyncio.CancelledError:
            pass
        finally:
            await self._close_session(session_id)

        return response

    async def message_handler(self, request: web.Request) -> web.Response:
        session_id = request.query.get("session_id") or request.headers.get("MCP-Session-ID")
        request_context, blocked = await self._preflight_request(request)
        if blocked:
            return blocked
        assert request_context is not None
        if request_context.role == ROLE_VIEWER:
            return self._forbidden_response("/message")
        payload, invalid_json = await self._parse_json_request(request)
        if invalid_json:
            return invalid_json
        assert payload is not None
        result = await self._gateway.handle(payload, request_context)
        assert payload is not None
        assert result is not None
        if payload.get("id") is None:
            return web.Response(status=202)
        if session_id and session_id in self._sessions:
            enqueued = await self._enqueue_session_payload(session_id, json_dumps(result.payload))
            if not enqueued:
                return web.json_response(
                    make_error_response(
                        payload.get("id"),
                        -32030,
                        "SSE session backpressure",
                        data={"category": "session_backpressure", "retryable": True, "session_id": session_id},
                    ),
                    status=503,
                )
            return web.json_response({"status": "queued", "session_id": session_id})

        return web.json_response(result.payload)

    async def rpc_handler(self, request: web.Request) -> web.Response:
        payload, blocked, result = await self._dispatch_gateway_request(request)
        if blocked:
            return blocked
        assert payload is not None
        assert result is not None
        if payload.get("id") is None:
            return web.Response(status=202)
        return web.json_response(result.payload)

    def build_app(self) -> web.Application:
        app = web.Application(client_max_size=self._config.gateway.request_max_bytes)
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
                web.get("/v1/admin/usage", self.admin_usage_handler),
                web.get("/sse", self.sse_handler),
                web.post("/message", self.message_handler),
                web.post("/mcp", self.rpc_handler),
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
            # Normal shutdown path when asyncio.run() cancels outstanding tasks.
            pass
        finally:
            await runner.cleanup()
