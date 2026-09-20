from __future__ import annotations

import asyncio
import html
import json
import time
from typing import Any, Dict, Optional

from aiohttp import web

from .auth import AuthUnavailableError
from .config import AppConfig
from .gateway import Gateway
from .jsonrpc import make_error_response
from .logging import Logger
from .protocol import DEFAULT_HTTP_PROTOCOL_VERSION, is_supported_protocol_version, negotiate_protocol_version
from .request_context import AuthenticatedPrincipal, RequestContext
from .telemetry import GatewayTelemetry

MAX_JSONRPC_BATCH_SIZE = 100


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

    @web.middleware
    async def _error_middleware(self, request: web.Request, handler):
        try:
            return await handler(request)
        except web.HTTPMethodNotAllowed as exc:
            allowed_methods = sorted(exc.allowed_methods or [])
            self._logger.warn(
                "http_method_not_allowed",
                endpoint=request.path,
                method=request.method,
                allowed_methods=allowed_methods,
                origin=request.headers.get("Origin"),
                access_control_request_method=request.headers.get("Access-Control-Request-Method"),
                access_control_request_headers=request.headers.get("Access-Control-Request-Headers"),
                content_type=request.headers.get("Content-Type"),
                user_agent=request.headers.get("User-Agent"),
            )
            response = web.Response(status=405, headers={"Allow": ", ".join(allowed_methods)}, text="Method Not Allowed")
            return self._with_mcp_cors_headers(request, response)
        except web.HTTPException:
            raise
        except Exception as exc:  # noqa: BLE001
            self._logger.error(
                "http_unhandled_exception",
                endpoint=request.path,
                error_type=type(exc).__name__,
                error=str(exc) or type(exc).__name__,
            )
            if request.path == "/mcp":
                response = web.json_response(make_error_response(None, -32603, "Internal error"), status=500)
                return self._with_mcp_cors_headers(request, response)
            return self._rest_error(500, "InternalError", "Unexpected server error.")

    @web.middleware
    async def _tracing_middleware(self, request: web.Request, handler):
        context = self._telemetry.extract_context(request.headers)
        with self._telemetry.start_http_server_span(request.method, request.path, context=context):
            try:
                response = await handler(request)
            except Exception as exc:  # noqa: BLE001
                self._telemetry.annotate_exception(exc)
                raise
            self._telemetry.annotate_http_response(response.status)
            return response

    def _principal_profile(self, request_context: RequestContext) -> Dict[str, Any]:
        principal = request_context.principal
        assert principal is not None
        return {
            "subject": principal.subject,
            "auth_scheme": principal.auth_scheme,
            "api_key_id": principal.api_key_id,
            "key_name": principal.key_name,
        }

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
        if unauthorized is None:
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
            return f"subject:{principal.auth_scheme}:{principal.subject}"
        return f"client:{request_context.client_id or 'anonymous'}"

    async def _fallback_rate_limit(self, scope_key: str, *, cost: int = 1) -> Optional[web.Response]:
        limit = max(1, self._config.gateway.rate_limit_per_minute)
        cost = max(1, cost)
        now = time.monotonic()
        async with self._rate_limit_lock:
            self._prune_rate_limit_state(now)
            window_start, count = self._rate_limit_state.get(scope_key, (now, 0))
            if now - window_start >= 60:
                window_start = now
                count = 0
            count += cost
            self._rate_limit_state[scope_key] = (window_start, count)
            if count <= limit:
                return None
        retry_after = max(1, int(60 - (now - window_start)))
        return web.json_response(
            make_error_response(None, -32029, "Rate limit exceeded"),
            status=429,
            headers={"Retry-After": str(retry_after)},
        )

    async def _rate_limit(self, request_context: RequestContext, *, cost: int = 1) -> Optional[web.Response]:
        scope_key = self._rate_limit_scope_key(request_context)
        limit = max(1, self._config.gateway.rate_limit_per_minute)
        cost = max(1, cost)
        if self._gateway.store_available():
            try:
                result = await self._gateway.consume_rate_limit(
                    scope_key=scope_key,
                    limit=limit,
                    window_seconds=60,
                    cost=cost,
                )
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
        return await self._fallback_rate_limit(scope_key, cost=cost)

    async def _pre_auth_rate_limit(self, client_id: str) -> Optional[web.Response]:
        # Authentication can require a database lookup, so enforce this limit locally
        # before allowing untrusted credentials to reach the authentication backend.
        return await self._fallback_rate_limit(f"pre_auth:client:{client_id}")

    async def _preflight_request(
        self,
        request: web.Request,
        endpoint: Optional[str] = None,
        *,
        require_auth: bool = True,
        strict_auth: bool = False,
    ) -> tuple[Optional[RequestContext], Optional[web.Response]]:
        client_id = self._client_id(request)
        blocked = await self._pre_auth_rate_limit(client_id)
        if blocked is not None:
            return None, blocked

        principal: Optional[AuthenticatedPrincipal] = None
        require_auth = require_auth or strict_auth
        if require_auth:
            if endpoint:
                principal, unauthorized = await self._authenticate_http_endpoint(
                    request,
                    endpoint,
                    require_principal=strict_auth,
                )
            else:
                principal, unauthorized = await self._authenticate(request, require_principal=strict_auth)
            if unauthorized is not None:
                return None, unauthorized
        request_context = RequestContext(client_id=client_id, principal=principal)
        blocked = await self._rate_limit(request_context)
        if blocked is not None:
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
    def _append_vary_values(existing: Optional[str], additions: str) -> str:
        merged: set[str] = set()
        if existing:
            merged.update(value.strip() for value in existing.split(",") if value.strip())
        merged.update(value.strip() for value in additions.split(",") if value.strip())
        return ", ".join(sorted(merged))

    def _mcp_cors_headers(self, request: web.Request) -> Dict[str, str]:
        origin = request.headers.get("Origin")
        allow_origin = origin if origin else "*"
        requested_headers = request.headers.get("Access-Control-Request-Headers")
        allow_headers = requested_headers or "Authorization, Content-Type, MCP-Protocol-Version"
        return {
            "Access-Control-Allow-Origin": allow_origin,
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": allow_headers,
            "Access-Control-Max-Age": "600",
            "Vary": "Origin, Access-Control-Request-Method, Access-Control-Request-Headers",
        }

    def _with_mcp_cors_headers(self, request: web.Request, response: web.Response) -> web.Response:
        # Unit tests call handlers with SimpleNamespace request doubles that may
        # omit `.path`; treat those direct MCP handler calls as /mcp.
        request_path = getattr(request, "path", "/mcp")
        if request_path != "/mcp":
            return response
        for header, value in self._mcp_cors_headers(request).items():
            if header == "Vary":
                response.headers["Vary"] = self._append_vary_values(response.headers.get("Vary"), value)
                continue
            response.headers.setdefault(header, value)
        return response

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
            return negotiate_protocol_version(requested_version), None

        return header_version or DEFAULT_HTTP_PROTOCOL_VERSION, None

    async def health_handler(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok"})

    async def root_handler(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok"})

    async def ready_handler(self, request: web.Request) -> web.Response:
        ready = self._gateway.is_ready()
        status = 200 if ready else 503
        return web.json_response({"ready": ready}, status=status)

    async def tools_handler(self, request: web.Request) -> web.Response:
        _, blocked = await self._preflight_request(
            request,
            endpoint="/tools",
            require_auth=not self._config.gateway.public_tools_catalog,
        )
        if blocked is not None:
            return blocked
        payload = await self._gateway.tools_catalog()
        return web.json_response(payload)

    async def metrics_handler(self, request: web.Request) -> web.Response:
        if not self._config.gateway.public_metrics:
            _, blocked = await self._preflight_request(request, endpoint="/metrics", strict_auth=True)
            if blocked is not None:
                return blocked
        body = self._telemetry.render_prometheus()
        return web.Response(body=body, headers={"Content-Type": self._telemetry.prometheus_content_type})

    async def me_handler(self, request: web.Request) -> web.Response:
        request_context, blocked = await self._preflight_request(request, endpoint="/v1/me", strict_auth=True)
        if blocked is not None:
            return blocked
        assert request_context is not None
        return web.json_response(self._principal_profile(request_context))

    async def mcp_get_handler(self, request: web.Request) -> web.Response:
        response = web.Response(status=405, headers={"Allow": "POST, OPTIONS"}, text="This endpoint does not support GET SSE streams.")
        return self._with_mcp_cors_headers(request, response)

    async def mcp_delete_handler(self, request: web.Request) -> web.Response:
        response = web.Response(status=405, headers={"Allow": "POST, OPTIONS"}, text="This endpoint does not support session deletion.")
        return self._with_mcp_cors_headers(request, response)

    async def mcp_options_handler(self, request: web.Request) -> web.Response:
        response = web.Response(status=204, headers={"Allow": "POST, OPTIONS"})
        return self._with_mcp_cors_headers(request, response)

    async def mcp_post_handler(self, request: web.Request) -> web.Response:
        request_context, blocked = await self._preflight_request(request)
        if blocked is not None:
            return self._with_mcp_cors_headers(request, blocked)
        assert request_context is not None
        payload, invalid_json = await self._parse_json_request(request)
        if invalid_json is not None:
            return self._with_mcp_cors_headers(request, invalid_json)
        if isinstance(payload, list):
            if len(payload) > MAX_JSONRPC_BATCH_SIZE:
                response = self._jsonrpc_http_response(
                    make_error_response(None, -32600, "JSON-RPC batch is too large"),
                    status=400,
                )
                return self._with_mcp_cors_headers(request, response)
        protocol_version, invalid_protocol = self._effective_protocol_version(request, payload)
        if invalid_protocol is not None:
            return self._with_mcp_cors_headers(request, invalid_protocol)
        if isinstance(payload, list):
            if len(payload) > 1:
                # Preflight already charged for the first message. Reserve the
                # remainder atomically before dispatching any batch work.
                blocked = await self._rate_limit(request_context, cost=len(payload) - 1)
                if blocked is not None:
                    return self._with_mcp_cors_headers(request, blocked)
            response = await self._handle_batch_message(payload, request_context, protocol_version=protocol_version)
            return self._with_mcp_cors_headers(request, response)
        single_payload, invalid = await self._handle_single_message(payload, request_context)
        if invalid is not None:
            return self._with_mcp_cors_headers(request, invalid)
        if single_payload is None:
            if protocol_version is None:
                return self._with_mcp_cors_headers(request, web.Response(status=202))
            return self._with_mcp_cors_headers(
                request,
                web.Response(status=202, headers={"MCP-Protocol-Version": protocol_version}),
            )
        if protocol_version is None and isinstance(single_payload, dict):
            result = single_payload.get("result")
            if isinstance(result, dict) and is_supported_protocol_version(result.get("protocolVersion")):
                protocol_version = result["protocolVersion"]
        response = self._jsonrpc_http_response(single_payload, protocol_version=protocol_version)
        return self._with_mcp_cors_headers(request, response)

    def build_app(self) -> web.Application:
        app = web.Application(
            client_max_size=self._config.gateway.request_max_bytes,
            middlewares=[self._tracing_middleware, self._error_middleware],
        )
        routes = [
            web.get("/", self.root_handler),
            web.get("/healthz", self.health_handler),
            web.get("/readyz", self.ready_handler),
            web.get("/tools", self.tools_handler),
            web.get("/metrics", self.metrics_handler),
            web.get("/v1/me", self.me_handler),
        ]
        routes.extend(
            [
                web.get("/mcp", self.mcp_get_handler),
                web.post("/mcp", self.mcp_post_handler),
                web.delete("/mcp", self.mcp_delete_handler),
                web.options("/mcp", self.mcp_options_handler),
            ]
        )
        app.add_routes(routes)
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
