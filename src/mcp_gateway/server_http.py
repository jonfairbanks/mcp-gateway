from __future__ import annotations

import asyncio
import html
import json
import time
from typing import Any, Dict, Optional
from uuid import uuid4

from aiohttp import web

from .config import AppConfig
from .gateway import Gateway
from .jsonrpc import json_dumps, make_error_response
from .logging import Logger
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

    def _authorize(self, request: web.Request) -> Optional[web.Response]:
        api_key = self._config.gateway.api_key
        if not api_key:
            return None
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            token = auth.split(" ", 1)[1]
            if token and token == api_key:
                return None
        return web.json_response(make_error_response(None, -32010, "Unauthorized"), status=401)

    def _authorize_http_endpoint(self, request: web.Request, endpoint: str) -> Optional[web.Response]:
        unauthorized = self._authorize(request)
        if not unauthorized:
            return None
        headers = {"WWW-Authenticate": 'Bearer realm="mcp-gateway"'}
        message = f"{endpoint} requires Authorization: Bearer <gateway.api_key>."
        hint = "Browsers do not send this header automatically. Use curl or an API client."
        accept = request.headers.get("Accept", "")
        if "text/html" in accept.lower():
            body = (
                "<!doctype html>"
                "<html><head><title>401 Unauthorized</title></head>"
                "<body>"
                "<h1>401 Unauthorized</h1>"
                f"<p>{html.escape(message)}</p>"
                f"<p>{html.escape(hint)}</p>"
                "</body></html>"
            )
            return web.Response(text=body, status=401, content_type="text/html", headers=headers)
        return web.json_response(
            {
                "error": "Unauthorized",
                "message": message,
                "hint": hint,
            },
            status=401,
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
            # If still over limit, evict the oldest windows first.
            overflow = len(self._rate_limit_state) - self._rate_limit_max_clients
            oldest = sorted(self._rate_limit_state.items(), key=lambda item: item[1][0])[:overflow]
            for client_id, _ in oldest:
                self._rate_limit_state.pop(client_id, None)
        self._next_rate_limit_gc_at = now + self._rate_limit_gc_interval_seconds

    async def _rate_limit(self, request: web.Request) -> Optional[web.Response]:
        limit = max(1, self._config.gateway.rate_limit_per_minute)
        now = time.monotonic()
        client_id = self._client_id(request)
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

    async def _preflight_request(self, request: web.Request, endpoint: Optional[str] = None) -> Optional[web.Response]:
        if endpoint:
            unauthorized = self._authorize_http_endpoint(request, endpoint)
        else:
            unauthorized = self._authorize(request)
        if unauthorized:
            return unauthorized
        return await self._rate_limit(request)

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
        blocked = await self._preflight_request(request, endpoint=endpoint)
        if blocked:
            return None, blocked, None
        payload, invalid_json = await self._parse_json_request(request)
        if invalid_json:
            return None, invalid_json, None
        client_id = self._client_id(request)
        result = await self._gateway.handle(payload, client_id)
        return payload, None, result

    async def health_handler(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok", "service": "mcp-gateway", **self._gateway.status_snapshot()})

    async def ready_handler(self, request: web.Request) -> web.Response:
        ready = self._gateway.is_ready()
        status = 200 if ready else 503
        return web.json_response({"ready": ready, **self._gateway.status_snapshot()}, status=status)

    async def tools_handler(self, request: web.Request) -> web.Response:
        blocked = await self._preflight_request(request, endpoint="/tools")
        if blocked:
            return blocked
        payload = await self._gateway.tools_catalog()
        return web.json_response(payload)

    async def metrics_handler(self, request: web.Request) -> web.Response:
        body = self._telemetry.render_prometheus()
        return web.Response(body=body, headers={"Content-Type": self._telemetry.prometheus_content_type})

    async def sse_handler(self, request: web.Request) -> web.StreamResponse:
        blocked = await self._preflight_request(request)
        if blocked:
            return blocked
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
        payload, blocked, result = await self._dispatch_gateway_request(request)
        if blocked:
            return blocked
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
