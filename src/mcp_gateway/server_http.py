from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Dict, Optional
from uuid import uuid4

from aiohttp import web

from .config import AppConfig
from .gateway import Gateway
from .jsonrpc import json_dumps, make_error_response
from .logging import Logger


class SseSession:
    def __init__(self, response: web.StreamResponse) -> None:
        self.response = response
        self.queue: asyncio.Queue[str] = asyncio.Queue()
        self.closed = False


class HttpServer:
    def __init__(self, config: AppConfig, gateway: Gateway, logger: Logger) -> None:
        self._config = config
        self._gateway = gateway
        self._logger = logger
        self._sessions: Dict[str, SseSession] = {}
        self._rate_limit_state: Dict[str, tuple[float, int]] = {}
        self._rate_limit_lock = asyncio.Lock()

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

    def _trusted_proxy(self, request: web.Request) -> bool:
        trusted = set(self._config.gateway.trusted_proxies)
        remote = request.remote or ""
        return remote in trusted

    def _client_id(self, request: web.Request) -> str:
        explicit = request.headers.get("X-Client-Id")
        if explicit:
            return explicit
        if self._trusted_proxy(request):
            forwarded_for = request.headers.get("X-Forwarded-For", "")
            if forwarded_for:
                return forwarded_for.split(",")[0].strip()
        return request.remote or "unknown"

    async def _rate_limit(self, request: web.Request) -> Optional[web.Response]:
        limit = max(1, self._config.gateway.rate_limit_per_minute)
        now = time.monotonic()
        client_id = self._client_id(request)
        async with self._rate_limit_lock:
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

    async def health_handler(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok", "service": "mcp-gateway", **self._gateway.status_snapshot()})

    async def ready_handler(self, request: web.Request) -> web.Response:
        ready = self._gateway.is_ready()
        status = 200 if ready else 503
        return web.json_response({"ready": ready, **self._gateway.status_snapshot()}, status=status)

    async def sse_handler(self, request: web.Request) -> web.StreamResponse:
        unauthorized = self._authorize(request)
        if unauthorized:
            return unauthorized
        rate_limited = await self._rate_limit(request)
        if rate_limited:
            return rate_limited

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
        session = SseSession(response)
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
            session.closed = True
            self._sessions.pop(session_id, None)

        return response

    async def message_handler(self, request: web.Request) -> web.Response:
        unauthorized = self._authorize(request)
        if unauthorized:
            return unauthorized
        rate_limited = await self._rate_limit(request)
        if rate_limited:
            return rate_limited

        session_id = request.query.get("session_id") or request.headers.get("MCP-Session-ID")
        try:
            payload = await request.json()
        except json.JSONDecodeError:
            return web.json_response(make_error_response(None, -32700, "Invalid JSON"), status=400)

        client_id = self._client_id(request)
        result = await self._gateway.handle(payload, client_id)
        if payload.get("id") is None:
            return web.Response(status=202)
        if session_id and session_id in self._sessions:
            session = self._sessions[session_id]
            await session.queue.put(json_dumps(result.payload))
            return web.json_response({"status": "queued", "session_id": session_id})

        return web.json_response(result.payload)

    async def rpc_handler(self, request: web.Request) -> web.Response:
        unauthorized = self._authorize(request)
        if unauthorized:
            return unauthorized
        rate_limited = await self._rate_limit(request)
        if rate_limited:
            return rate_limited

        try:
            payload = await request.json()
        except json.JSONDecodeError:
            return web.json_response(make_error_response(None, -32700, "Invalid JSON"), status=400)

        client_id = self._client_id(request)
        result = await self._gateway.handle(payload, client_id)
        if payload.get("id") is None:
            return web.Response(status=202)
        return web.json_response(result.payload)

    def build_app(self) -> web.Application:
        app = web.Application(client_max_size=self._config.gateway.request_max_bytes)
        app.add_routes(
            [
                web.get("/healthz", self.health_handler),
                web.get("/readyz", self.ready_handler),
                web.get("/sse", self.sse_handler),
                web.post("/message", self.message_handler),
                web.post("/rpc", self.rpc_handler),
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
        finally:
            await runner.cleanup()
