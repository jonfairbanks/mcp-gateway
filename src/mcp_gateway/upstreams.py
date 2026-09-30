from __future__ import annotations

import asyncio
import io
import json
import os
import time
from collections.abc import Iterator
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

import aiohttp
from opentelemetry.propagate import inject

from .protocol import CURRENT_PROTOCOL_VERSION, is_supported_protocol_version


@dataclass
class UpstreamResponse:
    payload: Dict[str, Any]
    success: bool


@dataclass
class SseEvent:
    event: str
    data: str


class StreamableHTTPUpstream:
    MAX_SSE_LINES = 16384
    MAX_SSE_EVENTS = 1024

    def __init__(
        self,
        endpoint: str,
        timeout_ms: int,
        headers: Optional[Dict[str, str]] = None,
        bearer_token_env_var: Optional[str] = None,
        serialize_requests: bool = False,
        response_max_bytes: int = 8 * 1024 * 1024,
    ) -> None:
        self._response_max_bytes = response_max_bytes
        self._endpoint = endpoint
        self._timeout = timeout_ms / 1000
        self._headers = headers or {}
        self._bearer_token_env_var = bearer_token_env_var
        self._serialize_requests = serialize_requests
        self._session_id: Optional[str] = None
        self._protocol_version = CURRENT_PROTOCOL_VERSION
        self._session: Optional[aiohttp.ClientSession] = None
        self._start_lock = asyncio.Lock()
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        if self._session is None:
            async with self._start_lock:
                if self._session is None:
                    timeout = aiohttp.ClientTimeout(total=self._timeout)
                    self._session = aiohttp.ClientSession(timeout=timeout)

    async def close(self) -> None:
        if self._session:
            await self._session.close()

    async def _request_headers(self) -> Dict[str, str]:
        headers = dict(self._headers)
        headers.setdefault("Content-Type", "application/json")
        # Some upstreams still return SSE-framed JSON-RPC responses even when the
        # gateway only uses POST /mcp, so keep the broader Accept header here.
        headers.setdefault("Accept", "application/json, text/event-stream")
        headers.setdefault("MCP-Protocol-Version", self._protocol_version)
        inject(headers)
        if self._session_id:
            headers["MCP-Session-ID"] = self._session_id
        if "Authorization" not in headers:
            if self._bearer_token_env_var:
                token = os.getenv(self._bearer_token_env_var, "")
                if token:
                    headers["Authorization"] = f"Bearer {token}"
        return headers

    def _capture_session_headers(self, resp: aiohttp.ClientResponse) -> None:
        session_id = resp.headers.get("MCP-Session-ID") or resp.headers.get("mcp-session-id")
        if session_id:
            self._session_id = session_id
        protocol_version = resp.headers.get("MCP-Protocol-Version") or resp.headers.get("mcp-protocol-version")
        if protocol_version:
            self._protocol_version = protocol_version

    @asynccontextmanager
    async def _request_guard(self):
        # Certain vendor MCPs require one-at-a-time HTTP requests even though the
        # gateway runs other upstreams concurrently.
        if self._serialize_requests:
            async with self._lock:
                yield
            return
        yield

    async def call(self, payload: Dict[str, Any]) -> UpstreamResponse:
        if not self._session:
            await self.start()
        if payload.get("method") == "initialize":
            params = payload.get("params") or {}
            protocol_version = params.get("protocolVersion")
            if isinstance(protocol_version, str):
                self._protocol_version = protocol_version

        assert self._session
        async with self._request_guard():
            async with self._session.post(self._endpoint, json=payload, headers=await self._request_headers()) as resp:
                self._capture_session_headers(resp)
                if resp.status == 202:
                    await self._read_body(resp)
                    return UpstreamResponse(
                        payload={"jsonrpc": "2.0", "id": payload.get("id"), "result": {"accepted": True}},
                        success=True,
                    )

                raw_text = await self._read_body(resp)
                data: Dict[str, Any]
                if raw_text:
                    try:
                        data = json.loads(raw_text)
                    except json.JSONDecodeError:
                        sse_payload = self._extract_sse_payload(raw_text, expected_id=payload.get("id"))
                        if sse_payload is not None:
                            data = sse_payload
                        else:
                            message = f"HTTP upstream returned non-JSON body (status {resp.status})"
                            if self._looks_like_sse(raw_text):
                                message = f"HTTP upstream returned SSE body without matching JSON-RPC response (status {resp.status})"
                            return UpstreamResponse(
                                payload={
                                    "jsonrpc": "2.0",
                                    "id": payload.get("id"),
                                    "error": {
                                        "code": -32003,
                                        "message": message,
                                        "data": {"body": raw_text[:500]},
                                    },
                                },
                                success=False,
                            )
                else:
                    data = {"jsonrpc": "2.0", "id": payload.get("id"), "result": {}}

                if resp.status >= 400 and "error" not in data:
                    data = {
                        "jsonrpc": "2.0",
                        "id": payload.get("id"),
                        "error": {
                            "code": -32003,
                            "message": f"HTTP upstream error status {resp.status}",
                            "data": data,
                        },
                    }
                if payload.get("method") == "initialize":
                    result = data.get("result")
                    if isinstance(result, dict) and is_supported_protocol_version(result.get("protocolVersion")):
                        self._protocol_version = result["protocolVersion"]
                success = "error" not in data
                return UpstreamResponse(payload=data, success=success)

    async def notify(self, payload: Dict[str, Any]) -> None:
        if not self._session:
            await self.start()
        assert self._session
        async with self._request_guard():
            async with self._session.post(self._endpoint, json=payload, headers=await self._request_headers()) as resp:
                self._capture_session_headers(resp)
                if resp.status >= 400:
                    body = await self._read_body(resp)
                    raise RuntimeError(f"HTTP upstream notify failed status {resp.status}: {body[:300]}")
                await self._read_body(resp)

    async def _read_body(self, resp: aiohttp.ClientResponse) -> str:
        # iter_chunked yields decoded bytes, so compressed responses have the same limit.
        body = bytearray()
        async for chunk in resp.content.iter_chunked(64 * 1024):
            if len(body) + len(chunk) > self._response_max_bytes:
                raise RuntimeError("HTTP upstream response exceeds http_response_max_bytes")
            body.extend(chunk)
        return body.decode(resp.charset or "utf-8", errors="replace")

    @staticmethod
    def _looks_like_sse(raw_text: str) -> bool:
        stripped = raw_text.lstrip()
        return stripped.startswith("event:") or stripped.startswith("data:") or "\nevent:" in raw_text or "\ndata:" in raw_text

    @classmethod
    def _parse_sse_events(cls, raw_text: str) -> Iterator[SseEvent]:
        event_name = "message"
        data_lines: list[str] = []
        event_count = 0
        line_count = 0
        # Avoid splitlines() and a second full list of event objects.
        for raw_line in io.StringIO(raw_text, newline=None):
            line_count += 1
            # _read_body already bounds all line and event bytes, including framing.
            if line_count > cls.MAX_SSE_LINES:
                raise RuntimeError("HTTP upstream SSE line limit exceeded")
            line = raw_line.rstrip("\r\n")
            if not line:
                if data_lines:
                    event_count += 1
                    if event_count > cls.MAX_SSE_EVENTS:
                        raise RuntimeError("HTTP upstream SSE event limit exceeded")
                    yield SseEvent(event=event_name, data="\n".join(data_lines))
                event_name = "message"
                data_lines = []
                continue
            if line.startswith(":"):
                continue
            field, separator, value = line.partition(":")
            if not separator:
                continue
            if value.startswith(" "):
                value = value[1:]
            if field == "event":
                event_name = value or "message"
            elif field == "data":
                data_lines.append(value)
        if data_lines:
            if event_count >= cls.MAX_SSE_EVENTS:
                raise RuntimeError("HTTP upstream SSE event limit exceeded")
            yield SseEvent(event=event_name, data="\n".join(data_lines))

    @classmethod
    def _extract_sse_payload(cls, raw_text: str, expected_id: Any) -> Optional[Dict[str, Any]]:
        candidate: Optional[Dict[str, Any]] = None
        for event in cls._parse_sse_events(raw_text):
            if not event.data:
                continue
            try:
                decoded = json.loads(event.data)
            except json.JSONDecodeError:
                continue
            if not isinstance(decoded, dict):
                continue
            if decoded.get("id") == expected_id:
                return decoded
            if candidate is None and ("result" in decoded or "error" in decoded):
                candidate = decoded
        if expected_id is None:
            return candidate
        return None


class StdioUpstream:
    def __init__(
        self,
        command: list[str],
        env: Dict[str, str],
        cwd: Optional[str],
        timeout_ms: int,
        read_limit_bytes: int,
        upstream_id: str,
        on_stderr_line: Optional[Callable[[str, str], None]] = None,
    ) -> None:
        self._command = command
        self._env = env
        self._cwd = cwd
        self._timeout = timeout_ms / 1000
        self._read_limit_bytes = max(64 * 1024, read_limit_bytes)
        self._upstream_id = upstream_id
        self._on_stderr_line = on_stderr_line
        self._initialize_payload: Optional[Dict[str, Any]] = None
        self._initialize_complete = False
        self._initialized = False
        self._process: Optional[asyncio.subprocess.Process] = None
        self._lock = asyncio.Lock()
        self._start_lock = asyncio.Lock()
        self._stderr_task: Optional[asyncio.Task[None]] = None

    async def start(self) -> None:
        async with self._start_lock:
            if self._process and self._process.returncode is None:
                return
            if self._process and self._process.returncode is not None:
                await self._discard_dead_process()
            self._initialize_complete = False
            self._initialized = False
            # Only runtime necessities are inherited. Credentials must be explicitly
            # assigned to this upstream in its env mapping.
            inherited = {"PATH", "HOME", "LANG", "LC_ALL", "LC_CTYPE", "TZ", "TMPDIR", "TMP", "TEMP", "SYSTEMROOT", "WINDIR"}
            merged_env = {name: value for name, value in os.environ.items() if name in inherited}
            merged_env.update(self._env)
            self._process = await asyncio.create_subprocess_exec(
                *self._command,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=merged_env,
                cwd=self._cwd,
                limit=self._read_limit_bytes,
            )
            self._stderr_task = asyncio.create_task(self._stream_stderr())

    async def _discard_dead_process(self) -> None:
        if self._stderr_task:
            self._stderr_task.cancel()
            try:
                await self._stderr_task
            except asyncio.CancelledError:
                pass
            self._stderr_task = None
        self._process = None

    async def close(self) -> None:
        self._initialize_complete = False
        self._initialized = False
        if not self._process:
            return
        if self._process.returncode is None:
            try:
                self._process.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(self._process.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                try:
                    self._process.kill()
                except ProcessLookupError:
                    pass
                await self._process.wait()
        if self._stderr_task:
            self._stderr_task.cancel()
            try:
                await self._stderr_task
            except asyncio.CancelledError:
                pass
            self._stderr_task = None
        self._process = None

    async def call(self, payload: Dict[str, Any]) -> UpstreamResponse:
        async with self._lock:
            try:
                await self.start()
                if payload.get("method") == "initialize":
                    self._initialize_complete = False
                    self._initialized = False
                else:
                    await self._ensure_initialized()
                response = await self._call_locked(payload)
                if payload.get("method") == "initialize" and self._valid_initialize_response(response):
                    self._initialize_payload = deepcopy(payload)
                    self._initialize_complete = True
                return response
            except (BrokenPipeError, ConnectionResetError):
                await self.close()
                raise RuntimeError("Upstream stdio closed; request outcome unknown") from None
            except (RuntimeError, asyncio.TimeoutError, asyncio.CancelledError):
                await self.close()
                raise

    async def notify(self, payload: Dict[str, Any]) -> None:
        async with self._lock:
            try:
                await self.start()
                await self._ensure_initialized()
                if payload.get("method") != "notifications/initialized" or not self._initialized:
                    await self._notify_locked(payload)
            except (BrokenPipeError, ConnectionResetError):
                await self.close()
                raise RuntimeError("Upstream stdio closed") from None
            except (RuntimeError, asyncio.TimeoutError, asyncio.CancelledError):
                await self.close()
                raise

    @staticmethod
    def _valid_initialize_response(response: UpstreamResponse) -> bool:
        result = response.payload.get("result")
        return response.success and isinstance(result, dict) and is_supported_protocol_version(result.get("protocolVersion"))

    async def _ensure_initialized(self) -> None:
        # The caller holds _lock, so concurrent requests share one replacement
        # handshake. Only initialization is repeated, never an uncertain tool call.
        if self._initialize_payload is None or self._initialized:
            return
        if not self._initialize_complete:
            response = await self._call_locked(self._initialize_payload)
            if not self._valid_initialize_response(response):
                raise RuntimeError("Upstream stdio initialization failed")
            self._initialize_complete = True
        await self._notify_locked({"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}})
        self._initialized = True

    async def _notify_locked(self, payload: Dict[str, Any]) -> None:
        assert self._process and self._process.stdin
        self._process.stdin.write((json.dumps(payload) + "\n").encode("utf-8"))
        # timeout() preserves caller cancellation even when drain completes in the
        # same event-loop turn (wait_for can swallow that race on Python 3.11).
        async with asyncio.timeout(self._timeout):
            await self._process.stdin.drain()

    async def _call_locked(self, payload: Dict[str, Any]) -> UpstreamResponse:
        assert self._process and self._process.stdout
        expected_id = payload.get("id")
        deadline = time.monotonic() + self._timeout
        await self._notify_locked(payload)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise asyncio.TimeoutError()
            async with asyncio.timeout(remaining):
                line = await self._process.stdout.readline()
            if not line:
                raise RuntimeError("Upstream stdio closed; request outcome unknown")
            data = json.loads(line.decode("utf-8"))
            if data.get("id") != expected_id:
                continue
            return UpstreamResponse(payload=data, success="error" not in data)

    async def _stream_stderr(self) -> None:
        if not self._process or not self._process.stderr:
            return
        try:
            while True:
                line = await self._process.stderr.readline()
                if not line:
                    return
                if not self._on_stderr_line:
                    continue
                text = line.decode("utf-8", errors="replace").rstrip()
                if text:
                    self._on_stderr_line(self._upstream_id, text)
        except asyncio.CancelledError:
            return
