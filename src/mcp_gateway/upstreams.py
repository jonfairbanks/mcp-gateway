from __future__ import annotations

import asyncio
import json
import os
import time
from contextlib import asynccontextmanager
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
    def __init__(
        self,
        endpoint: str,
        timeout_ms: int,
        headers: Optional[Dict[str, str]] = None,
        bearer_token_env_var: Optional[str] = None,
        serialize_requests: bool = False,
    ) -> None:
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
                    await resp.read()
                    return UpstreamResponse(
                        payload={"jsonrpc": "2.0", "id": payload.get("id"), "result": {"accepted": True}},
                        success=True,
                    )

                raw_text = await resp.text()
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
                    body = await resp.text()
                    raise RuntimeError(f"HTTP upstream notify failed status {resp.status}: {body[:300]}")
                await resp.read()

    @staticmethod
    def _looks_like_sse(raw_text: str) -> bool:
        stripped = raw_text.lstrip()
        return stripped.startswith("event:") or stripped.startswith("data:") or "\nevent:" in raw_text or "\ndata:" in raw_text

    @staticmethod
    def _parse_sse_events(raw_text: str) -> list[SseEvent]:
        events: list[SseEvent] = []
        event_name = "message"
        data_lines: list[str] = []
        saw_field = False

        for line in raw_text.splitlines():
            if not line:
                if saw_field:
                    events.append(SseEvent(event=event_name, data="\n".join(data_lines)))
                event_name = "message"
                data_lines = []
                saw_field = False
                continue
            if line.startswith(":"):
                continue
            field, separator, value = line.partition(":")
            if not separator:
                continue
            if value.startswith(" "):
                value = value[1:]
            saw_field = True
            if field == "event":
                event_name = value or "message"
            elif field == "data":
                data_lines.append(value)

        if saw_field:
            events.append(SseEvent(event=event_name, data="\n".join(data_lines)))
        return events

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
            merged_env = None
            if self._env:
                # Merge custom env vars over process env so PATH and runtime defaults are preserved.
                merged_env = {**os.environ, **self._env}
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
        await self.start()
        async with self._lock:
            expected_id = payload.get("id")
            deadline = time.monotonic() + self._timeout
            for attempt in range(2):
                if attempt > 0:
                    await self.start()
                assert self._process
                assert self._process.stdin
                assert self._process.stdout
                try:
                    self._process.stdin.write((json.dumps(payload) + "\n").encode("utf-8"))
                    await self._process.stdin.drain()
                except (BrokenPipeError, ConnectionResetError):
                    await self._discard_dead_process()
                    if attempt == 0:
                        continue
                    raise RuntimeError("Upstream stdio closed") from None

                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise asyncio.TimeoutError()
                    line = await asyncio.wait_for(self._process.stdout.readline(), timeout=remaining)
                    if not line:
                        await self._discard_dead_process()
                        if attempt == 0:
                            break
                        raise RuntimeError("Upstream stdio closed") from None
                    data = json.loads(line.decode("utf-8"))
                    # Stdio upstreams may emit notifications/progress messages between requests.
                    # Keep reading until we receive the response for this request id.
                    if data.get("id") != expected_id:
                        continue
                    success = "error" not in data
                    return UpstreamResponse(payload=data, success=success)
            raise RuntimeError("Upstream stdio closed")

    async def notify(self, payload: Dict[str, Any]) -> None:
        await self.start()
        assert self._process
        assert self._process.stdin
        async with self._lock:
            try:
                self._process.stdin.write((json.dumps(payload) + "\n").encode("utf-8"))
                await self._process.stdin.drain()
            except (BrokenPipeError, ConnectionResetError):
                await self._discard_dead_process()
                raise RuntimeError("Upstream stdio closed") from None

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
