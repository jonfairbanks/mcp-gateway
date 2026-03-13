from __future__ import annotations

import json
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict


def format_startup_summary(summary: Dict[str, Any]) -> str:
    upstreams = summary.get("upstreams", [])
    status_width = max((len(str(item.get("status", ""))) for item in upstreams), default=6)
    id_width = max((len(str(item.get("id", ""))) for item in upstreams), default=2)

    lines = [
        "Startup Summary",
        (
            f"Gateway ready: {'yes' if summary.get('gateway_ready') else 'no'}"
            f" | upstreams: {summary.get('ready_upstream_count', 0)} ready,"
            f" {summary.get('degraded_upstream_count', 0)} degraded,"
            f" {summary.get('failed_upstream_count', 0)} failed"
        ),
    ]
    for upstream in upstreams:
        line = f"{str(upstream.get('status', 'unknown')).upper():<{status_width}}  {str(upstream.get('id', '')):<{id_width}}"
        details = [f"tools={upstream.get('tool_count', 0)}"]
        stage = upstream.get("stage")
        reason = upstream.get("reason")
        if stage:
            details.append(f"stage={stage}")
        if reason:
            details.append(f"reason={reason}")
        lines.append(f"{line}  {' | '.join(details)}")
    return "\n".join(lines)


@dataclass
class Logger:
    stdout_json: bool

    def _emit(self, payload: Dict[str, Any]) -> None:
        if self.stdout_json:
            sys.stdout.write(json.dumps(payload, separators=(",", ":")) + "\n")
            sys.stdout.flush()

    def info(self, event: str, **fields: Any) -> None:
        self._emit({"level": "info", "event": event, **fields})

    def warn(self, event: str, **fields: Any) -> None:
        self._emit({"level": "warn", "event": event, **fields})

    def error(self, event: str, **fields: Any) -> None:
        self._emit({"level": "error", "event": event, **fields})

    def pretty_startup_summary(self, summary: Dict[str, Any]) -> None:
        if not sys.stderr.isatty():
            return
        sys.stderr.write(format_startup_summary(summary) + "\n")
        sys.stderr.flush()


class Timer:
    def __init__(self) -> None:
        self._start = time.perf_counter()

    def elapsed_ms(self) -> int:
        return int((time.perf_counter() - self._start) * 1000)
