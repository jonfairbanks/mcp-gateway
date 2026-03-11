from __future__ import annotations

import json
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional


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


class Timer:
    def __init__(self) -> None:
        self._start = time.perf_counter()

    def elapsed_ms(self) -> int:
        return int((time.perf_counter() - self._start) * 1000)
