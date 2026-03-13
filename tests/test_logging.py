from __future__ import annotations

from mcp_gateway.logging import Logger, format_startup_summary


class FakeStderr:
    def __init__(self, *, tty: bool) -> None:
        self._tty = tty
        self.chunks: list[str] = []

    def isatty(self) -> bool:
        return self._tty

    def write(self, chunk: str) -> int:
        self.chunks.append(chunk)
        return len(chunk)

    def flush(self) -> None:
        return None


def test_format_startup_summary_is_human_readable() -> None:
    summary = {
        "gateway_ready": True,
        "ready_upstream_count": 2,
        "degraded_upstream_count": 0,
        "failed_upstream_count": 1,
        "upstreams": [
            {"id": "context7", "status": "ready", "tool_count": 2},
            {"id": "aws-mcp", "status": "failed", "tool_count": 0, "stage": "initialize", "reason": "aws login required"},
        ],
    }

    rendered = format_startup_summary(summary)

    assert "Startup Summary" in rendered
    assert "Gateway ready: yes" in rendered
    assert "READY" in rendered
    assert "FAILED" in rendered
    assert "reason=aws login required" in rendered


def test_pretty_startup_summary_only_writes_for_tty(monkeypatch) -> None:
    logger = Logger(stdout_json=True)
    summary = {
        "gateway_ready": True,
        "ready_upstream_count": 1,
        "degraded_upstream_count": 0,
        "failed_upstream_count": 0,
        "upstreams": [{"id": "context7", "status": "ready", "tool_count": 2}],
    }

    tty_stderr = FakeStderr(tty=True)
    monkeypatch.setattr("sys.stderr", tty_stderr)
    logger.pretty_startup_summary(summary)
    assert "".join(tty_stderr.chunks).startswith("Startup Summary\n")

    non_tty_stderr = FakeStderr(tty=False)
    monkeypatch.setattr("sys.stderr", non_tty_stderr)
    logger.pretty_startup_summary(summary)
    assert non_tty_stderr.chunks == []
