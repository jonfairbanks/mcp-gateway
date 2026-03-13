from __future__ import annotations

import asyncio
from types import SimpleNamespace

from mcp_gateway import cli


class RecordingLogger:
    def __init__(self) -> None:
        self.warnings: list[tuple[str, dict]] = []
        self.errors: list[tuple[str, dict]] = []
        self.infos: list[tuple[str, dict]] = []

    def warn(self, event: str, **fields) -> None:
        self.warnings.append((event, fields))

    def error(self, event: str, **fields) -> None:
        self.errors.append((event, fields))

    def info(self, event: str, **fields) -> None:
        self.infos.append((event, fields))


def test_validate_runtime_config_requires_explicit_unauthenticated_opt_in() -> None:
    logger = RecordingLogger()
    config = SimpleNamespace(gateway=SimpleNamespace(api_key="", allow_unauthenticated=False))

    try:
        cli._validate_runtime_config(config, logger)
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("expected runtime config validation to exit")

    assert logger.errors
    assert logger.errors[0][0] == "authentication_required"


def test_validate_runtime_config_warns_when_unauthenticated_mode_is_explicit() -> None:
    logger = RecordingLogger()
    config = SimpleNamespace(gateway=SimpleNamespace(api_key="", allow_unauthenticated=True))

    cli._validate_runtime_config(config, logger)

    assert logger.warnings
    assert logger.warnings[0][0] == "authentication_disabled"


def test_cache_cleanup_loop_runs_and_stops_cleanly(monkeypatch) -> None:
    logger = RecordingLogger()
    monkeypatch.setattr(cli, "CACHE_CLEANUP_INTERVAL_SECONDS", 0.0)

    class FakeStore:
        def __init__(self) -> None:
            self.called = asyncio.Event()

        async def cleanup_expired_cache(self) -> int:
            self.called.set()
            return 2

    store = FakeStore()

    async def run_test() -> None:
        task = asyncio.create_task(cli._run_cache_cleanup_loop(store, logger))
        await asyncio.wait_for(store.called.wait(), timeout=1.0)
        task.cancel()
        await task

    asyncio.run(run_test())

    assert logger.infos
    assert logger.infos[0][0] == "cache_cleanup"
