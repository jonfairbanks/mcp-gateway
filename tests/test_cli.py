from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from mcp_gateway import cli
from mcp_gateway.logging import Logger


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
    config = SimpleNamespace(gateway=SimpleNamespace(auth_mode="single_shared", api_key="", allow_unauthenticated=False))

    with pytest.raises(SystemExit, match="2"):
        cli._validate_runtime_config(config, logger)

    assert logger.errors[0][0] == "authentication_required"


def test_validate_runtime_config_warns_when_unauthenticated_mode_is_explicit() -> None:
    logger = RecordingLogger()
    config = SimpleNamespace(gateway=SimpleNamespace(auth_mode="single_shared", api_key="", allow_unauthenticated=True))

    cli._validate_runtime_config(config, logger)

    assert logger.warnings[0][0] == "authentication_disabled"


def test_validate_runtime_config_allows_postgres_auth_mode_without_shared_key() -> None:
    logger = RecordingLogger()
    config = SimpleNamespace(gateway=SimpleNamespace(auth_mode="postgres_api_keys", api_key="", allow_unauthenticated=False))

    cli._validate_runtime_config(config, logger)

    assert logger.errors == []


def test_validate_database_runtime_requires_database_for_postgres_auth_mode() -> None:
    logger = RecordingLogger()
    config = SimpleNamespace(gateway=SimpleNamespace(auth_mode="postgres_api_keys"))

    with pytest.raises(SystemExit, match="2"):
        cli._validate_database_runtime(config, logger, "")

    assert logger.errors[0][0] == "database_required"


def test_validate_database_runtime_reports_human_readable_stderr(capsys) -> None:
    logger = Logger(stdout_json=False)
    config = SimpleNamespace(gateway=SimpleNamespace(auth_mode="postgres_api_keys"))

    with pytest.raises(SystemExit, match="2"):
        cli._validate_database_runtime(config, logger, "")

    captured = capsys.readouterr()
    assert "ERROR: DATABASE_URL not set" in captured.err
    assert "hint: Set DATABASE_URL or switch gateway.auth_mode to single_shared" in captured.err


def test_validate_database_runtime_skips_single_shared_mode() -> None:
    logger = RecordingLogger()
    config = SimpleNamespace(gateway=SimpleNamespace(auth_mode="single_shared"))

    cli._validate_database_runtime(config, logger, "")

    assert logger.errors == []


def test_load_environment_calls_python_dotenv(monkeypatch) -> None:
    called_with: Path | None = None

    def fake_load_dotenv(*, dotenv_path: Path) -> None:
        nonlocal called_with
        called_with = dotenv_path

    monkeypatch.setattr(cli, "load_dotenv", fake_load_dotenv)
    monkeypatch.setattr(cli.Path, "cwd", lambda: Path("/tmp/test-cwd"))

    assert cli._load_environment() is None
    assert called_with == Path("/tmp/test-cwd/.env")


def test_load_environment_returns_dotenv_path_when_present(monkeypatch, tmp_path) -> None:
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text("MCP_GATEWAY_API_KEY=secret\n", encoding="utf-8")
    monkeypatch.setattr(cli.Path, "cwd", lambda: tmp_path)

    assert cli._load_environment() == dotenv_path


def test_run_create_api_key_reports_auth_mode_misconfiguration(monkeypatch, capsys) -> None:
    config = SimpleNamespace(gateway=SimpleNamespace(auth_mode="single_shared"))
    monkeypatch.setattr(cli, "load_config", lambda _path: config)
    monkeypatch.setenv("DATABASE_URL", "postgresql://example")

    with pytest.raises(SystemExit, match="2"):
        asyncio.run(cli._run_create_api_key("config.yaml", key_name="laptop", expires_days=None))

    captured = capsys.readouterr()
    assert "ERROR: gateway.auth_mode must be postgres_api_keys" in captured.err


def test_run_validate_config_reports_success(monkeypatch, capsys) -> None:
    config = SimpleNamespace(upstreams=[object()], gateway=SimpleNamespace(auth_mode="single_shared"))
    monkeypatch.setattr(cli, "load_config", lambda _path: config)
    monkeypatch.setattr(cli, "_load_environment", lambda: None)

    cli._run_validate_config("config.yaml")

    captured = capsys.readouterr()
    assert "INFO config_valid" in captured.out
    assert "config_path=\"config.yaml\"" in captured.out


def test_build_parser_supports_single_owner_commands() -> None:
    parser = cli.build_parser()

    assert parser.parse_args(["create-api-key", "--config", "config.yaml", "--key-name", "laptop"]).command == "create-api-key"
    assert parser.parse_args(["list-api-keys", "--config", "config.yaml"]).command == "list-api-keys"
    assert parser.parse_args(["revoke-api-key", "--config", "config.yaml", "--key-id", "key-1"]).command == "revoke-api-key"
    for removed_command in ("create-user", "create-group", "add-group-member", "grant-integration", "grant-platform"):
        with pytest.raises(SystemExit):
            parser.parse_args([removed_command, "--config", "config.yaml"])


def test_run_create_api_key_prints_plaintext_once(monkeypatch, capsys) -> None:
    class FakeStore:
        async def close(self) -> None:
            return None

    class FakeAuth:
        async def issue_api_key(self, *, key_name: str, expires_days: int | None):
            assert key_name == "laptop"
            assert expires_days == 7
            return {"id": "key-1", "api_key": "mgw_secret"}

    monkeypatch.setattr(cli, "_open_auth_context", lambda _path: _async_result((RecordingLogger(), FakeStore(), FakeAuth())))

    asyncio.run(cli._run_create_api_key("config.yaml", key_name="laptop", expires_days=7))

    assert capsys.readouterr().out == '{"id":"key-1","api_key":"mgw_secret"}\n'


def test_run_revoke_api_key_rejects_unknown_or_invalid_id(monkeypatch, capsys) -> None:
    class FakeStore:
        async def close(self) -> None:
            return None

    class FakeAuth:
        async def revoke_api_key(self, api_key_id: str):
            return None

    monkeypatch.setattr(cli, "_open_auth_context", lambda _path: _async_result((Logger(stdout_json=False), FakeStore(), FakeAuth())))

    with pytest.raises(SystemExit, match="2"):
        asyncio.run(cli._run_revoke_api_key("config.yaml", key_id="not-a-uuid"))
    assert "ERROR: key_id must be a valid UUID" in capsys.readouterr().err

    with pytest.raises(SystemExit, match="2"):
        asyncio.run(cli._run_revoke_api_key("config.yaml", key_id="82ad853d-a6d1-4483-94f0-18197262090a"))
    assert "ERROR: API key was not found" in capsys.readouterr().err


def test_cache_cleanup_loop_runs_and_stops_cleanly(monkeypatch) -> None:
    logger = RecordingLogger()
    monkeypatch.setattr(cli, "CACHE_CLEANUP_INTERVAL_SECONDS", 0.0)

    class FakeStore:
        def __init__(self) -> None:
            self.called = asyncio.Event()

        async def cleanup_expired_cache(self) -> int:
            self.called.set()
            return 2

        async def cleanup_expired_rate_limits(self) -> int:
            return 1

    store = FakeStore()

    async def run_test() -> None:
        task = asyncio.create_task(cli._run_cache_cleanup_loop(store, logger))
        await asyncio.wait_for(store.called.wait(), timeout=1.0)
        task.cancel()
        await task

    asyncio.run(run_test())

    assert logger.infos[0][0] == "cache_cleanup"
    assert logger.infos[0][1]["deleted_rows"] == 2
    assert logger.infos[0][1]["rate_limit_deleted_rows"] == 1


async def _async_result(value):
    return value
