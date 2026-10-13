from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from mcp_gateway import cli
from mcp_gateway.server_http import HttpServer
from tests.gateway_test_support import RecordingLogger, _config_with_upstreams


@pytest.mark.parametrize("failure", ["start", "telemetry", "gateway", "body", "gateway_close", "telemetry_close", "store_close"])
def test_gateway_resources_close_every_acquired_resource(monkeypatch, failure):
    calls = []

    def record(name):
        calls.append(name)
        if name == failure:
            raise RuntimeError(name)

    class Store:
        def __init__(self, _dsn):
            pass

        async def start(self):
            record("start")

        async def close(self):
            record("store_close")

    class Telemetry:
        def __init__(self, **_kwargs):
            record("telemetry")

        async def close(self):
            record("telemetry_close")

    class Gateway:
        def __init__(self, *_args):
            record("gateway")

        async def close(self):
            record("gateway_close")

    monkeypatch.setattr(cli, "PostgresStore", Store)
    monkeypatch.setattr(cli, "GatewayTelemetry", Telemetry)
    monkeypatch.setattr(cli, "Gateway", Gateway)

    logger = RecordingLogger()

    async def run():
        with pytest.raises(RuntimeError, match=failure):
            async with cli._gateway_resources(_config_with_upstreams([]), logger, "unused"):
                record("body")

    asyncio.run(run())
    expected = ["store_close"]
    if failure not in {"start", "telemetry"}:
        expected.insert(0, "telemetry_close")
    if failure not in {"start", "telemetry", "gateway"}:
        expected.insert(0, "gateway_close")
    assert [name for name in calls if name.endswith("_close")] == expected
    if failure.endswith("_close"):
        resource = {"store_close": "postgres", "telemetry_close": "telemetry", "gateway_close": "gateway"}[failure]
        assert logger.errors == [("resource_close_failed", {
            "resource": resource, "error_type": "RuntimeError", "error": failure,
        })]


@pytest.mark.parametrize("command", ["_serve_http", "_run_warmup_check", "_run_list_integrations"])
def test_gateway_commands_close_resources_on_operation_failure(monkeypatch, command):
    closed = []
    config = _config_with_upstreams([])
    monkeypatch.setattr(cli, "_load_environment", lambda: None)
    monkeypatch.setattr(cli, "load_config", lambda _path: config)
    monkeypatch.delenv("DATABASE_URL", raising=False)

    class Store:
        def __init__(self, _dsn):
            pass

        async def start(self):
            pass

        async def close(self):
            closed.append("store")

    class Telemetry:
        def __init__(self, **_kwargs):
            pass

        async def close(self):
            closed.append("telemetry")

    class Gateway:
        def __init__(self, *_args):
            pass

        async def warmup(self):
            raise RuntimeError("operation failed")

        async def list_integrations(self):
            raise RuntimeError("operation failed")

        async def close(self):
            closed.append("gateway")

    monkeypatch.setattr(cli, "PostgresStore", Store)
    monkeypatch.setattr(cli, "GatewayTelemetry", Telemetry)
    monkeypatch.setattr(cli, "Gateway", Gateway)
    with pytest.raises(RuntimeError, match="operation failed"):
        asyncio.run(getattr(cli, command)("unused"))
    assert closed == ["gateway", "telemetry", "store"]


@pytest.mark.parametrize("stage", ["setup", "site"])
def test_http_runner_cleans_up_after_startup_failure(monkeypatch, stage):
    closed = []

    class Runner:
        def __init__(self, _app):
            pass

        async def setup(self):
            if stage == "setup":
                raise RuntimeError("setup failed")

        async def cleanup(self):
            closed.append("runner")

    class Site:
        def __init__(self, *_args):
            pass

        async def start(self):
            raise RuntimeError("site failed")

    monkeypatch.setattr("mcp_gateway.server_http.web.AppRunner", Runner)
    monkeypatch.setattr("mcp_gateway.server_http.web.TCPSite", Site)
    server = HttpServer(_config_with_upstreams([]), object(), object(), object())
    with pytest.raises(RuntimeError, match=f"{stage} failed"):
        asyncio.run(server.run())
    assert closed == ["runner"]


@pytest.mark.skipif(sys.platform == "win32", reason="Unix SIGTERM lifecycle")
@pytest.mark.parametrize("stage", ["warmup", "serving"])
def test_sigterm_closes_stdio_and_gateway_resources(tmp_path, stage):
    # Exercise the real CLI, HTTP runner and stdio child in a disposable subprocess.
    # Only Postgres and telemetry are replaced to record resource closure offline.
    repository = Path(__file__).resolve().parents[1]
    journal = tmp_path / "requests.jsonl"
    events = tmp_path / "events.jsonl"
    script = r'''
import asyncio
import json
import os
import sys
from pathlib import Path
from mcp_gateway import cli, server_http
from tests.gateway_test_support import RecordingLogger, _config_with_upstreams, _upstream

journal, events, stage, fixture = sys.argv[1:]
def record(event):
    with open(events, 'a') as f:
        f.write(json.dumps(event) + '\n')
class Store(cli.PostgresStore):
    async def close(self):
        await super().close()
        record('store_closed')
class Telemetry(cli.GatewayTelemetry):
    async def close(self):
        await super().close()
        record('telemetry_closed')
class Gateway(cli.Gateway):
    async def warmup(self):
        await super().warmup()
        if stage == 'warmup':
            record('ready')
            await asyncio.Event().wait()
    async def close(self):
        await super().close()
        record('gateway_closed')
class Runner(server_http.web.AppRunner):
    async def cleanup(self):
        await super().cleanup()
        record('runner_closed')
class Logger(cli.Logger):
    def info(self, event, **fields):
        if event == 'http_server_started':
            record('ready')

upstream = _upstream('fixture')
upstream.transport = 'stdio'
upstream.command = [sys.executable, fixture]
upstream.env = {'FAKE_STDIO_JOURNAL': journal}
config = _config_with_upstreams([upstream])
config.gateway.listen_host = '127.0.0.1'
config.gateway.listen_port = 0
cli.load_config = lambda _: config
cli._load_environment = lambda: None
cli.PostgresStore = Store
cli.GatewayTelemetry = Telemetry
cli.Gateway = Gateway
cli.Logger = Logger
server_http.web.AppRunner = Runner
asyncio.run(cli._run_http('unused'))
record('finished')
'''
    env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(repository / "src"), str(repository)])}
    env.pop("DATABASE_URL", None)
    env.pop("OTEL_TRACES_EXPORTER", None)
    fixture = repository / "tests/fixtures/fake_stdio_upstream.py"
    child_pids = set()
    with (tmp_path / "output.log").open("w+") as output:
        process = subprocess.Popen(
            [sys.executable, "-c", script, str(journal), str(events), stage, str(fixture)],
            cwd=tmp_path, env=env, stdout=output, stderr=output,
        )
        try:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                if events.exists() and '"ready"' in events.read_text():
                    break
                if process.poll() is not None:
                    output.seek(0)
                    pytest.fail(f"gateway exited before ready: {output.read()}")
                time.sleep(0.01)
            else:
                pytest.fail("gateway did not become ready")
            child_pids = {row["pid"] for row in map(json.loads, journal.read_text().splitlines())}
            process.send_signal(signal.SIGTERM)
            assert process.wait(timeout=5) == 0
            recorded = list(map(json.loads, events.read_text().splitlines()))
            expected = ["ready"]
            if stage == "serving":
                expected.append("runner_closed")
            assert recorded == expected + ["gateway_closed", "telemetry_closed", "store_closed", "finished"]
            for pid in child_pids:
                with pytest.raises(ProcessLookupError):
                    os.kill(pid, 0)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)
            if journal.exists():
                child_pids.update(row["pid"] for row in map(json.loads, journal.read_text().splitlines()))
            for pid in child_pids:
                try:
                    os.kill(pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass


def test_http_task_cancellation_propagates_and_restores_sigterm_handler(monkeypatch):
    previous_handler = signal.getsignal(signal.SIGTERM)

    async def cancel(_path):
        raise asyncio.CancelledError

    monkeypatch.setattr(cli, "_serve_http", cancel)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(cli._run_http("unused"))
    assert signal.getsignal(signal.SIGTERM) == previous_handler
