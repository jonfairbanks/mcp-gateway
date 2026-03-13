from __future__ import annotations

import argparse
import asyncio
import os

from .config import load_config
from .gateway import Gateway
from .logging import Logger
from .postgres import PostgresStore
from .server_http import HttpServer
from .telemetry import GatewayTelemetry

CACHE_CLEANUP_INTERVAL_SECONDS = 300.0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mcp-gateway")
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve_parser = subparsers.add_parser("serve")
    serve_parser.add_argument("--config", required=True)

    return parser


def _validate_runtime_config(config, logger: Logger) -> None:
    if config.gateway.api_key:
        return
    if config.gateway.allow_unauthenticated:
        logger.warn(
            "authentication_disabled",
            reason="gateway.api_key not set",
            explicit_opt_in=True,
        )
        return
    logger.error(
        "authentication_required",
        reason="gateway.api_key not set",
        suggestion="Set gateway.api_key or enable gateway.allow_unauthenticated",
    )
    raise SystemExit(2)


async def _run_cache_cleanup_loop(store: PostgresStore, logger: Logger) -> None:
    try:
        while True:
            await asyncio.sleep(CACHE_CLEANUP_INTERVAL_SECONDS)
            try:
                deleted_rows = await store.cleanup_expired_cache()
            except Exception as exc:  # noqa: BLE001
                logger.warn("cache_cleanup_failed", error=str(exc))
                continue
            if deleted_rows > 0:
                logger.info("cache_cleanup", deleted_rows=deleted_rows)
    except asyncio.CancelledError:
        pass


async def _run_http(config_path: str) -> None:
    config = load_config(config_path)
    logger = Logger(stdout_json=config.logging.stdout_json)
    _validate_runtime_config(config, logger)
    dsn = os.getenv("DATABASE_URL", "")
    if not dsn:
        logger.warn(
            "database_disabled",
            reason="DATABASE_URL not set",
            impact="postgres logging and persistent cache are disabled",
        )
    store = PostgresStore(dsn)
    await store.start()
    telemetry = GatewayTelemetry()
    gateway = Gateway(config, store, logger, telemetry)
    cache_cleanup_task: asyncio.Task[None] | None = None
    try:
        await gateway.warmup()
        startup_summary = gateway.startup_summary()
        logger.info("startup_summary", **startup_summary)
        logger.pretty_startup_summary(startup_summary)
        if dsn:
            cache_cleanup_task = asyncio.create_task(_run_cache_cleanup_loop(store, logger))
        server = HttpServer(config, gateway, logger, telemetry)
        await server.run()
    finally:
        if cache_cleanup_task:
            cache_cleanup_task.cancel()
            try:
                await cache_cleanup_task
            except asyncio.CancelledError:
                pass
        await gateway.close()
        await telemetry.close()
        await store.close()


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    try:
        if args.command == "serve":
            asyncio.run(_run_http(args.config))
        else:
            raise SystemExit(f"Unknown command: {args.command}")
    except KeyboardInterrupt:
        raise SystemExit(130) from None


if __name__ == "__main__":
    main()
