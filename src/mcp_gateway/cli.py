from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

from .auth import AUTH_MODE_POSTGRES_API_KEYS, AuthService
from .config import load_config
from .gateway import Gateway
from .logging import Logger
from .postgres import PostgresStore
from .server_http import HttpServer
from .telemetry import GatewayTelemetry

CACHE_CLEANUP_INTERVAL_SECONDS = 300.0


def _emit_cli_feedback(logger: Logger, level: str, event: str, **fields) -> None:
    log_method = getattr(logger, level)
    log_method(event, **fields)
    if getattr(logger, "stdout_json", True):
        return

    reason = fields.get("reason") or event.replace("_", " ")
    suggestion = fields.get("suggestion")
    detail_fields = {
        key: value
        for key, value in fields.items()
        if key not in {"reason", "suggestion"} and value not in {None, ""}
    }
    detail_suffix = (
        " (" + ", ".join(f"{key}={value}" for key, value in detail_fields.items()) + ")"
        if detail_fields
        else ""
    )
    sys.stderr.write(f"{level.upper()}: {reason}{detail_suffix}\n")
    if suggestion:
        sys.stderr.write(f"hint: {suggestion}\n")
    sys.stderr.flush()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mcp-gateway")
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve_parser = subparsers.add_parser("serve")
    serve_parser.add_argument("--config", required=True)

    create_key_parser = subparsers.add_parser("create-api-key")
    create_key_parser.add_argument("--config", required=True)
    create_key_parser.add_argument("--subject", required=True)
    create_key_parser.add_argument("--display-name")
    create_key_parser.add_argument("--role", default="member")
    create_key_parser.add_argument("--key-name", default="default")
    create_key_parser.add_argument("--expires-days", type=int)

    return parser


def _validate_runtime_config(config, logger: Logger) -> None:
    if config.gateway.auth_mode == AUTH_MODE_POSTGRES_API_KEYS:
        return
    if config.gateway.api_key:
        return
    if config.gateway.allow_unauthenticated:
        _emit_cli_feedback(
            logger,
            "warn",
            "authentication_disabled",
            reason="gateway.api_key not set",
            explicit_opt_in=True,
        )
        return
    _emit_cli_feedback(
        logger,
        "error",
        "authentication_required",
        reason="gateway.api_key not set",
        suggestion="Set gateway.api_key or enable gateway.allow_unauthenticated",
    )
    raise SystemExit(2)


def _validate_database_runtime(config, logger: Logger, dsn: str) -> None:
    if config.gateway.auth_mode != AUTH_MODE_POSTGRES_API_KEYS:
        return
    if dsn:
        return
    _emit_cli_feedback(
        logger,
        "error",
        "database_required",
        reason="DATABASE_URL not set",
        auth_mode=config.gateway.auth_mode,
        suggestion="Set DATABASE_URL or switch gateway.auth_mode to single_shared",
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
    _validate_database_runtime(config, logger, dsn)
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


async def _run_create_api_key(
    config_path: str,
    *,
    subject: str,
    display_name: str | None,
    role: str,
    key_name: str,
    expires_days: int | None,
) -> None:
    config = load_config(config_path)
    logger = Logger(stdout_json=False)
    dsn = os.getenv("DATABASE_URL", "")
    _validate_database_runtime(config, logger, dsn)
    if config.gateway.auth_mode != AUTH_MODE_POSTGRES_API_KEYS:
        _emit_cli_feedback(
            logger,
            "error",
            "auth_mode_required",
            reason="gateway.auth_mode must be postgres_api_keys",
            suggestion="Set gateway.auth_mode to postgres_api_keys before issuing database-backed API keys",
        )
        raise SystemExit(2)
    store = PostgresStore(dsn)
    await store.start()
    auth = AuthService(config, store, logger)
    try:
        issued = await auth.issue_api_key(
            subject=subject,
            display_name=display_name,
            role=role,
            key_name=key_name,
            expires_days=expires_days,
        )
    finally:
        await store.close()
    print(json.dumps(issued, separators=(",", ":")))


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    try:
        if args.command == "serve":
            asyncio.run(_run_http(args.config))
        elif args.command == "create-api-key":
            asyncio.run(
                _run_create_api_key(
                    args.config,
                    subject=args.subject,
                    display_name=args.display_name,
                    role=args.role,
                    key_name=args.key_name,
                    expires_days=args.expires_days,
                )
            )
        else:
            raise SystemExit(f"Unknown command: {args.command}")
    except KeyboardInterrupt:
        raise SystemExit(130) from None


if __name__ == "__main__":
    main()
