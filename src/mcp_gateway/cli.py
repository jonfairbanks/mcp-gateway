from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import NoReturn

from dotenv import load_dotenv

from .auth import AuthService
from .config import (
    AUTH_MODE_POSTGRES_API_KEYS,
    load_config,
)
from .errors import ConflictError, NotFoundError
from .gateway import Gateway
from .logging import Logger
from .postgres import PostgresStore
from .server_http import HttpServer
from .telemetry import GatewayTelemetry

CACHE_CLEANUP_INTERVAL_SECONDS = 300.0


def _load_environment() -> Path | None:
    dotenv_path = Path.cwd() / ".env"
    load_dotenv(dotenv_path=dotenv_path)
    if dotenv_path.exists():
        return dotenv_path
    return None


def _emit_cli_feedback(logger: Logger, level: str, event: str, **fields) -> None:
    if getattr(logger, "stdout_json", True):
        log_method = getattr(logger, level)
        log_method(event, **fields)
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
    create_key_parser.add_argument("--role", choices=["admin"])
    create_key_parser.add_argument("--key-name", default="default")
    create_key_parser.add_argument("--expires-days", type=int)

    validate_parser = subparsers.add_parser("validate-config")
    validate_parser.add_argument("--config", required=True)

    warmup_parser = subparsers.add_parser("warmup-check")
    warmup_parser.add_argument("--config", required=True)

    integrations_parser = subparsers.add_parser("list-integrations")
    integrations_parser.add_argument("--config", required=True)

    create_user_parser = subparsers.add_parser("create-user")
    create_user_parser.add_argument("--config", required=True)
    create_user_parser.add_argument("--subject", required=True)
    create_user_parser.add_argument("--display-name")
    create_user_parser.add_argument("--role", choices=["admin"])
    create_user_parser.add_argument("--issue-api-key", action="store_true")
    create_user_parser.add_argument("--key-name", default="default")

    create_group_parser = subparsers.add_parser("create-group")
    create_group_parser.add_argument("--config", required=True)
    create_group_parser.add_argument("--name", required=True)
    create_group_parser.add_argument("--description")

    add_group_member_parser = subparsers.add_parser("add-group-member")
    add_group_member_parser.add_argument("--config", required=True)
    add_group_member_parser.add_argument("--group-id", required=True)
    add_group_member_parser.add_argument("--subject", required=True)

    grant_integration_parser = subparsers.add_parser("grant-integration")
    grant_integration_parser.add_argument("--config", required=True)
    grant_integration_parser.add_argument("--group-id", required=True)
    grant_integration_parser.add_argument("--upstream-id", required=True)

    grant_platform_parser = subparsers.add_parser("grant-platform")
    grant_platform_parser.add_argument("--config", required=True)
    grant_platform_parser.add_argument("--group-id", required=True)
    grant_platform_parser.add_argument("--permission", required=True)

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


def _validate_postgres_admin_runtime(config, logger: Logger, dsn: str) -> None:
    _validate_database_runtime(config, logger, dsn)
    if config.gateway.auth_mode == AUTH_MODE_POSTGRES_API_KEYS:
        return
    _emit_cli_feedback(
        logger,
        "error",
        "auth_mode_required",
        reason="gateway.auth_mode must be postgres_api_keys",
        suggestion="Set gateway.auth_mode to postgres_api_keys before running admin commands",
    )
    raise SystemExit(2)


def _print_json(payload: object) -> None:
    print(json.dumps(payload, separators=(",", ":")))


async def _run_cache_cleanup_loop(store: PostgresStore, logger: Logger) -> None:
    try:
        while True:
            await asyncio.sleep(CACHE_CLEANUP_INTERVAL_SECONDS)
            try:
                deleted_rows = await store.cleanup_expired_cache()
                deleted_rate_limit_rows = await store.cleanup_expired_rate_limits()
            except Exception as exc:  # noqa: BLE001
                logger.warn("cache_cleanup_failed", error=str(exc))
                continue
            if deleted_rows > 0 or deleted_rate_limit_rows > 0:
                logger.info(
                    "cache_cleanup",
                    deleted_rows=deleted_rows,
                    rate_limit_deleted_rows=deleted_rate_limit_rows,
                )
    except asyncio.CancelledError:
        pass


async def _run_http(config_path: str) -> None:
    dotenv_path = _load_environment()
    config = load_config(config_path)
    logger = Logger(stdout_json=config.logging.stdout_json)
    if dotenv_path is not None:
        logger.info("dotenv_loaded", path=str(dotenv_path))
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
    telemetry = GatewayTelemetry(enabled=config.gateway.tracing_enabled)
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
    dotenv_path = _load_environment()
    config = load_config(config_path)
    logger = Logger(stdout_json=False)
    if dotenv_path is not None:
        logger.info("dotenv_loaded", path=str(dotenv_path))
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


def _run_validate_config(config_path: str) -> None:
    dotenv_path = _load_environment()
    config = load_config(config_path)
    logger = Logger(stdout_json=False)
    if dotenv_path is not None:
        logger.info("dotenv_loaded", path=str(dotenv_path))
    logger.info(
        "config_valid",
        config_path=config_path,
        upstream_count=len(config.upstreams),
        auth_mode=config.gateway.auth_mode,
    )


async def _open_gateway_context(
    config_path: str,
    *,
    require_postgres_admin: bool = False,
) -> tuple[Logger, PostgresStore, GatewayTelemetry, Gateway]:
    dotenv_path = _load_environment()
    config = load_config(config_path)
    logger = Logger(stdout_json=False)
    if dotenv_path is not None:
        logger.info("dotenv_loaded", path=str(dotenv_path))
    dsn = os.getenv("DATABASE_URL", "")
    if require_postgres_admin:
        _validate_postgres_admin_runtime(config, logger, dsn)
    elif config.gateway.auth_mode == AUTH_MODE_POSTGRES_API_KEYS:
        _validate_database_runtime(config, logger, dsn)
    store = PostgresStore(dsn)
    await store.start()
    telemetry = GatewayTelemetry(enabled=config.gateway.tracing_enabled)
    gateway = Gateway(config, store, logger, telemetry)
    return logger, store, telemetry, gateway


async def _run_warmup_check(config_path: str) -> None:
    _, store, telemetry, gateway = await _open_gateway_context(config_path)
    try:
        await gateway.warmup()
        _print_json(gateway.startup_summary())
    finally:
        await gateway.close()
        await telemetry.close()
        await store.close()


async def _run_list_integrations(config_path: str) -> None:
    _, store, telemetry, gateway = await _open_gateway_context(config_path)
    try:
        _print_json({"items": await gateway.list_integrations()})
    finally:
        await gateway.close()
        await telemetry.close()
        await store.close()


def _handle_admin_command_error(exc: Exception) -> "NoReturn":
    logger = Logger(stdout_json=False)
    if isinstance(exc, ConflictError):
        _emit_cli_feedback(logger, "error", "conflict", reason=str(exc))
        raise SystemExit(2)
    if isinstance(exc, NotFoundError):
        _emit_cli_feedback(logger, "error", "not_found", reason=str(exc))
        raise SystemExit(2)
    if isinstance(exc, ValueError):
        _emit_cli_feedback(logger, "error", "invalid_request", reason=str(exc))
        raise SystemExit(2)
    raise exc


async def _run_create_user(
    config_path: str,
    *,
    subject: str,
    display_name: str | None,
    role: str | None,
    issue_api_key: bool,
    key_name: str,
) -> None:
    _, store, telemetry, gateway = await _open_gateway_context(config_path, require_postgres_admin=True)
    try:
        user = await gateway.create_user(subject=subject, display_name=display_name, role=role)
        if user is None:
            raise ConflictError("A user with that subject already exists.")
        payload: dict[str, object] = {"user": user}
        if issue_api_key:
            payload["issued_api_key"] = await gateway.issue_api_key_for_user(user_id=user["id"], key_name=key_name)
        _print_json(payload)
    except (ConflictError, NotFoundError, ValueError) as exc:
        _handle_admin_command_error(exc)
    finally:
        await gateway.close()
        await telemetry.close()
        await store.close()


async def _run_create_group(config_path: str, *, name: str, description: str | None) -> None:
    _, store, telemetry, gateway = await _open_gateway_context(config_path, require_postgres_admin=True)
    try:
        group = await gateway.create_group(name=name, description=description)
        if group is None:
            raise ConflictError("A group with that name already exists.")
        _print_json(group)
    except (ConflictError, NotFoundError, ValueError) as exc:
        _handle_admin_command_error(exc)
    finally:
        await gateway.close()
        await telemetry.close()
        await store.close()


async def _run_add_group_member(config_path: str, *, group_id: str, subject: str) -> None:
    _, store, telemetry, gateway = await _open_gateway_context(config_path, require_postgres_admin=True)
    try:
        _print_json(await gateway.add_group_member(group_id, subject=subject))
    except (ConflictError, NotFoundError, ValueError) as exc:
        _handle_admin_command_error(exc)
    finally:
        await gateway.close()
        await telemetry.close()
        await store.close()


async def _run_grant_integration(config_path: str, *, group_id: str, upstream_id: str) -> None:
    _, store, telemetry, gateway = await _open_gateway_context(config_path, require_postgres_admin=True)
    try:
        _print_json(await gateway.add_group_integration_grant(group_id, upstream_id=upstream_id))
    except (ConflictError, NotFoundError, ValueError) as exc:
        _handle_admin_command_error(exc)
    finally:
        await gateway.close()
        await telemetry.close()
        await store.close()


async def _run_grant_platform(config_path: str, *, group_id: str, permission: str) -> None:
    _, store, telemetry, gateway = await _open_gateway_context(config_path, require_postgres_admin=True)
    try:
        _print_json(await gateway.add_group_platform_grant(group_id, permission=permission))
    except (ConflictError, NotFoundError, ValueError) as exc:
        _handle_admin_command_error(exc)
    finally:
        await gateway.close()
        await telemetry.close()
        await store.close()


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
        elif args.command == "validate-config":
            _run_validate_config(args.config)
        elif args.command == "warmup-check":
            asyncio.run(_run_warmup_check(args.config))
        elif args.command == "list-integrations":
            asyncio.run(_run_list_integrations(args.config))
        elif args.command == "create-user":
            asyncio.run(
                _run_create_user(
                    args.config,
                    subject=args.subject,
                    display_name=args.display_name,
                    role=args.role,
                    issue_api_key=args.issue_api_key,
                    key_name=args.key_name,
                )
            )
        elif args.command == "create-group":
            asyncio.run(_run_create_group(args.config, name=args.name, description=args.description))
        elif args.command == "add-group-member":
            asyncio.run(_run_add_group_member(args.config, group_id=args.group_id, subject=args.subject))
        elif args.command == "grant-integration":
            asyncio.run(_run_grant_integration(args.config, group_id=args.group_id, upstream_id=args.upstream_id))
        elif args.command == "grant-platform":
            asyncio.run(_run_grant_platform(args.config, group_id=args.group_id, permission=args.permission))
        else:
            raise SystemExit(f"Unknown command: {args.command}")
    except KeyboardInterrupt:
        raise SystemExit(130) from None


if __name__ == "__main__":
    main()
