from __future__ import annotations

import argparse
import asyncio
import os

from .config import load_config
from .gateway import Gateway
from .logging import Logger
from .postgres import PostgresStore
from .server_http import HttpServer


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mcp-gateway")
    subparsers = parser.add_subparsers(dest="command", required=True)

    serve_parser = subparsers.add_parser("serve")
    serve_parser.add_argument("--config", required=True)

    return parser


async def _run_http(config_path: str) -> None:
    config = load_config(config_path)
    logger = Logger(stdout_json=config.logging.stdout_json)
    dsn = os.getenv("DATABASE_URL", "")
    if not dsn:
        logger.warn(
            "database_disabled",
            reason="DATABASE_URL not set",
            impact="postgres logging and persistent cache are disabled",
        )
    store = PostgresStore(dsn)
    await store.start()
    gateway = Gateway(config, store, logger)
    try:
        await gateway.warmup()
        server = HttpServer(config, gateway, logger)
        await server.run()
    finally:
        await gateway.close()
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
        raise SystemExit(130)


if __name__ == "__main__":
    main()
