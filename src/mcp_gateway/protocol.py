from __future__ import annotations

from typing import Any

CURRENT_PROTOCOL_VERSION = "2025-11-25"
DEFAULT_HTTP_PROTOCOL_VERSION = CURRENT_PROTOCOL_VERSION
SUPPORTED_PROTOCOL_VERSIONS = (CURRENT_PROTOCOL_VERSION,)
SUPPORTED_PROTOCOL_VERSION_SET = frozenset(SUPPORTED_PROTOCOL_VERSIONS)


def is_supported_protocol_version(value: Any) -> bool:
    return isinstance(value, str) and value in SUPPORTED_PROTOCOL_VERSION_SET


def negotiate_protocol_version(requested_version: Any) -> str:
    if is_supported_protocol_version(requested_version):
        return requested_version
    return CURRENT_PROTOCOL_VERSION
