from __future__ import annotations

from typing import Dict, List, Optional

from .config import UpstreamConfig


def build_routes(upstreams: List[UpstreamConfig]) -> Dict[str, UpstreamConfig]:
    routes: Dict[str, UpstreamConfig] = {}
    for upstream in upstreams:
        for prefix in upstream.tool_routes:
            routes[prefix] = upstream
    return routes


def select_upstream(
    upstreams: List[UpstreamConfig],
    routes: Dict[str, UpstreamConfig],
    tool_name: Optional[str],
) -> Optional[UpstreamConfig]:
    if not tool_name:
        return upstreams[0] if upstreams else None

    matched_upstream: Optional[UpstreamConfig] = None
    matched_prefix_length = -1
    for prefix, upstream in routes.items():
        if tool_name.startswith(prefix) and len(prefix) > matched_prefix_length:
            matched_upstream = upstream
            matched_prefix_length = len(prefix)
    if matched_upstream:
        return matched_upstream

    namespace = tool_name.split(".")[0]
    for upstream in upstreams:
        if upstream.id == namespace or upstream.name == namespace:
            return upstream

    return upstreams[0] if upstreams else None
