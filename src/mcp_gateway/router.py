from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from .config import UpstreamConfig


@dataclass
class RouteResult:
    upstream: UpstreamConfig


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

    for prefix, upstream in routes.items():
        if tool_name.startswith(prefix):
            return upstream

    namespace = tool_name.split(".")[0]
    for upstream in upstreams:
        if upstream.id == namespace or upstream.name == namespace:
            return upstream

    return upstreams[0] if upstreams else None
