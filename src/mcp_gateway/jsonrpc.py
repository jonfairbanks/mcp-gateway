from __future__ import annotations

import json
from typing import Any, Dict, Optional

import orjson


def json_dumps(value: Any) -> str:
    return orjson.dumps(value).decode("utf-8")


def _normalize_cache_params(value: Any) -> Any:
    if isinstance(value, dict):
        normalized: Dict[str, Any] = {}
        for key, item in value.items():
            if key == "_meta" and isinstance(item, dict):
                meta = {meta_key: _normalize_cache_params(meta_value) for meta_key, meta_value in item.items() if meta_key != "progressToken"}
                if meta:
                    normalized[key] = meta
                continue
            normalized[key] = _normalize_cache_params(item)
        return normalized
    if isinstance(value, list):
        return [_normalize_cache_params(item) for item in value]
    return value


def normalize_params(params: Any) -> str:
    return json.dumps(_normalize_cache_params(params), sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def make_error_response(request_id: Any, code: int, message: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "jsonrpc": "2.0",
        "id": request_id,
        "error": {
            "code": code,
            "message": message,
        },
    }
    if data is not None:
        payload["error"]["data"] = data
    return payload


def make_result_response(request_id: Any, result: Any) -> Dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "result": result,
    }
