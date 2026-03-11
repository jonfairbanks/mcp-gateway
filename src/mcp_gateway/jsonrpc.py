from __future__ import annotations

import json
from typing import Any, Dict, Optional

import orjson


def json_dumps(value: Any) -> str:
    return orjson.dumps(value).decode("utf-8")


def normalize_params(params: Any) -> str:
    return json.dumps(params, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


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
