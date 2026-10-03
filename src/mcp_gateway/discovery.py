"""Bounded, local tool discovery for clients without native deferred loading."""
from __future__ import annotations

import copy
import math
import re
from collections import Counter
from typing import Any

import orjson

SEARCH_TOOL = "gateway_search_tools"
CALL_TOOL = "gateway_call_tool"
RESERVED_NAMES = frozenset({SEARCH_TOOL, CALL_TOOL})
DISCOVERY_PATH = "/mcp"
FULL_PATH = "/mcp/full"
MAX_QUERY_LENGTH = 512
MAX_RESULTS = 10
MAX_RESULT_BYTES = 64 * 1024
STOP_WORDS = frozenset("a an and are as at be by for from how i in is it me my of on or that the this to using with".split())
ALIASES = {"pr": "pull request", "repo": "repository", "docs": "documentation", "k8s": "kubernetes"}
INSTRUCTIONS = (
    "Use gateway_search_tools to find a tool and its inputSchema, then gateway_call_tool to execute it. "
    "Narrow searches with an upstream integration ID or exact tool name. If a search misses, try more specific terms. "
    "Existing gateway policies apply. This endpoint does not execute scripts or arbitrary JSON-RPC methods."
)


def discovery_tools(upstream_ids: list[str]) -> list[dict[str, Any]]:
    return [
        {
            "name": SEARCH_TOOL,
            "description": "Find tools by task or exact name. Returns input schemas and annotations. Use upstream to narrow results.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "minLength": 1, "maxLength": MAX_QUERY_LENGTH},
                    "limit": {"type": "integer", "minimum": 1, "maximum": MAX_RESULTS, "default": 5},
                    "upstream": {"type": "string", **({"enum": upstream_ids} if upstream_ids else {})},
                },
                "required": ["query"],
                "additionalProperties": False,
            },
            "annotations": {"readOnlyHint": True, "openWorldHint": False},
        },
        {
            "name": CALL_TOOL,
            "description": "Call a discovered tool with arguments matching its inputSchema. Tool-specific side effects and gateway policies apply.",
            "inputSchema": {
                "type": "object",
                "properties": {"name": {"type": "string", "minLength": 1}, "arguments": {"type": "object"}},
                "required": ["name", "arguments"],
                "additionalProperties": False,
            },
            # The target can write or interact with external services; do not imply read-only execution.
            "annotations": {"readOnlyHint": False, "destructiveHint": True, "openWorldHint": True},
        },
    ]


def _terms(text: str) -> list[str]:
    text = re.sub(r"([a-z])([A-Z])", r"\1 \2", text)
    result = []
    for word in re.findall(r"[a-z0-9]+", text.lower()):
        if word in STOP_WORDS:
            continue
        if len(word) > 4 and word.endswith("ies"):
            word = word[:-3] + "y"
        elif len(word) > 4 and word.endswith("s") and not word.endswith(("ss", "us")):
            word = word[:-1]
        result.append(word)
    return result


def _enum_terms(value: Any) -> list[str]:
    result: list[str] = []
    if isinstance(value, dict):
        enum = value.get("enum", [])
        for item in enum if isinstance(enum, list) else []:
            if isinstance(item, str):
                result.extend(_terms(item))
        for key, item in value.items():
            if key != "enum":
                result.extend(_enum_terms(item))
    elif isinstance(value, list):
        for item in value:
            result.extend(_enum_terms(item))
    return result


def call_arguments(arguments: Any) -> tuple[str, dict[str, Any]]:
    if not isinstance(arguments, dict) or set(arguments) != {"name", "arguments"}:
        raise ValueError("Supply a tool name and an arguments object")
    name, params = arguments["name"], arguments["arguments"]
    if not isinstance(name, str) or not name.strip() or not isinstance(params, dict):
        raise ValueError("Supply a tool name and an arguments object")
    if name in RESERVED_NAMES:
        raise ValueError("Discovery wrappers cannot call themselves")
    return name, params


class ToolDiscoveryIndex:
    """One registry snapshot, published atomically with the gateway's routing table."""

    def __init__(self, tools: list[dict[str, Any]], sources: dict[str, str], upstream_ids: list[str]) -> None:
        self._tools = {
            tool["name"]: copy.deepcopy({key: tool[key] for key in ("name", "description", "inputSchema", "annotations") if key in tool})
            for tool in tools
        }
        self._sources = dict(sources)
        self._upstream_ids = frozenset(upstream_ids)
        self._documents = []
        frequency: Counter[str] = Counter()
        for name, tool in self._tools.items():
            description_text = tool.get("description", "")
            description = Counter(_terms(description_text if isinstance(description_text, str) else ""))
            title = set(_terms(name))
            schema = tool.get("inputSchema", {})
            properties = schema.get("properties", {}) if isinstance(schema, dict) else {}
            parameters = set(_terms(" ".join(properties))) if isinstance(properties, dict) else set()
            enums = set(_enum_terms(schema))
            provider = set(_terms(sources[name]))
            frequency.update(set(description) | title | parameters | enums | provider)
            self._documents.append((name, description, title, parameters, enums, provider))
        count = len(self._documents)
        self._idf = {word: math.log(1 + (count - number + 0.5) / (number + 0.5)) for word, number in frequency.items()}
        self._average_length = max(1, sum(sum(row[1].values()) for row in self._documents) / max(1, count))
        self._validators: dict[str, Any] = {}

    def contains(self, name: str) -> bool:
        return name in self._tools

    def validate_arguments(self, name: str, arguments: dict[str, Any]) -> None:
        # Native clients and index construction do not need the validation runtime.
        from jsonschema.exceptions import SchemaError
        from jsonschema.validators import validator_for
        from referencing import Registry
        from referencing.exceptions import Unresolvable

        if name not in self._tools:
            raise ValueError("Unknown or unavailable tool")
        try:
            if name not in self._validators:
                schema = self._tools[name].get("inputSchema", {})
                if not isinstance(schema, (dict, bool)):
                    raise ValueError("Tool input schema cannot be validated; use the /mcp/full endpoint")
                cls = validator_for(schema)
                cls.check_schema(schema)
                # Explicitly disable retrieval of remote $ref resources from upstream schemas.
                self._validators[name] = cls(schema, registry=Registry())
            error = next(self._validators[name].iter_errors(arguments), None)
        except (SchemaError, Unresolvable) as exc:
            raise ValueError("Tool input schema cannot be validated; use the /mcp/full endpoint") from exc
        if error is not None:
            # Validation messages can contain argument values; do not include them in errors or logs.
            raise ValueError("Arguments do not match the tool input schema")

    def search(self, arguments: Any) -> str:
        if not isinstance(arguments, dict) or set(arguments) - {"query", "limit", "upstream"}:
            raise ValueError("Invalid search arguments")
        query, limit, upstream = arguments.get("query"), arguments.get("limit", 5), arguments.get("upstream")
        if not isinstance(query, str) or not query.strip() or len(query) > MAX_QUERY_LENGTH:
            raise ValueError("Query must contain 1 to 512 characters")
        if type(limit) is not int or not 1 <= limit <= MAX_RESULTS:
            raise ValueError("Limit must be an integer from 1 to 10")
        if "upstream" in arguments and (not isinstance(upstream, str) or upstream not in self._upstream_ids):
            raise ValueError("Unknown upstream integration")
        words = set(_terms(query))
        for word in tuple(words):
            words.update(_terms(ALIASES.get(word, "")))
        scores = []
        for name, description, title, parameters, enums, provider in self._documents:
            if upstream is not None and self._sources[name] != upstream:
                continue
            length = sum(description.values())
            score = 0.0
            for word in words:
                tf = description[word]
                normalized = tf * 2.2 / (tf + 1.2 * (0.25 + 0.75 * length / self._average_length)) if tf else 0
                score += self._idf.get(word, 0) * (
                    normalized + 2.5 * (word in title) + 0.3 * (word in parameters)
                    + 1.5 * (word in enums) + (word in provider)
                )
            if query.strip().lower() == name.lower():
                score += 1000
            if score > 0:
                scores.append((score, name))
        scores.sort(key=lambda row: (-row[0], row[1]))
        result: dict[str, Any] = {"tools": []}
        omitted = 0
        for _, name in scores[:limit]:
            candidate = {"tools": result["tools"] + [self._tools[name]], "omitted_count": MAX_RESULTS,
                         "message": "Some schemas exceed the result budget; narrow the search or use /mcp/full."}
            if len(orjson.dumps(candidate)) <= MAX_RESULT_BYTES:
                result["tools"].append(self._tools[name])
            else:
                omitted += 1
        if omitted:
            result.update(omitted_count=omitted, message="Some schemas exceed the result budget; narrow the search or use /mcp/full.")
        return orjson.dumps(result).decode("utf-8")
