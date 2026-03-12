from __future__ import annotations

from mcp_gateway.config import load_config


def test_http_sse_defaults_to_concurrent_requests(tmp_path) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
gateway:
  api_key: "secret"
upstreams:
  - id: "remote"
    transport: "http_sse"
    endpoint: "https://example.com/mcp"
""".strip(),
        encoding="utf-8",
    )

    config = load_config(str(config_file))
    assert len(config.upstreams) == 1
    assert config.upstreams[0].http_serialize_requests is False


def test_http_sse_can_force_serialized_requests(tmp_path) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
gateway:
  api_key: "secret"
upstreams:
  - id: "remote"
    transport: "http_sse"
    endpoint: "https://example.com/mcp"
    http_serialize_requests: true
""".strip(),
        encoding="utf-8",
    )

    config = load_config(str(config_file))
    assert len(config.upstreams) == 1
    assert config.upstreams[0].http_serialize_requests is True
