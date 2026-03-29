from __future__ import annotations

import pytest

from mcp_gateway.config import load_config


def test_streamable_http_defaults_to_concurrent_requests(tmp_path) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
gateway:
  api_key: "secret"
upstreams:
  - id: "remote"
    transport: "streamable_http"
    endpoint: "https://example.com/mcp"
""".strip(),
        encoding="utf-8",
    )

    config = load_config(str(config_file))
    assert len(config.upstreams) == 1
    assert config.gateway.auth_mode == "single_shared"
    assert config.gateway.allow_unauthenticated is False
    assert config.gateway.bootstrap_admin_api_key == ""
    assert config.gateway.public_tools_catalog is False
    assert config.gateway.public_metrics is False
    assert config.gateway.tracing_enabled is False
    assert config.gateway.readiness_mode == "any"
    assert config.gateway.required_ready_upstreams == []
    assert config.gateway.readiness_min_healthy_upstreams is None
    assert config.gateway.readiness_min_healthy_percent is None
    assert config.logging.extra_redact_fields == []
    assert config.logging.store_request_bodies is False
    assert config.logging.store_response_bodies is False
    assert config.cache.allowed_tools == []
    assert config.upstreams[0].http_serialize_requests is False


def test_streamable_http_can_force_serialized_requests(tmp_path) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
gateway:
  api_key: "secret"
upstreams:
  - id: "remote"
    transport: "streamable_http"
    endpoint: "https://example.com/mcp"
    http_serialize_requests: true
""".strip(),
        encoding="utf-8",
    )

    config = load_config(str(config_file))
    assert len(config.upstreams) == 1
    assert config.upstreams[0].http_serialize_requests is True


def test_rejects_legacy_http_sse_transport(tmp_path) -> None:
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

    with pytest.raises(ValueError, match="http_sse"):
        load_config(str(config_file))


def test_loads_unauthenticated_flag_and_extra_redact_fields(tmp_path) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
gateway:
  allow_unauthenticated: true
logging:
  extra_redact_fields:
    - custom_secret
upstreams:
  - id: "remote"
    transport: "streamable_http"
    endpoint: "https://example.com/mcp"
""".strip(),
        encoding="utf-8",
    )

    config = load_config(str(config_file))
    assert config.gateway.allow_unauthenticated is True
    assert config.logging.extra_redact_fields == ["custom_secret"]


def test_loads_public_tools_catalog_flag(tmp_path) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
gateway:
  api_key: "secret"
  public_tools_catalog: true
upstreams:
  - id: "remote"
    transport: "streamable_http"
    endpoint: "https://example.com/mcp"
""".strip(),
        encoding="utf-8",
    )

    config = load_config(str(config_file))
    assert config.gateway.public_tools_catalog is True


def test_loads_public_metrics_flag(tmp_path) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
gateway:
  api_key: "secret"
  public_metrics: true
upstreams:
  - id: "remote"
    transport: "streamable_http"
    endpoint: "https://example.com/mcp"
""".strip(),
        encoding="utf-8",
    )

    config = load_config(str(config_file))
    assert config.gateway.public_metrics is True


def test_loads_postgres_auth_mode_and_bootstrap_key(tmp_path) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
gateway:
  auth_mode: "postgres_api_keys"
  bootstrap_admin_api_key: "bootstrap-secret"
upstreams:
  - id: "remote"
    transport: "streamable_http"
    endpoint: "https://example.com/mcp"
""".strip(),
        encoding="utf-8",
    )

    config = load_config(str(config_file))
    assert config.gateway.auth_mode == "postgres_api_keys"
    assert config.gateway.bootstrap_admin_api_key == "bootstrap-secret"


@pytest.mark.parametrize("auth_mode", ["dual_migration", "oidc_jwt", "nope"])
def test_rejects_unknown_auth_mode(tmp_path, auth_mode: str) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        f"""
gateway:
  auth_mode: "{auth_mode}"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="gateway.auth_mode"):
        load_config(str(config_file))


def test_load_config_expands_required_env_refs(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("MCP_GATEWAY_API_KEY", "env-secret")
    monkeypatch.setenv("NOTION_TOKEN", "ntn_env_token")
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
gateway:
  api_key: "${MCP_GATEWAY_API_KEY}"
upstreams:
  - id: "notion"
    transport: "stdio"
    command: "npx"
    args:
      - "-y"
      - "@notionhq/notion-mcp-server"
    env:
      NOTION_TOKEN: "${NOTION_TOKEN}"
""".strip(),
        encoding="utf-8",
    )

    config = load_config(str(config_file))
    assert config.gateway.api_key == "env-secret"
    assert config.upstreams[0].env["NOTION_TOKEN"] == "ntn_env_token"


def test_load_config_uses_default_for_missing_or_empty_env_refs(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("MCP_GATEWAY_API_KEY", raising=False)
    monkeypatch.setenv("UPSTREAM_CWD", "")
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
gateway:
  api_key: "${MCP_GATEWAY_API_KEY:-fallback-secret}"
upstreams:
  - id: "remote"
    transport: "stdio"
    command: "npx"
    cwd: "${UPSTREAM_CWD:-/tmp/mcp-gateway}"
""".strip(),
        encoding="utf-8",
    )

    config = load_config(str(config_file))
    assert config.gateway.api_key == "fallback-secret"
    assert config.upstreams[0].cwd == "/tmp/mcp-gateway"


def test_rejects_invalid_bearer_token_env_var_name(tmp_path) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
gateway:
  api_key: "secret"
upstreams:
  - id: "github"
    transport: "streamable_http"
    endpoint: "https://example.com/mcp"
    bearer_token_env_var: "github_pat_example-token"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Invalid environment variable name"):
        load_config(str(config_file))


def test_load_config_rejects_missing_required_env_refs(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("MCP_GATEWAY_API_KEY", raising=False)
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
gateway:
  api_key: "${MCP_GATEWAY_API_KEY}"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="MCP_GATEWAY_API_KEY"):
        load_config(str(config_file))


def test_rejects_non_mapping_env_block(tmp_path) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
gateway:
  api_key: "secret"
upstreams:
  - id: "aws-mcp"
    transport: "stdio"
    command: "uvx"
    env:
      - AWS_ACCESS_KEY_ID=abc
      - AWS_SECRET_ACCESS_KEY=def
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"upstreams\[aws-mcp\]\.env must be a mapping"):
        load_config(str(config_file))


def test_rejects_non_mapping_http_headers_block(tmp_path) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
gateway:
  api_key: "secret"
upstreams:
  - id: "github"
    transport: "streamable_http"
    endpoint: "https://example.com/mcp"
    http_headers:
      - Authorization=Bearer token
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"upstreams\[github\]\.http_headers must be a mapping"):
        load_config(str(config_file))


def test_loads_required_readiness_mode(tmp_path) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
gateway:
  api_key: "secret"
  readiness_mode: "required"
  required_ready_upstreams:
    - "github"
upstreams:
  - id: "github"
    transport: "streamable_http"
    endpoint: "https://example.com/mcp"
""".strip(),
        encoding="utf-8",
    )

    config = load_config(str(config_file))
    assert config.gateway.readiness_mode == "required"
    assert config.gateway.required_ready_upstreams == ["github"]


def test_rejects_required_readiness_mode_without_required_upstreams(tmp_path) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
gateway:
  api_key: "secret"
  readiness_mode: "required"
upstreams:
  - id: "github"
    transport: "streamable_http"
    endpoint: "https://example.com/mcp"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="required_ready_upstreams must not be empty"):
        load_config(str(config_file))


def test_rejects_required_readiness_mode_with_unknown_upstream(tmp_path) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
gateway:
  api_key: "secret"
  readiness_mode: "required"
  required_ready_upstreams:
    - "missing"
upstreams:
  - id: "github"
    transport: "streamable_http"
    endpoint: "https://example.com/mcp"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="references unknown upstream"):
        load_config(str(config_file))


def test_rejects_threshold_readiness_mode_without_threshold_config(tmp_path) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
gateway:
  api_key: "secret"
  readiness_mode: "threshold"
upstreams:
  - id: "github"
    transport: "streamable_http"
    endpoint: "https://example.com/mcp"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="requires gateway.readiness_min_healthy_upstreams or gateway.readiness_min_healthy_percent"):
        load_config(str(config_file))


def test_rejects_streamable_http_upstream_without_endpoint(tmp_path) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
gateway:
  api_key: "secret"
upstreams:
  - id: "github"
    transport: "streamable_http"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"upstreams\[github\]\.endpoint is required"):
        load_config(str(config_file))


def test_rejects_stdio_upstream_without_command(tmp_path) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
gateway:
  api_key: "secret"
upstreams:
  - id: "context7"
    transport: "stdio"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"upstreams\[context7\]\.command is required"):
        load_config(str(config_file))


def test_rejects_duplicate_upstream_ids(tmp_path) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
gateway:
  api_key: "secret"
upstreams:
  - id: "github"
    transport: "streamable_http"
    endpoint: "https://example.com/one"
  - id: "github"
    transport: "streamable_http"
    endpoint: "https://example.com/two"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Duplicate upstream id"):
        load_config(str(config_file))


def test_rejects_overlapping_tool_routes_across_upstreams(tmp_path) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
gateway:
  api_key: "secret"
upstreams:
  - id: "github"
    transport: "streamable_http"
    endpoint: "https://example.com/github"
    tool_routes:
      - "github."
  - id: "github-admin"
    transport: "streamable_http"
    endpoint: "https://example.com/github-admin"
    tool_routes:
      - "github.admin."
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Ambiguous overlapping tool routes"):
        load_config(str(config_file))


def test_aggregates_multiple_config_validation_errors(tmp_path) -> None:
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        """
gateway:
  api_key: "secret"
  listen_port: 0
upstreams:
  - id: "broken"
    transport: "streamable_http"
    timeout_ms: 0
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError) as exc:
        load_config(str(config_file))

    message = str(exc.value)
    assert "gateway.listen_port must be between 1 and 65535" in message
    assert "upstreams[broken].endpoint is required when transport is 'streamable_http'" in message
    assert "upstreams[broken].timeout_ms must be greater than 0" in message
