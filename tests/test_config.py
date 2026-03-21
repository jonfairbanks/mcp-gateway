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
    assert config.logging.extra_redact_fields == []
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
