# AGENTS.md

## Repository Overview

This repository is a Python 3.11+ MCP gateway service packaged with `setuptools`.

- Source code lives under `src/mcp_gateway/`
- Tests live under `tests/`
- Runtime configuration and operational docs live in `README.md` and `docs/`

## Setup

Use an editable install for local development so the package, runtime dependencies, and test tools are available together:

```bash
python -m pip install -e ".[dev]"
```

If you need to run the gateway locally, the README includes the full bootstrap flow, including `config.example.yaml`, `schema.sql`, and the required environment variables.

## Test Commands

Run the default test suite:

```bash
python -m pytest
```

Run only the Postgres-backed integration test:

```bash
docker compose up -d postgres
export DATABASE_URL='postgresql://postgres:postgres@localhost:5432/mcp_gateway'
python -m pytest tests/test_integration_app.py
```

Run linting:

```bash
python -m ruff check .
```

The CI workflow runs the same lint and test commands in `.github/workflows/pr-checks.yml`.

## Coding Conventions

- Target Python 3.11.
- Follow Ruff rules configured in `pyproject.toml`:
  - line length: 120
  - lint selection: `E`, `F`, `I`, `B`, `BLE`
  - `E501` is ignored
- Keep imports sorted and use standard Python formatting conventions.
- Prefer small, focused modules under `src/mcp_gateway/`.
- Keep tests in `tests/` and name them `test_*.py`.
- Use `python -m pytest` and `python -m ruff check .` before considering a change complete.
- Preserve existing environment variable behavior, especially the `DATABASE_URL` fallback used by integration tests.

## Notes For Agents

- Check `README.md` for runtime assumptions before changing startup or config behavior.
- Check `docs/development.md` before changing test setup or integration-test behavior.
- If you touch Postgres, auth, or HTTP routing code, add or update tests in the matching area rather than relying only on the full suite.
