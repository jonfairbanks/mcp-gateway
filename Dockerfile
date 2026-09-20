# syntax=docker/dockerfile:1.7
FROM node:24.21.0-bookworm-slim AS upstreams
WORKDIR /opt/mcp-upstreams
COPY upstreams/package.json upstreams/package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci --omit=dev --ignore-scripts --no-audit --no-fund

FROM python:3.11-slim-bookworm

COPY --from=upstreams /usr/local/bin/node /usr/local/bin/node
COPY --from=upstreams /usr/local/lib/node_modules/npm /usr/local/lib/node_modules/npm
RUN ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm && \
    ln -s ../lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx
COPY --from=upstreams /opt/mcp-upstreams /opt/mcp-upstreams
ENV PATH="/opt/mcp-upstreams/node_modules/.bin:${PATH}"

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt/lists,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install uv

COPY pyproject.toml README.md /app/
RUN python - <<'PY' > /tmp/requirements.txt
import tomllib

with open("/app/pyproject.toml", "rb") as f:
    project = tomllib.load(f)["project"]

for dependency in project["dependencies"]:
    print(dependency)
PY
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r /tmp/requirements.txt

COPY src /app/src
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --no-deps .

COPY config.example.yaml schema.sql /app/

RUN groupadd --gid 10001 app && \
    useradd --uid 10001 --gid app --create-home --shell /usr/sbin/nologin app && \
    chown -R app:app /app

USER app:app

EXPOSE 8080

ENTRYPOINT ["mcp-gateway"]
CMD ["serve", "--config", "/app/config.example.yaml"]
