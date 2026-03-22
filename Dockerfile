FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV CHROME_PATH=/usr/bin/chromium
ENV CHROME_BIN=/usr/bin/chromium
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    chromium \
    nodejs \
    npm \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv

COPY pyproject.toml README.md /app/
RUN python - <<'PY' > /tmp/requirements.txt
import tomllib

with open("/app/pyproject.toml", "rb") as f:
    project = tomllib.load(f)["project"]

for dependency in project["dependencies"]:
    print(dependency)
PY
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY src /app/src
RUN pip install --no-cache-dir --no-deps .

COPY config.example.yaml schema.sql /app/

RUN groupadd --gid 10001 app && \
    useradd --uid 10001 --gid app --create-home --shell /usr/sbin/nologin app && \
    chown -R app:app /app

USER app:app

EXPOSE 8080

CMD ["mcp-gateway", "serve", "--config", "/app/config.example.yaml"]
