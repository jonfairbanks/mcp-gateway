FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY pyproject.toml README.md /app/
COPY src /app/src
RUN pip install --no-cache-dir .

COPY config.yaml schema.sql /app/

EXPOSE 8080

CMD ["mcp-gateway", "serve-http", "--config", "/app/config.yaml"]
