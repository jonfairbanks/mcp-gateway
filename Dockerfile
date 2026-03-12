FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY pyproject.toml README.md /app/
COPY src /app/src
RUN pip install --no-cache-dir .

COPY config.example.yaml schema.sql /app/

RUN groupadd --system --gid 10001 app && \
    useradd --system --uid 10001 --gid app --create-home app && \
    chown -R app:app /app

USER app:app

EXPOSE 8080

CMD ["mcp-gateway", "serve", "--config", "/app/config.example.yaml"]
