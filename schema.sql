CREATE TABLE IF NOT EXISTS mcp_requests (
  id UUID PRIMARY KEY,
  timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
  upstream_id TEXT,
  method TEXT NOT NULL,
  tool_name TEXT,
  params JSONB,
  raw_request JSONB,
  client_id TEXT,
  cache_key TEXT
);

CREATE TABLE IF NOT EXISTS mcp_responses (
  id UUID PRIMARY KEY,
  request_id UUID REFERENCES mcp_requests(id) ON DELETE CASCADE,
  timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
  success BOOLEAN NOT NULL,
  latency_ms INTEGER NOT NULL,
  cache_hit BOOLEAN NOT NULL,
  response JSONB
);

CREATE TABLE IF NOT EXISTS mcp_denials (
  id UUID PRIMARY KEY,
  request_id UUID REFERENCES mcp_requests(id) ON DELETE CASCADE,
  timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
  upstream_id TEXT,
  tool_name TEXT,
  reason TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS mcp_cache (
  cache_key TEXT PRIMARY KEY,
  response JSONB NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_mcp_cache_expires_at ON mcp_cache (expires_at);
