CREATE TABLE IF NOT EXISTS mcp_requests (
  id UUID PRIMARY KEY,
  timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
  upstream_id TEXT,
  method TEXT NOT NULL,
  tool_name TEXT,
  params JSONB,
  raw_request JSONB,
  client_id TEXT,
  auth_api_key_id UUID,
  auth_subject TEXT,
  auth_scheme TEXT,
  cache_key TEXT
);

ALTER TABLE mcp_requests ADD COLUMN IF NOT EXISTS auth_api_key_id UUID;
ALTER TABLE mcp_requests ADD COLUMN IF NOT EXISTS auth_subject TEXT;
ALTER TABLE mcp_requests ADD COLUMN IF NOT EXISTS auth_scheme TEXT;

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

CREATE TABLE IF NOT EXISTS gateway_rate_limits (
  scope_key TEXT NOT NULL,
  window_started_at TIMESTAMPTZ NOT NULL,
  request_count INTEGER NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (scope_key, window_started_at)
);

CREATE INDEX IF NOT EXISTS idx_gateway_rate_limits_expires_at ON gateway_rate_limits (expires_at);

CREATE TABLE IF NOT EXISTS gateway_access_keys (
  id UUID PRIMARY KEY,
  key_name TEXT NOT NULL,
  key_prefix TEXT NOT NULL UNIQUE,
  key_hash TEXT NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_used_at TIMESTAMPTZ,
  expires_at TIMESTAMPTZ,
  revoked_at TIMESTAMPTZ
);
