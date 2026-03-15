CREATE TABLE IF NOT EXISTS mcp_requests (
  id UUID PRIMARY KEY,
  timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
  upstream_id TEXT,
  method TEXT NOT NULL,
  tool_name TEXT,
  params JSONB,
  raw_request JSONB,
  client_id TEXT,
  auth_user_id UUID,
  auth_api_key_id UUID,
  auth_role TEXT,
  cache_key TEXT
);

ALTER TABLE mcp_requests ADD COLUMN IF NOT EXISTS auth_user_id UUID;
ALTER TABLE mcp_requests ADD COLUMN IF NOT EXISTS auth_api_key_id UUID;
ALTER TABLE mcp_requests ADD COLUMN IF NOT EXISTS auth_role TEXT;

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

CREATE TABLE IF NOT EXISTS gateway_users (
  id UUID PRIMARY KEY,
  subject TEXT NOT NULL UNIQUE,
  display_name TEXT NOT NULL,
  role TEXT NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gateway_api_keys (
  id UUID PRIMARY KEY,
  user_id UUID NOT NULL REFERENCES gateway_users(id) ON DELETE CASCADE,
  key_name TEXT NOT NULL,
  key_prefix TEXT NOT NULL UNIQUE,
  key_hash TEXT NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_used_at TIMESTAMPTZ,
  expires_at TIMESTAMPTZ,
  revoked_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_gateway_api_keys_user_id ON gateway_api_keys (user_id);
CREATE INDEX IF NOT EXISTS idx_gateway_api_keys_key_prefix ON gateway_api_keys (key_prefix);
