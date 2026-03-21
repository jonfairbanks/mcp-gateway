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
  auth_subject TEXT,
  auth_scheme TEXT,
  auth_group_names JSONB NOT NULL DEFAULT '[]'::jsonb,
  authorized_upstream_id TEXT,
  cache_key TEXT
);

ALTER TABLE mcp_requests ADD COLUMN IF NOT EXISTS auth_user_id UUID;
ALTER TABLE mcp_requests ADD COLUMN IF NOT EXISTS auth_api_key_id UUID;
ALTER TABLE mcp_requests ADD COLUMN IF NOT EXISTS auth_role TEXT;
ALTER TABLE mcp_requests ADD COLUMN IF NOT EXISTS auth_subject TEXT;
ALTER TABLE mcp_requests ADD COLUMN IF NOT EXISTS auth_scheme TEXT;
ALTER TABLE mcp_requests ADD COLUMN IF NOT EXISTS auth_group_names JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE mcp_requests ADD COLUMN IF NOT EXISTS authorized_upstream_id TEXT;

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

CREATE TABLE IF NOT EXISTS gateway_users (
  id UUID PRIMARY KEY,
  subject TEXT NOT NULL UNIQUE,
  display_name TEXT NOT NULL,
  role TEXT,
  issuer TEXT,
  email TEXT,
  auth_source TEXT,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  last_seen_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE gateway_users ADD COLUMN IF NOT EXISTS issuer TEXT;
ALTER TABLE gateway_users ADD COLUMN IF NOT EXISTS email TEXT;
ALTER TABLE gateway_users ADD COLUMN IF NOT EXISTS auth_source TEXT;
ALTER TABLE gateway_users ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ;
ALTER TABLE gateway_users ALTER COLUMN role DROP NOT NULL;
UPDATE gateway_users SET role = NULL WHERE role IN ('member', 'viewer');

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

CREATE TABLE IF NOT EXISTS gateway_groups (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  description TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gateway_group_memberships (
  group_id UUID NOT NULL REFERENCES gateway_groups(id) ON DELETE CASCADE,
  user_id UUID NOT NULL REFERENCES gateway_users(id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (group_id, user_id)
);

CREATE TABLE IF NOT EXISTS gateway_group_integration_grants (
  group_id UUID NOT NULL REFERENCES gateway_groups(id) ON DELETE CASCADE,
  upstream_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (group_id, upstream_id)
);

CREATE TABLE IF NOT EXISTS gateway_group_platform_grants (
  group_id UUID NOT NULL REFERENCES gateway_groups(id) ON DELETE CASCADE,
  permission TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (group_id, permission)
);

CREATE TABLE IF NOT EXISTS gateway_policy_state (
  singleton_key TEXT PRIMARY KEY,
  policy_revision BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO gateway_policy_state (singleton_key, policy_revision)
VALUES ('default', 0)
ON CONFLICT (singleton_key) DO NOTHING;
