BEGIN;

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

DO $$
BEGIN
  IF to_regclass('gateway_api_keys') IS NOT NULL AND to_regclass('gateway_users') IS NOT NULL THEN
    INSERT INTO gateway_access_keys (
      id,
      key_name,
      key_prefix,
      key_hash,
      is_active,
      created_at,
      last_used_at,
      expires_at,
      revoked_at
    )
    SELECT
      keys.id,
      keys.key_name,
      keys.key_prefix,
      keys.key_hash,
      keys.is_active,
      keys.created_at,
      keys.last_used_at,
      keys.expires_at,
      keys.revoked_at
    FROM gateway_api_keys AS keys
    JOIN gateway_users AS users ON users.id = keys.user_id
    WHERE keys.is_active = TRUE
      AND keys.revoked_at IS NULL
      AND (keys.expires_at IS NULL OR keys.expires_at > now())
      AND users.is_active = TRUE
      AND lower(trim(users.role)) = 'admin'
    ON CONFLICT DO NOTHING;
  END IF;
END $$;

COMMIT;
