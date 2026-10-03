# CHANGELOG

<!-- version list -->

## v1.0.0 (2026-10-03)

### Bug Fixes

- **auth**: Enforce grants for non-tool methods
  ([`457397a`](https://github.com/jonfairbanks/mcp-gateway/commit/457397ae606053071384f90f2b8e17bf384164ea))

- **auth**: Rate limit clients before authentication
  ([`35e40d6`](https://github.com/jonfairbanks/mcp-gateway/commit/35e40d66f0f26f24d594a97a5a5c52c6c58a2e9e))

- **auth**: Require explicit gateway API key
  ([`b86e554`](https://github.com/jonfairbanks/mcp-gateway/commit/b86e554603d1b8e84ce8f42bb95788802b4b27a6))

- **cache**: Preserve request IDs on cache hits
  ([`61ccdbc`](https://github.com/jonfairbanks/mcp-gateway/commit/61ccdbcb54cc41e39731a9440e7024809ff9d148))

- **http**: Bound and rate limit JSON-RPC batches
  ([`ad788c4`](https://github.com/jonfairbanks/mcp-gateway/commit/ad788c4a1267ae021b087484303c15863830f2dc))

- **metrics**: Bound method label cardinality
  ([`bea9e01`](https://github.com/jonfairbanks/mcp-gateway/commit/bea9e01f66df7a24458477f536092f4618860be9))

- **security**: Address Codex Security findings
  ([`4d78c7c`](https://github.com/jonfairbanks/mcp-gateway/commit/4d78c7ce6c6e547bfa2750c46f3a287c8dbd0f68))

- **upstreams**: Preserve cancellation on Python 3.11
  ([`970a033`](https://github.com/jonfairbanks/mcp-gateway/commit/970a03358e5e624662bf4a8b210ffd020357c812))

- **upstreams**: Restore stdio sessions and align SSE response limits
  ([`8adb340`](https://github.com/jonfairbanks/mcp-gateway/commit/8adb340de35cf25c3175c8cb24e5bec2db7167c6))

### Continuous Integration

- **release**: Automate weekly develop promotion
  ([`1db9f13`](https://github.com/jonfairbanks/mcp-gateway/commit/1db9f130408fd0e090df99fc4d7eff1924eadadc))

### Documentation

- Recommend compact discovery endpoint
  ([`a56f3ff`](https://github.com/jonfairbanks/mcp-gateway/commit/a56f3ffbd61e1be06d7d42197f3a1ad677c0aacf))

- Remove unused client migration guidance
  ([`d9c4c19`](https://github.com/jonfairbanks/mcp-gateway/commit/d9c4c195345668e73c1974ec281d3adc095b4535))

- **config**: Enable compact discovery in sample
  ([`437c932`](https://github.com/jonfairbanks/mcp-gateway/commit/437c932170e5c4b18fd09ac352bd8723d86c29ec))

### Features

- **discovery**: Add opt-in tool search endpoint
  ([`bde4e2a`](https://github.com/jonfairbanks/mcp-gateway/commit/bde4e2ac1751b09f80ccf7f210b7bad05fff9c93))

- **discovery**: Always expose compact discovery endpoint
  ([`858e135`](https://github.com/jonfairbanks/mcp-gateway/commit/858e135e6781804f6ed915a152869d452a306157))

- **discovery**: Make compact discovery the default endpoint
  ([`60b9d02`](https://github.com/jonfairbanks/mcp-gateway/commit/60b9d02ea4016c67c1c4db92f0d71cf007eb6bf2))

### Refactoring

- **auth**: Replace RBAC with owner API keys
  ([`282eb8b`](https://github.com/jonfairbanks/mcp-gateway/commit/282eb8b916dbd208ba16e41de0ffed28dcdaef48))

### Breaking Changes

- **auth**: User/group/grant commands and key self-service HTTP endpoints are removed. Apply
  schema.sql and migrations/001_owner_api_keys.sql before deploying postgres_api_keys mode. Only
  eligible legacy admin keys are imported; key management now uses create-api-key, list-api-keys,
  and revoke-api-key.

- **discovery**: /mcp now exposes compact search and call wrappers. Move direct-call clients to
  /mcp/full and compact clients from the removed /mcp/discovery route to /mcp.
