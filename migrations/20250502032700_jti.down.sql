DROP INDEX IF EXISTS idx_jtis_expires_at;
DROP INDEX IF EXISTS idx_refresh_tokens_expires_at;
DROP INDEX IF EXISTS idx_auth_codes_expires_at;
DROP INDEX IF EXISTS idx_par_expires_at;

DROP TABLE IF EXISTS oauth_used_jtis;
