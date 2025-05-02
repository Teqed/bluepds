-- Table for tracking used JTIs to prevent replay attacks
CREATE TABLE IF NOT EXISTS oauth_used_jtis (
    jti TEXT PRIMARY KEY NOT NULL,
    issuer TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL
);

-- Create indexes for faster lookups and cleanup
CREATE INDEX IF NOT EXISTS idx_par_expires_at ON oauth_par_requests(expires_at);
CREATE INDEX IF NOT EXISTS idx_auth_codes_expires_at ON oauth_authorization_codes(expires_at);
CREATE INDEX IF NOT EXISTS idx_refresh_tokens_expires_at ON oauth_refresh_tokens(expires_at);
CREATE INDEX IF NOT EXISTS idx_jtis_expires_at ON oauth_used_jtis(expires_at);
