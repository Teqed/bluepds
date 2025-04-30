CREATE TABLE IF NOT EXISTS oauth_par_requests (
    request_uri TEXT PRIMARY KEY NOT NULL,
    client_id TEXT NOT NULL,
    response_type TEXT NOT NULL,
    code_challenge TEXT NOT NULL,
    code_challenge_method TEXT NOT NULL,
    state TEXT,
    login_hint TEXT,
    scope TEXT,
    redirect_uri TEXT,
    response_mode TEXT,
    display TEXT,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS oauth_authorization_codes (
    code TEXT PRIMARY KEY NOT NULL,
    client_id TEXT NOT NULL,
    subject TEXT NOT NULL,
    code_challenge TEXT NOT NULL,
    code_challenge_method TEXT NOT NULL,
    redirect_uri TEXT NOT NULL,
    scope TEXT,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    used BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE TABLE IF NOT EXISTS oauth_refresh_tokens (
    token TEXT PRIMARY KEY NOT NULL,
    client_id TEXT NOT NULL,
    subject TEXT NOT NULL,
    dpop_thumbprint TEXT NOT NULL,
    scope TEXT,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    revoked BOOLEAN NOT NULL DEFAULT FALSE
);
