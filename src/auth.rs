use anyhow::{Context as _, anyhow};
use atrium_crypto::{
    keypair::{Did as _, Secp256k1Keypair},
    verify::Verifier,
};
use axum::{extract::FromRequestParts, http::StatusCode};
use base64::Engine as _;
use sha2::{Digest, Sha256};

use crate::{AppState, Error, error::ErrorMessage};

/// Request extractor for authenticated users.
/// If specified in an API endpoint, this guarantees the API can only be called
/// by an authenticated user.
pub(crate) struct AuthenticatedUser {
    /// The DID of the authenticated user.
    did: String,
}

impl AuthenticatedUser {
    /// Get the DID of the authenticated user.
    pub(crate) fn did(&self) -> String {
        self.did.clone()
    }
}

impl FromRequestParts<AppState> for AuthenticatedUser {
    type Rejection = Error;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        // Check for authorization header (either for Bearer or DPoP tokens)
        let auth_header = parts
            .headers
            .get(axum::http::header::AUTHORIZATION)
            .ok_or_else(|| {
                Error::with_status(StatusCode::UNAUTHORIZED, anyhow!("no authorization header"))
            })?;

        let auth_str = auth_header.to_str().map_err(|_| {
            Error::with_status(
                StatusCode::UNAUTHORIZED,
                anyhow!("authorization header should be valid utf-8"),
            )
        })?;

        // Check for DPoP header
        let dpop_header = parts.headers.get("dpop");
        let has_dpop = dpop_header.is_some();

        // Handle different token types
        if auth_str.starts_with("Bearer ") || auth_str.starts_with("DPoP ") {
            let token = auth_str.splitn(2, ' ').nth(1).unwrap();
            if has_dpop {
                // Process DPoP token - the Authorization header contains the access token
                // and the DPoP header contains the proof
                let dpop_token = dpop_header.unwrap().to_str().map_err(|_| {
                    Error::with_status(
                        StatusCode::UNAUTHORIZED,
                        anyhow!("DPoP header should be valid utf-8"),
                    )
                })?;

                return validate_dpop_token(token, dpop_token, parts, state).await;
            } else {
                // Standard Bearer token
                return validate_bearer_token(token, state).await;
            }
        }

        // If we reach here, no valid authorization method was found
        Err(Error::with_status(
            StatusCode::UNAUTHORIZED,
            anyhow!("unsupported authorization method"),
        ))
    }
}

/// Validate a standard Bearer token
async fn validate_bearer_token(token: &str, state: &AppState) -> Result<AuthenticatedUser, Error> {
    // Validate JWT token
    let (typ, claims) = verify(&state.signing_key.did(), token)
        .map_err(|e| {
            Error::with_status(
                StatusCode::UNAUTHORIZED,
                e.context("failed to verify bearer token"),
            )
        })
        .context("token auth should verify")?;

    // Ensure this is an authentication token.
    if typ != "at+jwt" {
        return Err(Error::with_status(
            StatusCode::UNAUTHORIZED,
            anyhow!("invalid token type: {typ}"),
        ));
    }

    // Ensure we are in the audience field.
    if let Some(aud) = claims.get("aud").and_then(serde_json::Value::as_str) {
        if aud != format!("did:web:{}", state.config.host_name) {
            return Err(Error::with_status(
                StatusCode::UNAUTHORIZED,
                anyhow!("invalid audience: {aud}"),
            ));
        }
    }

    // Check token expiration
    if let Some(exp) = claims.get("exp").and_then(serde_json::Value::as_i64) {
        let now = chrono::Utc::now().timestamp();
        if now >= exp {
            return Err(Error::with_message(
                StatusCode::BAD_REQUEST,
                anyhow!("token has expired"),
                ErrorMessage::new("ExpiredToken", "Token has expired"),
            ));
        }
    }

    // Extract subject (DID)
    if let Some(did) = claims.get("sub").and_then(serde_json::Value::as_str) {
        let _status = sqlx::query_scalar!(r#"SELECT status FROM accounts WHERE did = ?"#, did)
            .fetch_one(&state.db)
            .await
            .with_context(|| format!("failed to query account {did}"))
            .context("should fetch account status")?;

        Ok(AuthenticatedUser {
            did: did.to_owned(),
        })
    } else {
        Err(Error::with_status(
            StatusCode::UNAUTHORIZED,
            anyhow!("invalid authorization token: missing subject"),
        ))
    }
}

/// Validate a DPoP token and proof
async fn validate_dpop_token(
    access_token: &str,
    dpop_token: &str,
    parts: &axum::http::request::Parts,
    state: &AppState,
) -> Result<AuthenticatedUser, Error> {
    // Step 1: Parse and validate the access token
    let (typ, claims) = verify(&state.signing_key.did(), access_token)
        .map_err(|e| {
            Error::with_status(
                StatusCode::UNAUTHORIZED,
                e.context("failed to verify access token"),
            )
        })
        .context("access token auth should verify")?;

    // Ensure this is an access token JWT
    if typ != "at+jwt" {
        return Err(Error::with_status(
            StatusCode::UNAUTHORIZED,
            anyhow!("invalid token type: {typ}"),
        ));
    }

    // Check token expiration
    if let Some(exp) = claims.get("exp").and_then(serde_json::Value::as_i64) {
        let now = chrono::Utc::now().timestamp();
        if now >= exp {
            return Err(Error::with_message(
                StatusCode::BAD_REQUEST,
                anyhow!("token has expired"),
                ErrorMessage::new("ExpiredToken", "Token has expired"),
            ));
        }
    }

    // Step 2: Parse and validate the DPoP proof
    let dpop_parts: Vec<&str> = dpop_token.split('.').collect();
    if dpop_parts.len() != 3 {
        return Err(Error::with_status(
            StatusCode::UNAUTHORIZED,
            anyhow!("invalid DPoP token format"),
        ));
    }

    // Decode header
    let dpop_header_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(dpop_parts[0])
        .context("failed to decode DPoP header")?;

    let dpop_header: serde_json::Value =
        serde_json::from_slice(&dpop_header_bytes).context("failed to parse DPoP header")?;

    // Check typ is "dpop+jwt"
    if dpop_header.get("typ").and_then(|v| v.as_str()) != Some("dpop+jwt") {
        return Err(Error::with_status(
            StatusCode::UNAUTHORIZED,
            anyhow!("invalid DPoP token type"),
        ));
    }

    // Decode claims
    let dpop_claims_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(dpop_parts[1])
        .context("failed to decode DPoP claims")?;

    let dpop_claims: serde_json::Value =
        serde_json::from_slice(&dpop_claims_bytes).context("failed to parse DPoP claims")?;

    // Check HTTP method
    if dpop_claims.get("htm").and_then(|v| v.as_str()) != Some(parts.method.as_str()) {
        return Err(Error::with_status(
            StatusCode::UNAUTHORIZED,
            anyhow!("DPoP token HTTP method mismatch"),
        ));
    }

    // Check HTTP URI
    let expected_uri = format!(
        "https://{}/xrpc{}",
        state.config.host_name,
        parts.uri.path()
    );
    if dpop_claims.get("htu").and_then(|v| v.as_str()) != Some(&expected_uri) {
        return Err(Error::with_status(
            StatusCode::UNAUTHORIZED,
            anyhow!(
                "DPoP token HTTP URI mismatch: expected {}, got {}",
                expected_uri,
                dpop_claims
                    .get("htu")
                    .and_then(|v| v.as_str())
                    .unwrap_or("None")
            ),
        ));
    }

    // Verify access token hash (ath)
    if let Some(ath) = dpop_claims.get("ath").and_then(|v| v.as_str()) {
        let mut hasher = Sha256::new();
        hasher.update(access_token.as_bytes());
        let token_hash = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hasher.finalize());

        if ath != token_hash {
            return Err(Error::with_status(
                StatusCode::UNAUTHORIZED,
                anyhow!("DPoP access token hash mismatch"),
            ));
        }
    }

    // Get JWK thumbprint from the access token "cnf" claim
    let jkt = claims
        .get("cnf")
        .and_then(|v| v.as_object())
        .and_then(|o| o.get("jkt"))
        .and_then(|v| v.as_str())
        .context("missing or invalid 'cnf.jkt' in access token")?;

    // Extract JWK from DPoP header
    let jwk = dpop_header
        .get("jwk")
        .context("missing jwk in DPoP header")?;

    // Calculate JWK thumbprint - simplified
    // TODO: Follow RFC7638 for canonical JWK thumbprint calculation
    let jwk_json = serde_json::to_string(jwk).context("failed to serialize JWK")?;
    let mut hasher = Sha256::new();
    hasher.update(jwk_json.as_bytes());
    let calculated_thumbprint =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hasher.finalize());

    // Verify JWK thumbprint
    if calculated_thumbprint != jkt {
        return Err(Error::with_status(
            StatusCode::UNAUTHORIZED,
            anyhow!("JWK thumbprint mismatch"),
        ));
    }

    // Extract subject (DID) from access token
    if let Some(did) = claims.get("sub").and_then(|v| v.as_str()) {
        let _status = sqlx::query_scalar!(r#"SELECT status FROM accounts WHERE did = ?"#, did)
            .fetch_one(&state.db)
            .await
            .with_context(|| format!("failed to query account {did}"))
            .context("should fetch account status")?;

        Ok(AuthenticatedUser {
            did: did.to_owned(),
        })
    } else {
        Err(Error::with_status(
            StatusCode::UNAUTHORIZED,
            anyhow!("invalid access token: missing subject"),
        ))
    }
}

/// Cryptographically sign a JSON web token with the specified key.
pub(crate) fn sign(
    key: &Secp256k1Keypair,
    typ: &str,
    claims: &serde_json::Value,
) -> anyhow::Result<String> {
    // RFC 9068
    let hdr = serde_json::json!({
        "typ": typ,
        "alg": "ES256K", // Secp256k1Keypair
    });
    let hdr = base64::prelude::BASE64_URL_SAFE_NO_PAD
        .encode(serde_json::to_vec(&hdr).context("failed to encode claims")?);
    let claims = base64::prelude::BASE64_URL_SAFE_NO_PAD
        .encode(serde_json::to_vec(&claims).context("failed to encode claims")?);
    let sig = key
        .sign(format!("{hdr}.{claims}").as_bytes())
        .context("failed to sign jwt")?;
    let sig = base64::prelude::BASE64_URL_SAFE_NO_PAD.encode(sig);

    Ok(format!("{hdr}.{claims}.{sig}"))
}

/// Cryptographically verify a JSON web token's validity using the specified public key.
pub(crate) fn verify(key: &str, token: &str) -> anyhow::Result<(String, serde_json::Value)> {
    let mut parts = token.splitn(3, '.');
    let hdr = parts.next().context("no header")?;
    let claims = parts.next().context("no claims")?;
    let sig = base64::prelude::BASE64_URL_SAFE_NO_PAD
        .decode(parts.next().context("no sig")?)
        .context("failed to decode signature")?;

    let (alg, key) = atrium_crypto::did::parse_did_key(key).context("failed to decode key")?;
    Verifier::default()
        .verify(alg, &key, format!("{hdr}.{claims}").as_bytes(), &sig)
        .context("failed to verify jwt")?;

    let hdr = base64::prelude::BASE64_URL_SAFE_NO_PAD
        .decode(hdr)
        .context("failed to decode hdr")?;
    let hdr =
        serde_json::from_slice::<serde_json::Value>(&hdr).context("failed to parse hdr as json")?;
    let typ = hdr
        .get("typ")
        .and_then(serde_json::Value::as_str)
        .context("hdr is invalid")?;

    let claims = base64::prelude::BASE64_URL_SAFE_NO_PAD
        .decode(claims)
        .context("failed to decode claims")?;
    let claims = serde_json::from_slice(&claims).context("failed to parse claims as json")?;

    Ok((typ.to_owned(), claims))
}
