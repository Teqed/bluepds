use anyhow::{Context as _, anyhow};
use atrium_crypto::{
    keypair::{Did as _, Secp256k1Keypair},
    verify::Verifier,
};
use axum::{extract::FromRequestParts, http::StatusCode};
use base64::Engine as _;
use diesel::prelude::*;
use sha2::{Digest as _, Sha256};

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

        let auth_str = auth_header.to_str().map_err(|e| {
            Error::with_status(
                StatusCode::UNAUTHORIZED,
                anyhow!("authorization header should be valid utf-8").context(e),
            )
        })?;

        // Check for DPoP header
        let dpop_header = parts.headers.get("dpop");
        let has_dpop = dpop_header.is_some();

        // Handle different token types
        if auth_str.starts_with("Bearer ") || auth_str.starts_with("DPoP ") {
            let token = auth_str
                .split_once(' ')
                .expect("Auth string should have a space")
                .1;

            if has_dpop {
                // Process DPoP token - the Authorization header contains the access token
                // and the DPoP header contains the proof
                let dpop_token = dpop_header
                    .expect("DPoP header should exist")
                    .to_str()
                    .map_err(|e| {
                        Error::with_status(
                            StatusCode::UNAUTHORIZED,
                            anyhow!("DPoP header should be valid utf-8").context(e),
                        )
                    })?;

                return validate_dpop_token(token, dpop_token, parts, state).await;
            }

            // Standard Bearer token
            return validate_bearer_token(token, state).await;
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
        use crate::schema::pds::account::dsl as AccountSchema;
        let did_clone = did.to_owned();

        let _did = state
            .db
            .get()
            .await
            .expect("failed to get db connection")
            .interact(move |conn| {
                AccountSchema::account
                    .filter(AccountSchema::did.eq(did_clone))
                    .select(AccountSchema::did)
                    .first::<String>(conn)
            })
            .await
            .expect("failed to query account");

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

#[expect(clippy::too_many_lines, reason = "validating dpop has many loc")]
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
        .decode(dpop_parts.first().context("header part missing")?)
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
        .decode(dpop_parts.get(1).context("claims part missing")?)
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
        parts.uri.path_and_query().expect("path and query to exist")
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

    // Extract JWK from DPoP header
    let jwk = dpop_header
        .get("jwk")
        .context("missing jwk in DPoP header")?;

    // Calculate JWK thumbprint - RFC7638 compliant
    let key_type = jwk
        .get("kty")
        .and_then(|v| v.as_str())
        .context("JWK missing kty property")?;

    // Define required properties for each key type
    let required_props = match key_type {
        "EC" => &["crv", "kty", "x", "y"][..],
        "RSA" => &["e", "kty", "n"][..],
        _ => {
            return Err(Error::with_status(
                StatusCode::UNAUTHORIZED,
                anyhow!("Unsupported JWK key type: {}", key_type),
            ));
        }
    };

    // Build a new JWK with only the required properties
    let mut canonical_jwk = serde_json::Map::new();

    for prop in required_props {
        let value = jwk
            .get(prop)
            .context(format!("JWK missing required property: {prop}"))?;
        drop(canonical_jwk.insert((*prop).to_owned(), value.clone()));
    }

    // Serialize with no whitespace
    let canonical_json = serde_json::to_string(&serde_json::Value::Object(canonical_jwk))
        .context("Failed to serialize canonical JWK")?;

    // Hash the canonical representation
    let mut hasher = Sha256::new();
    hasher.update(canonical_json.as_bytes());
    let calculated_thumbprint =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hasher.finalize());

    // Get JWK thumbprint from the access token "cnf" claim
    let jkt = claims
        .get("cnf")
        .and_then(|v| v.as_object())
        .and_then(|o| o.get("jkt"))
        .and_then(|v| v.as_str())
        .context("missing or invalid 'cnf.jkt' in access token")?;

    // Verify JWK thumbprint
    if calculated_thumbprint != jkt {
        return Err(Error::with_status(
            StatusCode::UNAUTHORIZED,
            anyhow!("JWK thumbprint mismatch"),
        ));
    }

    // Check JTI replay using the database
    let jti = dpop_claims
        .get("jti")
        .and_then(|j| j.as_str())
        .context("Missing jti claim in DPoP token")?;

    let timestamp = chrono::Utc::now().timestamp();

    use crate::schema::pds::oauth_used_jtis::dsl as JtiSchema;

    // Check if JTI has been used before
    let jti_string = jti.to_owned();
    let jti_used = state
        .db
        .get()
        .await
        .expect("failed to get db connection")
        .interact(move |conn| {
            JtiSchema::oauth_used_jtis
                .filter(JtiSchema::jti.eq(jti_string))
                .count()
                .get_result::<i64>(conn)
        })
        .await
        .expect("failed to query JTI")
        .expect("failed to get JTI count");

    if jti_used > 0 {
        return Err(Error::with_status(
            StatusCode::UNAUTHORIZED,
            anyhow!("DPoP token has been replayed"),
        ));
    }

    // Store the JTI to prevent replay attacks
    // Get expiry from token or default to 60 seconds
    let exp = dpop_claims
        .get("exp")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or_else(|| timestamp.checked_add(60).unwrap_or(timestamp));

    // Convert SQLx INSERT to Diesel
    let jti_str = jti.to_owned();
    let thumbprint_str = calculated_thumbprint.to_string();
    let _ = state
        .db
        .get()
        .await
        .expect("failed to get db connection")
        .interact(move |conn| {
            diesel::insert_into(JtiSchema::oauth_used_jtis)
                .values((
                    JtiSchema::jti.eq(jti_str),
                    JtiSchema::issuer.eq(thumbprint_str),
                    JtiSchema::created_at.eq(timestamp),
                    JtiSchema::expires_at.eq(exp),
                ))
                .execute(conn)
        })
        .await
        .expect("failed to insert JTI")
        .expect("failed to insert JTI");

    // Extract subject (DID) from access token
    if let Some(did) = claims.get("sub").and_then(|v| v.as_str()) {
        use crate::schema::pds::account::dsl as AccountSchema;

        let did_clone = did.to_owned();

        let _did = state
            .db
            .get()
            .await
            .expect("failed to get db connection")
            .interact(move |conn| {
                AccountSchema::account
                    .filter(AccountSchema::did.eq(did_clone))
                    .select(AccountSchema::did)
                    .first::<String>(conn)
            })
            .await
            .expect("failed to query account")
            .expect("failed to get account");

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
