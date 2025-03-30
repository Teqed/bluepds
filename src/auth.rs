//! Authentication primitives.

use anyhow::{Context, anyhow};
use atrium_crypto::{
    keypair::{Did, Secp256k1Keypair},
    verify::Verifier,
};
use axum::{extract::FromRequestParts, http::StatusCode};
use base64::Engine;

use crate::{AppState, Error, error::ErrorMessage};

/// This is an axum request extractor that represents an authenticated user.
///
/// If specified in an API endpoint, this will guarantee that the API can only be called
/// by an authenticated user.
pub(crate) struct AuthenticatedUser {
    did: String,
}

impl AuthenticatedUser {
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
        let token = parts
            .headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|auth| {
                auth.to_str()
                    .ok()
                    .and_then(|auth| auth.strip_prefix("Bearer "))
            });

        let token = match token {
            Some(tok) => tok,
            None => {
                return Err(Error::with_status(
                    StatusCode::UNAUTHORIZED,
                    anyhow!("no bearer token"),
                ));
            }
        };

        // N.B: We ignore all fields inside of the token up until this point because they can be
        // attacker-controlled.
        let (typ, claims) = verify(&state.signing_key.did(), token).map_err(|e| {
            Error::with_status(
                StatusCode::UNAUTHORIZED,
                e.context("failed to verify auth token"),
            )
        })?;

        // Ensure this is an authentication token.
        if typ != "at+jwt" {
            return Err(Error::with_status(
                StatusCode::UNAUTHORIZED,
                anyhow!("invalid token {typ}"),
            ));
        }

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

        if let Some(did) = claims.get("iss").and_then(serde_json::Value::as_str) {
            let _status = sqlx::query_scalar!(r#"SELECT status FROM accounts WHERE did = ?"#, did)
                .fetch_one(&state.db)
                .await
                .with_context(|| format!("failed to query account {did}"))?;

            Ok(AuthenticatedUser {
                did: did.to_string(),
            })
        } else {
            Err(Error::with_status(
                StatusCode::UNAUTHORIZED,
                anyhow!("invalid authorization token"),
            ))
        }
    }
}

/// Cryptographically sign a JSON web token with the specified key.
pub(crate) fn sign(
    key: &Secp256k1Keypair,
    typ: &str,
    claims: serde_json::Value,
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

    Ok((typ.to_string(), claims))
}
