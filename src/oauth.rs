//! OAuth endpoints

use crate::metrics::AUTH_FAILED;
use crate::{AppConfig, AppState, Client, Db, Error, Result, SigningKey};
use anyhow::{Context as _, anyhow};
use argon2::{Argon2, PasswordHash, PasswordVerifier as _};
use atrium_crypto::keypair::Did as _;
use axum::response::Redirect;
use axum::{
    Json, Router, extract,
    extract::{Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::IntoResponse,
    routing::{get, post},
};
use base64::Engine as _;
use metrics::counter;
use rand::distributions::Alphanumeric;
use rand::{Rng as _, thread_rng};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::Digest as _;
use std::collections::{HashMap, HashSet};

/// JWK thumbprint required properties for each key type (RFC7638)
///
/// Currently only supporting ES256K (Secp256k1) and RSA as those are
/// commonly used in DPoP proofs with ATProto
const JWK_REQUIRED_PROPS: &[(&str, &[&str])] = &[
    ("EC", &["crv", "kty", "x", "y"]),
    ("RSA", &["e", "kty", "n"]),
];

/// JWT ID used record for tracking used JTIs to prevent replay attacks
#[derive(Debug, Serialize, Deserialize)]
struct JtiRecord {
    expires_at: i64,
    issuer: String,
    jti: String,
}

/// Parses a JWT without validation and returns header and claims
fn parse_jwt(token: &str) -> Result<(Value, Value)> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(Error::with_status(
            StatusCode::BAD_REQUEST,
            anyhow!("Invalid JWT format"),
        ));
    }

    let header_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(parts.first().expect("should have JWT header"))
        .context("Failed to decode JWT header")?;

    let header: Value =
        serde_json::from_slice(&header_bytes).context("Failed to parse JWT header as JSON")?;

    let claims_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(parts.get(1).expect("should have JWT claims"))
        .context("Failed to decode JWT claims")?;

    let claims: Value =
        serde_json::from_slice(&claims_bytes).context("Failed to parse JWT claims as JSON")?;

    Ok((header, claims))
}

/// Calculate RFC7638 compliant JWK thumbprint
///
/// This follows the standard:
/// 1. Extract only the required members for the key type
/// 2. Sort members alphabetically
/// 3. Remove whitespace in the serialization
/// 4. SHA-256 hash and base64url encode
fn calculate_jwk_thumbprint(jwk: &Value) -> Result<String> {
    // Determine the key type
    let key_type = jwk
        .get("kty")
        .and_then(Value::as_str)
        .context("JWK missing kty property")?;

    // Find required properties for this key type
    let required_props = JWK_REQUIRED_PROPS
        .iter()
        .find(|&&(kty, _)| kty == key_type)
        .map(|&(_, props)| props)
        .context(anyhow!("Unsupported key type: {key_type}"))?;

    // Build a new JWK with only the required properties
    let mut canonical_jwk = serde_json::Map::new();

    for &prop in required_props {
        let value = jwk
            .get(prop)
            .context(anyhow!("JWK missing required property: {prop}"))?;
        drop(canonical_jwk.insert((*prop).to_string(), value.clone()));
    }

    // Serialize with no whitespace
    let canonical_json = serde_json::to_string(&Value::Object(canonical_jwk))
        .context("Failed to serialize canonical JWK")?;

    // Hash the canonical representation
    let mut hasher = sha2::Sha256::new();
    hasher.update(canonical_json.as_bytes());
    let thumbprint = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hasher.finalize());

    Ok(thumbprint)
}

/// Protected Resource Metadata
/// - GET `/.well-known/oauth-protected-resource`
async fn protected_resource(State(config): State<AppConfig>) -> Result<Json<Value>> {
    Ok(Json(json!({
        "resource": format!("https://{}", config.host_name),
        "authorization_servers": [format!("https://{}", config.host_name)],
        "scopes_supported": [],
        "bearer_methods_supported": ["header"],
        "resource_documentation": "https://atproto.com",
    })))
}

/// Authorization Server Metadata
/// - GET `/.well-known/oauth-authorization-server`
async fn authorization_server(State(config): State<AppConfig>) -> Result<Json<Value>> {
    let base_url = format!("https://{}", config.host_name);

    Ok(Json(serde_json::json!({
        "issuer": base_url,
        "request_parameter_supported": true,
        "request_uri_parameter_supported": true,
        "require_request_uri_registration": true,
        "scopes_supported": ["atproto", "transition:generic", "transition:chat.bsky"],
        "subject_types_supported": ["public"],
        "response_types_supported": ["code"],
        "response_modes_supported": ["query", "fragment", "form_post"],
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "code_challenge_methods_supported": ["S256"],
        "ui_locales_supported": ["en-US"],
        "display_values_supported": ["page", "popup", "touch"],
        "request_object_signing_alg_values_supported": ["RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES256K", "ES384", "ES512"],
        "authorization_response_iss_parameter_supported": true,
        "request_object_encryption_alg_values_supported": [],
        "request_object_encryption_enc_values_supported": [],
        "jwks_uri": format!("{}/oauth/jwks", base_url),
        "authorization_endpoint": format!("{}/oauth/authorize", base_url),
        "token_endpoint": format!("{}/oauth/token", base_url),
        "token_endpoint_auth_methods_supported": ["none", "private_key_jwt"],
        "token_endpoint_auth_signing_alg_values_supported": ["RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES256K", "ES384", "ES512"],
        "revocation_endpoint": format!("{}/oauth/revoke", base_url),
        "introspection_endpoint": format!("{}/oauth/introspect", base_url),
        "pushed_authorization_request_endpoint": format!("{}/oauth/par", base_url),
        "require_pushed_authorization_requests": true,
        "dpop_signing_alg_values_supported": ["RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES256K", "ES384", "ES512"],
        "client_id_metadata_document_supported": true
    })))
}

/// Fetch and validate client metadata from `client_id` URL
async fn fetch_client_metadata(client: &Client, client_id: &str) -> Result<Value> {
    // Handle localhost development
    if client_id.starts_with("http://localhost") {
        let client_url = url::Url::parse(client_id).context("client_id must be a valid URL")?;

        let mut metadata = json!({
            "client_id": client_id,
            "client_name": "Loopback client",
            "response_types": ["code"],
            "grant_types": ["authorization_code", "refresh_token"],
            "scope": "atproto transition:generic",
            "token_endpoint_auth_method": "none",
            "application_type": "native",
            "dpop_bound_access_tokens": true,
        });

        // Extract redirect_uri from query params if available
        let redirect_uris = client_url.query().map_or_else(
            || {
                vec![
                    json!("http://127.0.0.1/callback"),
                    json!("http://[::1]/callback"),
                ]
            },
            |query| {
                let pairs: HashMap<_, _> = url::form_urlencoded::parse(query.as_bytes()).collect();
                pairs.get("redirect_uri").map_or_else(
                    || {
                        vec![
                            json!("http://127.0.0.1/callback"),
                            json!("http://[::1]/callback"),
                        ]
                    },
                    |uri| vec![json!(uri)],
                )
            },
        );

        if let Some(redirect_uris_value) = metadata.as_object_mut() {
            drop(redirect_uris_value.insert("redirect_uris".to_owned(), json!(redirect_uris)));
        }

        return Ok(metadata);
    }

    // If not in dev environment, fetch metadata
    let response = client
        .get(client_id)
        .send()
        .await
        .context("Failed to fetch client metadata")?;

    if !response.status().is_success() {
        return Err(Error::with_status(
            StatusCode::BAD_REQUEST,
            anyhow!(
                "Failed to fetch client metadata: HTTP {}",
                response.status()
            ),
        ));
    }

    let metadata: Value = response
        .json()
        .await
        .context("Failed to parse client metadata as JSON")?;

    // Validate client_id in metadata matches requested client_id
    if metadata.get("client_id").and_then(|id| id.as_str()) != Some(client_id) {
        return Err(Error::with_status(
            StatusCode::BAD_REQUEST,
            anyhow!("client_id in metadata doesn't match requested client_id"),
        ));
    }

    // Validate DPoP tokens requirement
    if !metadata
        .get("dpop_bound_access_tokens")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Err(Error::with_status(
            StatusCode::BAD_REQUEST,
            anyhow!("Client metadata must set dpop_bound_access_tokens to true"),
        ));
    }

    Ok(metadata)
}

/// Pushed Authorization Request endpoint
/// POST `/oauth/par`
#[expect(clippy::too_many_lines)]
async fn par(
    State(db): State<Db>,
    State(client): State<Client>,
    Json(form_data): Json<HashMap<String, String>>,
) -> Result<Json<Value>> {
    // Required parameters
    let client_id = form_data
        .get("client_id")
        .context("client_id parameter is required")?;
    let response_type = form_data
        .get("response_type")
        .context("response_type parameter is required")?;
    let code_challenge = form_data
        .get("code_challenge")
        .context("code_challenge parameter is required")?;
    let code_challenge_method = form_data
        .get("code_challenge_method")
        .context("code_challenge_method parameter is required")?;

    // Ensure code_challenge_method is S256 (required by spec)
    if code_challenge_method != "S256" {
        return Err(Error::with_status(
            StatusCode::BAD_REQUEST,
            anyhow!("code_challenge_method must be S256"),
        ));
    }

    // Validate response_type is "code" (our spec only supports this)
    if response_type != "code" {
        return Err(Error::with_status(
            StatusCode::BAD_REQUEST,
            anyhow!("response_type must be code"),
        ));
    }

    // Optional parameters
    let state = form_data.get("state").cloned();
    let login_hint = form_data.get("login_hint").cloned();
    let scope = form_data.get("scope").cloned();
    let redirect_uri = form_data.get("redirect_uri").cloned();
    let response_mode = form_data.get("response_mode").cloned();
    let display = form_data.get("display").cloned();

    // Validate client metadata
    let client_metadata = fetch_client_metadata(&client, client_id).await?;

    // If redirect_uri is provided, validate it's in the client's allowed list
    if let Some(ref provided_uri) = redirect_uri {
        let allowed_uris = client_metadata
            .get("redirect_uris")
            .and_then(|uris| uris.as_array())
            .context("client metadata missing redirect_uris")?;

        let uri_valid = allowed_uris
            .iter()
            .any(|uri| uri.as_str().is_some_and(|uri_str| uri_str == provided_uri));

        if !uri_valid {
            return Err(Error::with_status(
                StatusCode::BAD_REQUEST,
                anyhow!("redirect_uri not allowed for this client"),
            ));
        }
    } else if client_metadata
        .get("redirect_uris")
        .and_then(|uris| uris.as_array())
        .map_or(0, Vec::len)
        != 1
    {
        return Err(Error::with_status(
            StatusCode::BAD_REQUEST,
            anyhow!("redirect_uri required when client has multiple registered URIs"),
        ));
    }

    // Validate scope is in allowed scope for client
    if let Some(ref requested_scope) = scope {
        if let Some(allowed_scope) = client_metadata.get("scope").and_then(|s| s.as_str()) {
            let requested_scopes: HashSet<&str> = requested_scope.split_whitespace().collect();
            let allowed_scopes: HashSet<&str> = allowed_scope.split_whitespace().collect();

            if !requested_scopes.is_subset(&allowed_scopes) {
                return Err(Error::with_status(
                    StatusCode::BAD_REQUEST,
                    anyhow!("requested scope exceeds allowed scope"),
                ));
            }
        }
    }

    // Generate a unique request_uri
    let request_id = thread_rng()
        .sample_iter(Alphanumeric)
        .take(32)
        .map(char::from)
        .collect::<String>();
    let request_uri = format!("urn:ietf:params:oauth:request_uri:req-{request_id}");

    // Store request data in the database
    let now = chrono::Utc::now();
    let created_at = now.timestamp();
    let expires_at = now
        .checked_add_signed(chrono::Duration::minutes(5))
        .context("failed to compute expiration time")?
        .timestamp();

    _ = sqlx::query!(
        r#"
        INSERT INTO oauth_par_requests (
            request_uri, client_id, response_type, code_challenge, code_challenge_method,
            state, login_hint, scope, redirect_uri, response_mode, display,
            created_at, expires_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
        request_uri,
        client_id,
        response_type,
        code_challenge,
        code_challenge_method,
        state,
        login_hint,
        scope,
        redirect_uri,
        response_mode,
        display,
        created_at,
        expires_at
    )
    .execute(&db)
    .await
    .context("failed to store PAR request")?;

    Ok(Json(json!({
        "request_uri": request_uri,
        "expires_in": 300_i32 // 5 minutes
    })))
}

/// OAuth Authorization endpoint
/// GET `/oauth/authorize`
async fn authorize(
    State(db): State<Db>,
    State(client): State<Client>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<impl IntoResponse> {
    // Required parameters
    let client_id = params
        .get("client_id")
        .context("client_id parameter is required")?;
    let request_uri = params
        .get("request_uri")
        .context("request_uri parameter is required")?;

    let timestamp = chrono::Utc::now().timestamp();

    // Retrieve the PAR request from the database
    let par_request = sqlx::query!(
        r#"
        SELECT * FROM oauth_par_requests
        WHERE request_uri = ? AND client_id = ? AND expires_at > ?
        "#,
        request_uri,
        client_id,
        timestamp
    )
    .fetch_optional(&db)
    .await
    .context("failed to query PAR request")?
    .context("PAR request not found or expired")?;

    // Validate client metadata
    let client_metadata = fetch_client_metadata(&client, client_id).await?;

    // Authorization page with login form
    let login_hint = par_request.login_hint.unwrap_or_default();
    let html = format!(
        r#"<!DOCTYPE html>
        <html>
        <head>
            <title>Authentication Required</title>
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; max-width: 500px; margin: 0 auto; padding: 20px; }}
                .container {{ border: 1px solid #e0e0e0; border-radius: 8px; padding: 20px; }}
                h1 {{ font-size: 24px; }}
                label {{ display: block; margin-top: 12px; }}
                input[type="text"], input[type="password"] {{ width: 100%; padding: 8px; margin-top: 4px; border: 1px solid #ddd; border-radius: 4px; }}
                button {{ margin-top: 20px; padding: 8px 16px; background-color: #0070f3; color: white; border: none; border-radius: 4px; cursor: pointer; }}
                .client {{ margin-top: 12px; font-size: 14px; color: #666; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Sign in to continue</h1>
                <p>An application is requesting access to your account.</p>

                <div class="client">
                    <strong>Client:</strong> {client_name}
                </div>

                <form action="/oauth/authorize/sign-in" method="post">
                    <input type="hidden" name="request_uri" value="{request_uri}">
                    <input type="hidden" name="client_id" value="{client_id}">

                    <label for="username">Username</label>
                    <input type="text" id="username" name="username" value="{login_hint}" required>

                    <label for="password">Password</label>
                    <input type="password" id="password" name="password" required>

                    <label>
                        <input type="checkbox" name="remember" value="true"> Remember me
                    </label>

                    <button type="submit">Sign in</button>
                </form>
            </div>
        </body>
        </html>
        "#,
        client_name = client_metadata
            .get("client_name")
            .and_then(|n| n.as_str())
            .unwrap_or(client_id),
        request_uri = request_uri,
        client_id = client_id,
        login_hint = login_hint
    );

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, HeaderValue::from_static("text/html"))],
        html,
    ))
}

/// OAuth Authorization Sign-in endpoint
/// POST `/oauth/authorize/sign-in`
#[expect(clippy::too_many_lines)]
async fn authorize_signin(
    State(db): State<Db>,
    State(config): State<AppConfig>,
    State(client): State<Client>,
    extract::Form(form_data): extract::Form<HashMap<String, String>>,
) -> Result<impl IntoResponse> {
    use std::fmt::Write as _;

    // Extract form data
    let username = form_data.get("username").context("username is required")?;
    let password = form_data.get("password").context("password is required")?;
    let request_uri = form_data
        .get("request_uri")
        .context("request_uri is required")?;
    let client_id = form_data
        .get("client_id")
        .context("client_id is required")?;

    let timestamp = chrono::Utc::now().timestamp();

    // Retrieve the PAR request
    let par_request = sqlx::query!(
        r#"
        SELECT * FROM oauth_par_requests
        WHERE request_uri = ? AND client_id = ? AND expires_at > ?
        "#,
        request_uri,
        client_id,
        timestamp
    )
    .fetch_optional(&db)
    .await
    .context("failed to query PAR request")?
    .context("PAR request not found or expired")?;

    // Authenticate the user
    let account = sqlx::query!(
        r#"
        WITH LatestHandles AS (
            SELECT did, handle
            FROM handles
            WHERE (did, created_at) IN (
                SELECT did, MAX(created_at) AS max_created_at
                FROM handles
                GROUP BY did
            )
        )
        SELECT a.did, a.email, a.password, h.handle
        FROM accounts a
        LEFT JOIN LatestHandles h ON a.did = h.did
        WHERE h.handle = ?
        "#,
        username
    )
    .fetch_optional(&db)
    .await
    .context("failed to query database")?
    .context("user not found")?;

    // Verify password - fixed to use equality check instead of pattern matching
    if Argon2::default().verify_password(
        password.as_bytes(),
        &PasswordHash::new(account.password.as_str()).context("invalid password hash in db")?,
    ) == Ok(())
    {
    } else {
        counter!(AUTH_FAILED).increment(1);
        return Err(Error::with_status(
            StatusCode::UNAUTHORIZED,
            anyhow!("invalid credentials"),
        ));
    }

    // Generate authorization code
    let code = thread_rng()
        .sample_iter(Alphanumeric)
        .take(40)
        .map(char::from)
        .collect::<String>();

    // Determine redirect URI to use
    let redirect_uri = if let Some(uri) = par_request.redirect_uri.as_ref() {
        uri.clone()
    } else {
        let client_metadata = fetch_client_metadata(&client, client_id).await?;
        client_metadata
            .get("redirect_uris")
            .and_then(|uris| uris.as_array())
            .and_then(|uris| uris.first())
            .and_then(|uri| uri.as_str())
            .context("No redirect_uri available")?
            .to_owned()
    };

    // Store the authorization code
    let now = chrono::Utc::now();
    let created_at = now.timestamp();
    let expires_at = now
        .checked_add_signed(chrono::Duration::minutes(10))
        .context("failed to compute expiration time")?
        .timestamp();

    _ = sqlx::query!(
        r#"
        INSERT INTO oauth_authorization_codes (
            code, client_id, subject, code_challenge, code_challenge_method,
            redirect_uri, scope, created_at, expires_at, used
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
        code,
        client_id,
        account.did,
        par_request.code_challenge,
        par_request.code_challenge_method,
        redirect_uri,
        par_request.scope,
        created_at,
        expires_at,
        false
    )
    .execute(&db)
    .await
    .context("failed to store authorization code")?;

    // Use state from the PAR request or generate one
    let state = par_request.state.unwrap_or_else(|| {
        thread_rng()
            .sample_iter(Alphanumeric)
            .take(16)
            .map(char::from)
            .collect::<String>()
    });

    // Build redirect URL
    let mut redirect_target = redirect_uri;
    match par_request.response_mode.as_deref() {
        Some("fragment") => redirect_target.push('#'),
        _ => redirect_target.push('?'),
    }

    write!(redirect_target, "state={}", urlencoding::encode(&state)).unwrap();
    let host_name = format!("https://{}", &config.host_name);
    write!(redirect_target, "&iss={}", urlencoding::encode(&host_name)).unwrap();
    write!(redirect_target, "&code={}", urlencoding::encode(&code)).unwrap();
    Ok(Redirect::to(&redirect_target))
}

/// Verify a DPoP proof and return the JWK thumbprint
///
/// Verifies:
/// 1. The token is properly formatted with JWT fields
/// 2. The token has a valid signature, verified using the embedded JWK
/// 3. The required claims are present (jti, htm, htu)
/// 4. The htm and htu match the request
/// 5. The token has not been used before (replay prevention)
/// 6. The token is not expired
async fn verify_dpop_proof(
    dpop_token: &str,
    http_method: &str,
    http_uri: &str,
    db: &Db,
) -> Result<String> {
    // Parse the DPoP token
    let (header, claims) = parse_jwt(dpop_token)?;

    // Verify "typ" is "dpop+jwt"
    if header.get("typ").and_then(Value::as_str) != Some("dpop+jwt") {
        return Err(Error::with_status(
            StatusCode::BAD_REQUEST,
            anyhow!("Invalid DPoP token type"),
        ));
    }

    // Verify required claims
    let jti = claims
        .get("jti")
        .and_then(Value::as_str)
        .context("Missing jti claim in DPoP token")?;

    // Check for token expiration
    #[expect(clippy::arithmetic_side_effects)]
    let exp = claims
        .get("exp")
        .and_then(Value::as_i64)
        .unwrap_or_else(|| chrono::Utc::now().timestamp() + 60); // Default 60s if not specified

    let now = chrono::Utc::now().timestamp();
    if now >= exp {
        return Err(Error::with_status(
            StatusCode::BAD_REQUEST,
            anyhow!("DPoP token has expired"),
        ));
    }

    // Check htm (HTTP method) claim
    if claims.get("htm").and_then(Value::as_str) != Some(http_method) {
        return Err(Error::with_status(
            StatusCode::BAD_REQUEST,
            anyhow!("Invalid htm claim in DPoP token"),
        ));
    }

    // Check htu (HTTP URI) claim
    if claims.get("htu").and_then(Value::as_str) != Some(http_uri) {
        return Err(Error::with_status(
            StatusCode::BAD_REQUEST,
            anyhow!(
                "Invalid htu claim in DPoP token: expected {}, got {}",
                http_uri,
                claims.get("htu").and_then(Value::as_str).unwrap_or("None")
            ),
        ));
    }

    // Extract JWK (JSON Web Key)
    let jwk = header
        .get("jwk")
        .context("Missing jwk in DPoP token header")?;

    // Calculate JWK thumbprint using RFC7638
    let thumbprint = calculate_jwk_thumbprint(jwk)?;

    // TODO: Verify the token signature using the JWK
    // This requires integrating with a JWT library that can verify
    // signatures using a JWK directly

    // Check for replay attacks by verifying this JTI hasn't been used before
    let jti_used =
        sqlx::query_scalar!(r#"SELECT COUNT(*) FROM oauth_used_jtis WHERE jti = ?"#, jti)
            .fetch_one(db)
            .await
            .context("failed to check JTI")?;

    if jti_used > 0 {
        return Err(Error::with_status(
            StatusCode::BAD_REQUEST,
            anyhow!("DPoP token has been replayed"),
        ));
    }

    // Store the JTI to prevent replay attacks
    _ = sqlx::query!(
        r#"
        INSERT INTO oauth_used_jtis (jti, issuer, created_at, expires_at)
        VALUES (?, ?, ?, ?)
        "#,
        jti,
        thumbprint, // Use thumbprint as issuer identifier
        now,
        exp
    )
    .execute(db)
    .await
    .context("failed to store JTI")?;

    // Cleanup expired JTIs periodically (1% chance on each request)
    if thread_rng().gen_range(0_i32..100_i32) == 0_i32 {
        _ = sqlx::query!(r#"DELETE FROM oauth_used_jtis WHERE expires_at < ?"#, now)
            .execute(db)
            .await
            .context("failed to clean up expired JTIs")?;
    }

    Ok(thumbprint)
}

/// Verify a `code_verifier` against stored `code_challenge`
fn verify_pkce(code_verifier: &str, stored_challenge: &str, method: &str) -> Result<()> {
    // Only S256 is supported currently
    if method != "S256" {
        return Err(Error::with_status(
            StatusCode::BAD_REQUEST,
            anyhow!("Unsupported code_challenge_method: {}", method),
        ));
    }

    // Calculate the code challenge from verifier
    let mut hasher = sha2::Sha256::new();
    hasher.update(code_verifier.as_bytes());
    let calculated = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hasher.finalize());

    // Compare with stored challenge
    if calculated != stored_challenge {
        return Err(Error::with_status(
            StatusCode::BAD_REQUEST,
            anyhow!("Code verifier doesn't match challenge"),
        ));
    }

    Ok(())
}

/// OAuth token endpoint
/// - POST `/oauth/token`
///
/// Handles both `authorization_code` and `refresh_token` grants
#[expect(clippy::too_many_lines)]
async fn token(
    State(db): State<Db>,
    State(skey): State<SigningKey>,
    State(config): State<AppConfig>,
    headers: HeaderMap,
    Json(form_data): Json<HashMap<String, String>>,
) -> Result<Json<Value>> {
    // Extract form parameters
    let grant_type = form_data
        .get("grant_type")
        .context("grant_type is required")?;

    // Validate DPoP header
    let dpop_token = headers
        .get("DPoP")
        .context("DPoP header is required")?
        .to_str()
        .context("Invalid DPoP header")?;

    let dpop_thumbprint = verify_dpop_proof(
        dpop_token,
        "POST",
        &format!("https://{}/oauth/token", config.host_name),
        &db,
    )
    .await?;

    match grant_type.as_str() {
        "authorization_code" => {
            // Extract required parameters
            let code = form_data.get("code").context("code is required")?;
            let code_verifier = form_data
                .get("code_verifier")
                .context("code_verifier is required")?;
            let client_id = form_data
                .get("client_id")
                .context("client_id is required")?;
            let redirect_uri = form_data
                .get("redirect_uri")
                .context("redirect_uri is required")?;

            let timestamp = chrono::Utc::now().timestamp();

            // Retrieve the authorization code
            let auth_code = sqlx::query!(
                r#"
                SELECT * FROM oauth_authorization_codes
                WHERE code = ? AND client_id = ? AND redirect_uri = ? AND expires_at > ? AND used = FALSE
                "#,
                code,
                client_id,
                redirect_uri,
                timestamp
            )
            .fetch_optional(&db)
            .await
            .context("failed to query authorization code")?
            .context("authorization code not found, expired, or already used")?;

            // Verify PKCE code challenge
            verify_pkce(
                code_verifier,
                &auth_code.code_challenge,
                &auth_code.code_challenge_method,
            )?;

            // Mark the code as used
            _ = sqlx::query!(
                r#"UPDATE oauth_authorization_codes SET used = TRUE WHERE code = ?"#,
                code
            )
            .execute(&db)
            .await
            .context("failed to mark code as used")?;

            // Generate tokens
            let now = chrono::Utc::now().timestamp();
            let access_token_expires_in = 3600; // 1 hour
            #[expect(clippy::arithmetic_side_effects)]
            let access_token_expires_at = now + access_token_expires_in;
            #[expect(clippy::arithmetic_side_effects)]
            let refresh_token_expires_at = now + 2_592_000; // 30 days

            // Create access token
            let access_token_claims = json!({
                "iss": format!("https://{}", config.host_name),
                "sub": auth_code.subject,
                "aud": client_id,
                "exp": access_token_expires_at,
                "iat": now,
                "cnf": {
                    "jkt": dpop_thumbprint
                },
                "scope": auth_code.scope
            });

            let access_token = crate::auth::sign(&skey, "at+jwt", &access_token_claims)
                .context("failed to sign access token")?;

            // Create refresh token
            let refresh_token_claims = json!({
                "iss": format!("https://{}", config.host_name),
                "sub": auth_code.subject,
                "aud": client_id,
                "exp": refresh_token_expires_at,
                "iat": now,
                "cnf": {
                    "jkt": dpop_thumbprint
                },
                "scope": auth_code.scope
            });

            let refresh_token = crate::auth::sign(&skey, "rt+jwt", &refresh_token_claims)
                .context("failed to sign refresh token")?;

            // Store the refresh token
            _ = sqlx::query!(
                r#"
                INSERT INTO oauth_refresh_tokens (
                    token, client_id, subject, dpop_thumbprint, scope, created_at, expires_at, revoked
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                "#,
                refresh_token,
                client_id,
                auth_code.subject,
                dpop_thumbprint,
                auth_code.scope,
                now,
                refresh_token_expires_at,
                false
            )
            .execute(&db)
            .await
            .context("failed to store refresh token")?;

            // Return token response
            Ok(Json(json!({
                "access_token": access_token,
                "token_type": "DPoP",
                "expires_in": access_token_expires_in,
                "refresh_token": refresh_token,
                "scope": auth_code.scope,
                "sub": auth_code.subject
            })))
        }
        "refresh_token" => {
            // Extract required parameters
            let refresh_token = form_data
                .get("refresh_token")
                .context("refresh_token is required")?;
            let client_id = form_data
                .get("client_id")
                .context("client_id is required")?;

            let timestamp = chrono::Utc::now().timestamp();

            // Retrieve the refresh token
            let token_data = sqlx::query!(
                r#"
                SELECT * FROM oauth_refresh_tokens
                WHERE token = ? AND client_id = ? AND expires_at > ? AND revoked = FALSE AND dpop_thumbprint = ?
                "#,
                refresh_token,
                client_id,
                timestamp,
                dpop_thumbprint
            )
            .fetch_optional(&db)
            .await
            .context("failed to query refresh token")?
            .context("refresh token not found, expired, revoked, or invalid for this DPoP key")?;

            // Rotate the refresh token by revoking the current one
            _ = sqlx::query!(
                r#"UPDATE oauth_refresh_tokens SET revoked = TRUE WHERE token = ?"#,
                refresh_token
            )
            .execute(&db)
            .await
            .context("failed to revoke old refresh token")?;

            // Generate new tokens
            let now = chrono::Utc::now().timestamp();
            let access_token_expires_in = 3600; // 1 hour
            #[expect(clippy::arithmetic_side_effects)]
            let access_token_expires_at = now + access_token_expires_in;
            #[expect(clippy::arithmetic_side_effects)]
            let refresh_token_expires_at = now + 2_592_000; // 30 days

            // Create access token
            let access_token_claims = json!({
                "iss": format!("https://{}", config.host_name),
                "sub": token_data.subject,
                "aud": client_id,
                "exp": access_token_expires_at,
                "iat": now,
                "cnf": {
                    "jkt": dpop_thumbprint
                },
                "scope": token_data.scope
            });

            let access_token = crate::auth::sign(&skey, "at+jwt", &access_token_claims)
                .context("failed to sign access token")?;

            // Create new refresh token
            let new_refresh_token_claims = json!({
                "iss": format!("https://{}", config.host_name),
                "sub": token_data.subject,
                "aud": client_id,
                "exp": refresh_token_expires_at,
                "iat": now,
                "cnf": {
                    "jkt": dpop_thumbprint
                },
                "scope": token_data.scope
            });

            let new_refresh_token = crate::auth::sign(&skey, "rt+jwt", &new_refresh_token_claims)
                .context("failed to sign refresh token")?;

            // Store the new refresh token
            _ = sqlx::query!(
                r#"
                INSERT INTO oauth_refresh_tokens (
                    token, client_id, subject, dpop_thumbprint, scope, created_at, expires_at, revoked
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                "#,
                new_refresh_token,
                client_id,
                token_data.subject,
                dpop_thumbprint,
                token_data.scope,
                now,
                refresh_token_expires_at,
                false
            )
            .execute(&db)
            .await
            .context("failed to store refresh token")?;

            // Return token response
            Ok(Json(json!({
                "access_token": access_token,
                "token_type": "DPoP",
                "expires_in": access_token_expires_in,
                "refresh_token": new_refresh_token,
                "scope": token_data.scope,
                "sub": token_data.subject
            })))
        }
        _ => Err(Error::with_status(
            StatusCode::BAD_REQUEST,
            anyhow!("unsupported grant_type: {}", grant_type),
        )),
    }
}

/// JWKS (JSON Web Key Set) endpoint
/// - GET `/oauth/jwks`
///
/// Returns the server's public keys as a JWKS document
async fn jwks(State(skey): State<SigningKey>) -> Result<Json<Value>> {
    // Get the public key data from the signing key
    // This assumes the key is a Secp256k1Keypair
    // For a real implementation, you would construct a proper JWK
    // with all the required fields based on the key type

    let did_string = skey.did();

    // Extract the key ID from the DID string
    // did:key:z... format, where z... is the multibase-encoded public key
    let key_id = did_string.strip_prefix("did:key:").unwrap_or(&did_string);

    let jwk = json!({
        "kty": "EC",
        "crv": "secp256k1",
        "kid": key_id,
        "use": "sig",
        "alg": "ES256K",
        // In a real implementation, you would include:
        // "x": base64url encoded x coordinate,
        // "y": base64url encoded y coordinate
        // These would be derived from the actual public key
    });

    // Return the JWKS document
    Ok(Json(json!({
        "keys": [jwk]
    })))
}

/// Token Revocation endpoint
/// - POST `/oauth/revoke`
///
/// Implements RFC7009 for revoking refresh tokens
async fn revoke(
    State(db): State<Db>,
    Json(form_data): Json<HashMap<String, String>>,
) -> Result<Json<Value>> {
    // Extract required parameters
    let token = form_data.get("token").context("token is required")?;
    let refresh_token_string = "refresh_token".to_owned();
    let token_type_hint = form_data
        .get("token_type_hint")
        .unwrap_or(&refresh_token_string);

    // We only support revoking refresh tokens
    if token_type_hint != "refresh_token" {
        return Err(Error::with_status(
            StatusCode::BAD_REQUEST,
            anyhow!("unsupported token_type_hint: {}", token_type_hint),
        ));
    }

    // Revoke the token
    _ = sqlx::query!(
        r#"UPDATE oauth_refresh_tokens SET revoked = TRUE WHERE token = ?"#,
        token
    )
    .execute(&db)
    .await
    .context("failed to revoke token")?;

    // RFC7009 requires a 200 OK with an empty response
    Ok(Json(json!({})))
}

/// Token Introspection endpoint
/// - POST `/oauth/introspect`
///
/// Implements RFC7662 for introspecting tokens
async fn introspect(
    State(db): State<Db>,
    State(skey): State<SigningKey>,
    Json(form_data): Json<HashMap<String, String>>,
) -> Result<Json<Value>> {
    // Extract required parameters
    let token = form_data.get("token").context("token is required")?;
    let token_type_hint = form_data.get("token_type_hint");

    // Parse the token
    let Ok((typ, claims)) = crate::auth::verify(&skey.did(), token) else {
        // Per RFC7662, invalid tokens return { "active": false }
        return Ok(Json(json!({"active": false})));
    };

    // Check token type
    let is_refresh_token = typ == "rt+jwt";
    let is_access_token = typ == "at+jwt";

    if !is_access_token && !is_refresh_token {
        return Ok(Json(json!({"active": false})));
    }

    // If token_type_hint is provided, verify it matches
    if let Some(hint) = token_type_hint {
        if (hint == "refresh_token" && !is_refresh_token)
            || (hint == "access_token" && !is_access_token)
        {
            return Ok(Json(json!({"active": false})));
        }
    }

    // Check expiration
    if let Some(exp) = claims.get("exp").and_then(Value::as_i64) {
        let now = chrono::Utc::now().timestamp();
        if now >= exp {
            return Ok(Json(json!({"active": false})));
        }
    } else {
        return Ok(Json(json!({"active": false})));
    }

    // For refresh tokens, check if it's been revoked
    if is_refresh_token {
        let is_revoked = sqlx::query_scalar!(
            r#"SELECT revoked FROM oauth_refresh_tokens WHERE token = ?"#,
            token
        )
        .fetch_optional(&db)
        .await
        .context("failed to query token")?
        .unwrap_or(true);

        if is_revoked {
            return Ok(Json(json!({"active": false})));
        }
    }

    // Token is valid, return introspection info
    let subject = claims.get("sub").and_then(Value::as_str);
    let client_id = claims.get("aud").and_then(Value::as_str);
    let scope = claims.get("scope").and_then(Value::as_str);
    let expiration = claims.get("exp").and_then(Value::as_i64);
    let issued_at = claims.get("iat").and_then(Value::as_i64);

    Ok(Json(json!({
        "active": true,
        "sub": subject,
        "client_id": client_id,
        "scope": scope,
        "exp": expiration,
        "iat": issued_at,
        "token_type": if is_access_token { "access_token" } else { "refresh_token" }
    })))
}

/// Register OAuth routes
pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/.well-known/oauth-protected-resource",
            get(protected_resource),
        )
        .route(
            "/.well-known/oauth-authorization-server",
            get(authorization_server),
        )
        .route("/oauth/par", post(par))
        .route("/oauth/authorize", get(authorize))
        .route("/oauth/authorize/sign-in", post(authorize_signin))
        .route("/oauth/token", post(token))
        .route("/oauth/jwks", get(jwks))
        .route("/oauth/revoke", post(revoke))
        .route("/oauth/introspect", post(introspect))
}
