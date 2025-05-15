//! OAuth endpoints

use crate::metrics::AUTH_FAILED;
use crate::{AppConfig, AppState, Client, Error, Result, SigningKey};
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
use deadpool_diesel::sqlite::Pool;
use diesel::*;
use metrics::counter;
use rand::distributions::Alphanumeric;
use rand::{Rng as _, thread_rng};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
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
    let mut hasher = Sha256::new();
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
    State(db): State<Pool>,
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

    use crate::schema::pds::oauth_par_requests::dsl as ParRequestSchema;
    let client_id = client_id.to_owned();
    let request_uri_cloned = request_uri.to_owned();
    let response_type = response_type.to_owned();
    let code_challenge = code_challenge.to_owned();
    let code_challenge_method = code_challenge_method.to_owned();
    let state = state.map(|s| s.to_owned());
    let login_hint = login_hint.map(|s| s.to_owned());
    let scope = scope.map(|s| s.to_owned());
    let redirect_uri = redirect_uri.map(|s| s.to_owned());
    let response_mode = response_mode.map(|s| s.to_owned());
    let display = display.map(|s| s.to_owned());
    let created_at = created_at;
    let expires_at = expires_at;
    _ = db
        .get()
        .await
        .expect("Failed to get database connection")
        .interact(move |conn| {
            insert_into(ParRequestSchema::oauth_par_requests)
                .values((
                    ParRequestSchema::request_uri.eq(&request_uri_cloned),
                    ParRequestSchema::client_id.eq(client_id),
                    ParRequestSchema::response_type.eq(response_type),
                    ParRequestSchema::code_challenge.eq(code_challenge),
                    ParRequestSchema::code_challenge_method.eq(code_challenge_method),
                    ParRequestSchema::state.eq(state),
                    ParRequestSchema::login_hint.eq(login_hint),
                    ParRequestSchema::scope.eq(scope),
                    ParRequestSchema::redirect_uri.eq(redirect_uri),
                    ParRequestSchema::response_mode.eq(response_mode),
                    ParRequestSchema::display.eq(display),
                    ParRequestSchema::created_at.eq(created_at),
                    ParRequestSchema::expires_at.eq(expires_at),
                ))
                .execute(conn)
        })
        .await
        .expect("Failed to store PAR request")
        .expect("Failed to store PAR request");

    Ok(Json(json!({
        "request_uri": request_uri,
        "expires_in": 300_i32 // 5 minutes
    })))
}

/// OAuth Authorization endpoint
/// GET `/oauth/authorize`
async fn authorize(
    State(db): State<Pool>,
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
    use crate::schema::pds::oauth_par_requests::dsl as ParRequestSchema;

    let request_uri_clone = request_uri.to_owned();
    let client_id_clone = client_id.to_owned();
    let timestamp_clone = timestamp.clone();
    let login_hint = db
        .get()
        .await
        .expect("Failed to get database connection")
        .interact(move |conn| {
            ParRequestSchema::oauth_par_requests
                .select(ParRequestSchema::login_hint)
                .filter(ParRequestSchema::request_uri.eq(request_uri_clone))
                .filter(ParRequestSchema::client_id.eq(client_id_clone))
                .filter(ParRequestSchema::expires_at.gt(timestamp_clone))
                .first::<Option<String>>(conn)
                .optional()
        })
        .await
        .expect("Failed to query PAR request")
        .expect("Failed to query PAR request")
        .expect("Failed to query PAR request");

    // Validate client metadata
    let client_metadata = fetch_client_metadata(&client, client_id).await?;

    // Authorization page with login form
    let login_hint = login_hint.unwrap_or_default();
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
    State(db): State<Pool>,
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
    use crate::schema::pds::oauth_par_requests::dsl as ParRequestSchema;
    #[derive(Queryable, Selectable)]
    #[diesel(table_name = crate::schema::pds::oauth_par_requests)]
    #[diesel(check_for_backend(sqlite::Sqlite))]
    struct ParRequest {
        request_uri: String,
        client_id: String,
        response_type: String,
        code_challenge: String,
        code_challenge_method: String,
        state: Option<String>,
        login_hint: Option<String>,
        scope: Option<String>,
        redirect_uri: Option<String>,
        response_mode: Option<String>,
        display: Option<String>,
        created_at: i64,
        expires_at: i64,
    }
    let request_uri_clone = request_uri.to_owned();
    let client_id_clone = client_id.to_owned();
    let timestamp_clone = timestamp.clone();
    let par_request = db
        .get()
        .await
        .expect("Failed to get database connection")
        .interact(move |conn| {
            ParRequestSchema::oauth_par_requests
                .filter(ParRequestSchema::request_uri.eq(request_uri_clone))
                .filter(ParRequestSchema::client_id.eq(client_id_clone))
                .filter(ParRequestSchema::expires_at.gt(timestamp_clone))
                .first::<ParRequest>(conn)
                .optional()
        })
        .await
        .expect("Failed to query PAR request")
        .expect("Failed to query PAR request")
        .expect("Failed to query PAR request");

    // Authenticate the user
    use rsky_pds::schema::pds::account::dsl as AccountSchema;
    use rsky_pds::schema::pds::actor::dsl as ActorSchema;
    let username_clone = username.to_owned();
    let account = db
        .get()
        .await
        .expect("Failed to get database connection")
        .interact(move |conn| {
            AccountSchema::account
                .filter(AccountSchema::email.eq(username_clone))
                .first::<rsky_pds::models::Account>(conn)
                .optional()
        })
        .await
        .expect("Failed to query account")
        .expect("Failed to query account")
        .expect("Failed to query account");
    // let actor = db
    //     .get()
    //     .await
    //     .expect("Failed to get database connection")
    //     .interact(move |conn| {
    //         ActorSchema::actor
    //             .filter(ActorSchema::did.eq(did))
    //             .first::<rsky_pds::models::Actor>(conn)
    //             .optional()
    //     })
    //     .await
    //     .expect("Failed to query actor")
    //     .expect("Failed to query actor")
    //     .expect("Failed to query actor");

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

    use crate::schema::pds::oauth_authorization_codes::dsl as AuthCodeSchema;
    let code_cloned = code.to_owned();
    let client_id = client_id.to_owned();
    let subject = account.did.to_owned();
    let code_challenge = par_request.code_challenge.to_owned();
    let code_challenge_method = par_request.code_challenge_method.to_owned();
    let redirect_uri_cloned = redirect_uri.to_owned();
    let scope = par_request.scope.to_owned();
    let used = false;
    _ = db
        .get()
        .await
        .expect("Failed to get database connection")
        .interact(move |conn| {
            insert_into(AuthCodeSchema::oauth_authorization_codes)
                .values((
                    AuthCodeSchema::code.eq(code_cloned),
                    AuthCodeSchema::client_id.eq(client_id),
                    AuthCodeSchema::subject.eq(subject),
                    AuthCodeSchema::code_challenge.eq(code_challenge),
                    AuthCodeSchema::code_challenge_method.eq(code_challenge_method),
                    AuthCodeSchema::redirect_uri.eq(redirect_uri_cloned),
                    AuthCodeSchema::scope.eq(scope),
                    AuthCodeSchema::created_at.eq(created_at),
                    AuthCodeSchema::expires_at.eq(expires_at),
                    AuthCodeSchema::used.eq(used),
                ))
                .execute(conn)
        })
        .await
        .expect("Failed to store authorization code")
        .expect("Failed to store authorization code");

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
/// RFC 7519 JSON Web Token (JWT) - 4.3.  Checking DPoP Proofs
/// To validate a DPoP proof, the receiving server MUST ensure the
/// following:
/// 1.   There is not more than one DPoP HTTP request header field.
/// 2.   The DPoP HTTP request header field value is a single and well-
///      formed JWT.
/// 3.   All required claims per Section 4.2 are contained in the JWT.
/// 4.   The typ JOSE Header Parameter has the value dpop+jwt.
/// 5.   The alg JOSE Header Parameter indicates a registered asymmetric
///      digital signature algorithm [IANA.JOSE.ALGS], is not none, is
///      supported by the application, and is acceptable per local
///      policy.
/// 6.   The JWT signature verifies with the public key contained in the
///      jwk JOSE Header Parameter.
/// 7.   The jwk JOSE Header Parameter does not contain a private key.
/// 8.   The htm claim matches the HTTP method of the current request.
/// 9.   The htu claim matches the HTTP URI value for the HTTP request in
///      which the JWT was received, ignoring any query and fragment
///      parts.
/// 10.  If the server provided a nonce value to the client, the nonce
///      claim matches the server-provided nonce value.
/// 11.  The creation time of the JWT, as determined by either the iat
///      claim or a server managed timestamp via the nonce claim, is
///      within an acceptable window (see Section 11.1).
/// 12.  If presented to a protected resource in conjunction with an
///      access token,
///      *  ensure that the value of the ath claim equals the hash of
///         that access token, and
///      *  confirm that the public key to which the access token is
///         bound matches the public key from the DPoP proof.
#[expect(clippy::too_many_lines)]
async fn verify_dpop_proof(
    dpop_token: &str,
    http_method: &str,
    http_uri: &str,
    db: &Pool,
    access_token: Option<&str>,
    bound_key_thumbprint: Option<&str>,
) -> Result<String> {
    // Parse the DPoP token
    let (header, claims) = parse_jwt(dpop_token)?;

    // 1. Verify "typ" is "dpop+jwt" (requirement #4)
    if header.get("typ").and_then(Value::as_str) != Some("dpop+jwt") {
        return Err(Error::with_status(
            StatusCode::BAD_REQUEST,
            anyhow!("Invalid DPoP token type"),
        ));
    }

    // 2. Check alg header (requirement #5)
    let alg = header
        .get("alg")
        .and_then(Value::as_str)
        .context("Missing alg in DPoP header")?;
    if alg == "none" || !["RS256", "ES256", "ES256K", "PS256"].contains(&alg) {
        return Err(Error::with_status(
            StatusCode::BAD_REQUEST,
            anyhow!("Unsupported or insecure signature algorithm"),
        ));
    }

    // 3. Extract JWK and verify no private key components (requirement #7)
    let jwk = header.get("jwk").context("missing jwk in DPoP header")?;
    if jwk.get("d").is_some() || jwk.get("p").is_some() || jwk.get("q").is_some() {
        return Err(Error::with_status(
            StatusCode::BAD_REQUEST,
            anyhow!("JWK contains private key components"),
        ));
    }

    // 4. Calculate JWK thumbprint
    let thumbprint = calculate_jwk_thumbprint(jwk)?;

    // 5. Verify JWT signature (requirement #6)
    // TODO: Implement signature verification with the JWK

    // 6. Verify required claims (requirement #3)
    let jti = claims
        .get("jti")
        .and_then(Value::as_str)
        .context("Missing jti claim in DPoP token")?;

    // 7. Check HTTP method matches htm claim (requirement #8)
    if claims.get("htm").and_then(Value::as_str) != Some(http_method) {
        return Err(Error::with_status(
            StatusCode::BAD_REQUEST,
            anyhow!("DPoP token HTTP method mismatch"),
        ));
    }

    // 8. Check HTTP URI matches htu claim (requirement #9)
    // Should perform proper URI normalization for production use
    if claims.get("htu").and_then(Value::as_str) != Some(http_uri) {
        return Err(Error::with_status(
            StatusCode::BAD_REQUEST,
            anyhow!(
                "DPoP token HTTP URI mismatch: expected {}, got {}",
                http_uri,
                claims.get("htu").and_then(Value::as_str).unwrap_or("None")
            ),
        ));
    }

    // 9. Verify token timestamps (requirement #11)
    let now = chrono::Utc::now().timestamp();

    // Check creation time (iat)
    if let Some(iat) = claims.get("iat").and_then(Value::as_i64) {
        // Token not too old (5 minute max age)
        if iat < now.saturating_sub(300) {
            return Err(Error::with_status(
                StatusCode::BAD_REQUEST,
                anyhow!("DPoP token too old"),
            ));
        }

        // Token not in the future (with clock skew allowance)
        if iat > now.saturating_add(60) {
            return Err(Error::with_status(
                StatusCode::BAD_REQUEST,
                anyhow!("DPoP token creation time is in the future"),
            ));
        }
    }

    // Check expiration (exp) if present
    let exp = claims
        .get("exp")
        .and_then(Value::as_i64)
        .unwrap_or_else(|| now.saturating_add(60)); // Default 60s if not present

    if now >= exp {
        return Err(Error::with_status(
            StatusCode::BAD_REQUEST,
            anyhow!("DPoP token has expired"),
        ));
    }

    // 10. Verify access token binding (requirement #12)
    if let Some(token) = access_token {
        // Verify ath claim matches token hash
        if let Some(ath) = claims.get("ath").and_then(Value::as_str) {
            let mut hasher = Sha256::new();
            hasher.update(token.as_bytes());
            let token_hash =
                base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hasher.finalize());

            if ath != token_hash {
                return Err(Error::with_status(
                    StatusCode::BAD_REQUEST,
                    anyhow!("DPoP access token hash mismatch"),
                ));
            }
        } else {
            return Err(Error::with_status(
                StatusCode::BAD_REQUEST,
                anyhow!("Missing ath claim for DPoP with access token"),
            ));
        }

        // Verify key binding matches
        if let Some(expected_thumbprint) = bound_key_thumbprint {
            if thumbprint != expected_thumbprint {
                return Err(Error::with_status(
                    StatusCode::BAD_REQUEST,
                    anyhow!("DPoP key doesn't match token-bound key"),
                ));
            }
        }
    }

    // 11. Check for replay attacks via JTI tracking
    use crate::schema::pds::oauth_used_jtis::dsl as JtiSchema;
    let jti_clone = jti.to_owned();
    let jti_used = db
        .get()
        .await
        .expect("Failed to get database connection")
        .interact(move |conn| {
            JtiSchema::oauth_used_jtis
                .filter(JtiSchema::jti.eq(jti_clone))
                .count()
                .get_result::<i64>(conn)
                .optional()
        })
        .await
        .expect("Failed to check JTI")
        .expect("Failed to check JTI")
        .unwrap_or(0);

    if jti_used > 0 {
        return Err(Error::with_status(
            StatusCode::BAD_REQUEST,
            anyhow!("DPoP token has been replayed"),
        ));
    }

    // 12. Store the JTI to prevent replay attacks
    let jti_cloned = jti.to_owned();
    let issuer = thumbprint.to_owned();
    let created_at = now;
    let expires_at = exp;
    _ = db
        .get()
        .await
        .expect("Failed to get database connection")
        .interact(move |conn| {
            insert_into(JtiSchema::oauth_used_jtis)
                .values((
                    JtiSchema::jti.eq(jti_cloned),
                    JtiSchema::issuer.eq(issuer),
                    JtiSchema::created_at.eq(created_at),
                    JtiSchema::expires_at.eq(expires_at),
                ))
                .execute(conn)
        })
        .await
        .expect("Failed to store JTI")
        .expect("Failed to store JTI");

    // 13. Cleanup expired JTIs periodically (1% chance on each request)
    if thread_rng().gen_range(0_i32..100_i32) == 0_i32 {
        let now_clone = now.to_owned();
        _ = db
            .get()
            .await
            .expect("Failed to get database connection")
            .interact(move |conn| {
                delete(JtiSchema::oauth_used_jtis)
                    .filter(JtiSchema::expires_at.lt(now_clone))
                    .execute(conn)
            })
            .await
            .expect("Failed to clean up expired JTIs")
            .expect("Failed to clean up expired JTIs");
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
    let mut hasher = Sha256::new();
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
    State(db): State<Pool>,
    State(skey): State<SigningKey>,
    State(config): State<AppConfig>,
    State(client): State<Client>,
    headers: HeaderMap,
    Json(form_data): Json<HashMap<String, String>>,
) -> Result<Json<Value>> {
    // Extract form parameters
    let grant_type = form_data
        .get("grant_type")
        .context("grant_type is required")?;
    let client_id = form_data
        .get("client_id")
        .context("client_id is required")?;

    // Validate DPoP header (Rule 1: Ensure DPoP is used)
    let dpop_token = headers
        .get("DPoP")
        .context("DPoP header is required")?
        .to_str()
        .context("Invalid DPoP header")?;

    // Get client metadata to determine client type (public vs confidential)
    let client_metadata = fetch_client_metadata(&client, client_id).await?;
    let is_confidential_client = client_metadata
        .get("token_endpoint_auth_method")
        .and_then(Value::as_str)
        .unwrap_or("none")
        == "private_key_jwt";

    // Verify DPoP proof
    let dpop_thumbprint_res = verify_dpop_proof(
        dpop_token,
        "POST",
        &format!("https://{}/oauth/token", config.host_name),
        &db,
        None,
        None,
    )
    .await?;

    if is_confidential_client {
        // Rule 3: Check client authentication consistency
        // For confidential clients, verify client_assertion
        let client_assertion_type = form_data
            .get("client_assertion_type")
            .context("client_assertion_type required for private_key_jwt auth")?;
        let _client_assertion = form_data
            .get("client_assertion")
            .context("client_assertion required for private_key_jwt auth")?;

        if client_assertion_type != "urn:ietf:params:oauth:client-assertion-type:jwt-bearer" {
            return Err(Error::with_status(
                StatusCode::BAD_REQUEST,
                anyhow!("Invalid client_assertion_type"),
            ));
        }

        // Verify client assertion JWT
        // This would involve similar logic to verify_dpop_proof but for client auth
        //
        // WIP: Practically unimplemented
        //
        // TODO: Figure out how this actually works

        // verify_client_assertion(&client, client_id, client_assertion).await?;

        // Rule 4: Ensure DPoP and client_assertion use different keys
        // let client_assertion_thumbprint = calculate_client_assertion_thumbprint(client_assertion)?;
        // if client_assertion_thumbprint == dpop_thumbprint {
        //     return Err(Error::with_status(
        //         StatusCode::BAD_REQUEST,
        //         anyhow!("DPoP proof and client assertion must use different keypairs"),
        //     ));
        // }
    } else {
        // Rule 2: For public clients, check if this DPoP key has been used before
        use crate::schema::pds::oauth_refresh_tokens::dsl as RefreshTokenSchema;
        let dpop_thumbprint_clone = dpop_thumbprint_res.to_owned();
        let client_id_clone = client_id.to_owned();
        let is_key_reused = db
            .get()
            .await
            .expect("Failed to get database connection")
            .interact(move |conn| {
                RefreshTokenSchema::oauth_refresh_tokens
                    .filter(RefreshTokenSchema::dpop_thumbprint.eq(dpop_thumbprint_clone))
                    .filter(RefreshTokenSchema::client_id.eq(client_id_clone))
                    .count()
                    .get_result::<i64>(conn)
                    .optional()
            })
            .await
            .expect("Failed to check key usage history")
            .expect("Failed to check key usage history")
            .unwrap_or(0)
            > 0;

        if is_key_reused && grant_type == "authorization_code" {
            return Err(Error::with_status(
                StatusCode::BAD_REQUEST,
                anyhow!("Public clients must use a new key for each token request"),
            ));
        }
    }

    match grant_type.as_str() {
        "authorization_code" => {
            // Process authorization code grant
            let code = form_data.get("code").context("code is required")?;
            let code_verifier = form_data
                .get("code_verifier")
                .context("code_verifier is required")?;
            let redirect_uri = form_data
                .get("redirect_uri")
                .context("redirect_uri is required")?;

            let timestamp = chrono::Utc::now().timestamp();

            // Retrieve and validate the authorization code
            use crate::schema::pds::oauth_authorization_codes::dsl as AuthCodeSchema;
            #[derive(Queryable, Selectable, Serialize)]
            #[diesel(table_name = crate::schema::pds::oauth_authorization_codes)]
            #[diesel(check_for_backend(sqlite::Sqlite))]
            struct AuthCode {
                code: String,
                client_id: String,
                subject: String,
                code_challenge: String,
                code_challenge_method: String,
                redirect_uri: String,
                scope: Option<String>,
                created_at: i64,
                expires_at: i64,
                used: bool,
            }
            let code_clone = code.to_owned();
            let client_id_clone = client_id.to_owned();
            let redirect_uri_clone = redirect_uri.to_owned();
            let auth_code = db
                .get()
                .await
                .expect("Failed to get database connection")
                .interact(move |conn| {
                    AuthCodeSchema::oauth_authorization_codes
                        .filter(AuthCodeSchema::code.eq(code_clone))
                        .filter(AuthCodeSchema::client_id.eq(client_id_clone))
                        .filter(AuthCodeSchema::redirect_uri.eq(redirect_uri_clone))
                        .filter(AuthCodeSchema::expires_at.gt(timestamp))
                        .filter(AuthCodeSchema::used.eq(false))
                        .first::<AuthCode>(conn)
                        .optional()
                })
                .await
                .expect("Failed to query authorization code")
                .expect("Failed to query authorization code")
                .expect("Failed to query authorization code");

            // Verify PKCE code challenge
            verify_pkce(
                code_verifier,
                &auth_code.code_challenge,
                &auth_code.code_challenge_method,
            )?;

            // Mark the code as used
            let code_cloned = code.to_owned();
            _ = db
                .get()
                .await
                .expect("Failed to get database connection")
                .interact(move |conn| {
                    update(AuthCodeSchema::oauth_authorization_codes)
                        .filter(AuthCodeSchema::code.eq(code_cloned))
                        .set(AuthCodeSchema::used.eq(true))
                        .execute(conn)
                })
                .await
                .expect("Failed to mark code as used")
                .expect("Failed to mark code as used");

            // Generate tokens with appropriate lifetimes
            let now = chrono::Utc::now().timestamp();

            // Rule 5: Access token valid for short period
            let access_token_expires_in = 3600_i64; // 1 hour (maximum allowed)
            let access_token_expires_at = now.saturating_add(access_token_expires_in);

            // Rule 11 & 12: Different refresh token lifetimes based on client type
            let refresh_token_expires_at = if is_confidential_client {
                now.saturating_add(15_552_000_i64) // 6 months for confidential clients
            } else {
                now.saturating_add(604_800_i64) // 1 week maximum for public clients
            };

            // Rule 5: Include subject claim with user DID
            let access_token_claims = json!({
                "iss": format!("https://{}", config.host_name),
                "sub": auth_code.subject, // User's DID
                "aud": client_id,
                "exp": access_token_expires_at,
                "iat": now,
                "cnf": {
                    "jkt": dpop_thumbprint_res // Rule 1: Bind to DPoP key
                },
                "scope": auth_code.scope
            });

            let access_token = crate::auth::sign(&skey, "at+jwt", &access_token_claims)
                .context("failed to sign access token")?;

            // Create refresh token with similar binding
            let refresh_token_claims = json!({
                "iss": format!("https://{}", config.host_name),
                "sub": auth_code.subject,
                "aud": client_id,
                "exp": refresh_token_expires_at,
                "iat": now,
                "cnf": {
                    "jkt": dpop_thumbprint_res // Rule 1: Bind to DPoP key
                },
                "scope": auth_code.scope
            });

            let refresh_token = crate::auth::sign(&skey, "rt+jwt", &refresh_token_claims)
                .context("failed to sign refresh token")?;

            // Store the refresh token with DPoP binding
            use crate::schema::pds::oauth_refresh_tokens::dsl as RefreshTokenSchema;
            let refresh_token_cloned = refresh_token.to_owned();
            let client_id_cloned = client_id.to_owned();
            let subject = auth_code.subject.to_owned();
            let dpop_thumbprint_cloned = dpop_thumbprint_res.to_owned();
            let scope = auth_code.scope.to_owned();
            let created_at = now;
            let expires_at = refresh_token_expires_at;
            _ = db
                .get()
                .await
                .expect("Failed to get database connection")
                .interact(move |conn| {
                    insert_into(RefreshTokenSchema::oauth_refresh_tokens)
                        .values((
                            RefreshTokenSchema::token.eq(refresh_token_cloned),
                            RefreshTokenSchema::client_id.eq(client_id_cloned),
                            RefreshTokenSchema::subject.eq(subject),
                            RefreshTokenSchema::dpop_thumbprint.eq(dpop_thumbprint_cloned),
                            RefreshTokenSchema::scope.eq(scope),
                            RefreshTokenSchema::created_at.eq(created_at),
                            RefreshTokenSchema::expires_at.eq(expires_at),
                            RefreshTokenSchema::revoked.eq(false),
                        ))
                        .execute(conn)
                })
                .await
                .expect("Failed to store refresh token")
                .expect("Failed to store refresh token");

            // Return token response with the subject claim
            Ok(Json(json!({
                "access_token": access_token,
                "token_type": "DPoP",
                "expires_in": access_token_expires_in,
                "refresh_token": refresh_token,
                "scope": auth_code.scope,
                "sub": auth_code.subject // Rule 5: Include subject claim
            })))
        }
        "refresh_token" => {
            // Process refresh token grant
            let refresh_token = form_data
                .get("refresh_token")
                .context("refresh_token is required")?;

            let timestamp = chrono::Utc::now().timestamp();

            // Rules 7 & 8: Verify refresh token and DPoP consistency
            // Retrieve the refresh token
            use crate::schema::pds::oauth_refresh_tokens::dsl as RefreshTokenSchema;
            #[derive(Queryable, Selectable, Serialize)]
            #[diesel(table_name = crate::schema::pds::oauth_refresh_tokens)]
            #[diesel(check_for_backend(sqlite::Sqlite))]
            struct TokenData {
                token: String,
                client_id: String,
                subject: String,
                dpop_thumbprint: String,
                scope: Option<String>,
                created_at: i64,
                expires_at: i64,
                revoked: bool,
            }
            let dpop_thumbprint_clone = dpop_thumbprint_res.to_owned();
            let refresh_token_clone = refresh_token.to_owned();
            let client_id_clone = client_id.to_owned();
            let token_data = db
                .get()
                .await
                .expect("Failed to get database connection")
                .interact(move |conn| {
                    RefreshTokenSchema::oauth_refresh_tokens
                        .filter(RefreshTokenSchema::token.eq(refresh_token_clone))
                        .filter(RefreshTokenSchema::client_id.eq(client_id_clone))
                        .filter(RefreshTokenSchema::expires_at.gt(timestamp))
                        .filter(RefreshTokenSchema::revoked.eq(false))
                        .filter(RefreshTokenSchema::dpop_thumbprint.eq(dpop_thumbprint_clone))
                        .first::<TokenData>(conn)
                        .optional()
                })
                .await
                .expect("Failed to query refresh token")
                .expect("Failed to query refresh token")
                .expect("Failed to query refresh token");

            // Rule 10: For confidential clients, verify key is still advertised in their jwks
            if is_confidential_client {
                let client_still_advertises_key = true; // Implement actual check against client jwks
                if !client_still_advertises_key {
                    // Revoke all tokens bound to this key
                    let client_id_cloned = client_id.to_owned();
                    let dpop_thumbprint_cloned = dpop_thumbprint_res.to_owned();
                    _ = db
                        .get()
                        .await
                        .expect("Failed to get database connection")
                        .interact(move |conn| {
                            update(RefreshTokenSchema::oauth_refresh_tokens)
                                .filter(RefreshTokenSchema::client_id.eq(client_id_cloned))
                                .filter(
                                    RefreshTokenSchema::dpop_thumbprint.eq(dpop_thumbprint_cloned),
                                )
                                .set(RefreshTokenSchema::revoked.eq(true))
                                .execute(conn)
                        })
                        .await
                        .expect("Failed to revoke tokens")
                        .expect("Failed to revoke tokens");

                    return Err(Error::with_status(
                        StatusCode::BAD_REQUEST,
                        anyhow!("Key no longer advertised by client"),
                    ));
                }
            }

            // Rotate the refresh token
            let refresh_token_cloned = refresh_token.to_owned();
            _ = db
                .get()
                .await
                .expect("Failed to get database connection")
                .interact(move |conn| {
                    update(RefreshTokenSchema::oauth_refresh_tokens)
                        .filter(RefreshTokenSchema::token.eq(refresh_token_cloned))
                        .set(RefreshTokenSchema::revoked.eq(true))
                        .execute(conn)
                })
                .await
                .expect("Failed to revoke old refresh token")
                .expect("Failed to revoke old refresh token");

            // Generate new tokens
            let now = chrono::Utc::now().timestamp();
            let access_token_expires_in = 3600_i64;
            let access_token_expires_at = now.saturating_add(access_token_expires_in);

            // Maintain the original expiry policy for refresh tokens
            let original_duration = token_data.expires_at.saturating_sub(token_data.created_at);
            let refresh_token_expires_at = now.saturating_add(original_duration);

            // Create access token
            let access_token_claims = json!({
                "iss": format!("https://{}", config.host_name),
                "sub": token_data.subject,
                "aud": client_id,
                "exp": access_token_expires_at,
                "iat": now,
                "cnf": {
                    "jkt": dpop_thumbprint_res
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
                    "jkt": dpop_thumbprint_res
                },
                "scope": token_data.scope
            });

            let new_refresh_token = crate::auth::sign(&skey, "rt+jwt", &new_refresh_token_claims)
                .context("failed to sign refresh token")?;

            // Store the new refresh token
            let new_refresh_token_cloned = new_refresh_token.to_owned();
            let client_id_cloned = client_id.to_owned();
            let subject = token_data.subject.to_owned();
            let dpop_thumbprint_cloned = dpop_thumbprint_res.to_owned();
            let scope = token_data.scope.to_owned();
            let created_at = now;
            let expires_at = refresh_token_expires_at;
            _ = db
                .get()
                .await
                .expect("Failed to get database connection")
                .interact(move |conn| {
                    insert_into(RefreshTokenSchema::oauth_refresh_tokens)
                        .values((
                            RefreshTokenSchema::token.eq(new_refresh_token_cloned),
                            RefreshTokenSchema::client_id.eq(client_id_cloned),
                            RefreshTokenSchema::subject.eq(subject),
                            RefreshTokenSchema::dpop_thumbprint.eq(dpop_thumbprint_cloned),
                            RefreshTokenSchema::scope.eq(scope),
                            RefreshTokenSchema::created_at.eq(created_at),
                            RefreshTokenSchema::expires_at.eq(expires_at),
                            RefreshTokenSchema::revoked.eq(false),
                        ))
                        .execute(conn)
                })
                .await
                .expect("Failed to store refresh token")
                .expect("Failed to store refresh token");

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
///
/// WIP: Practically unimplemented
///
/// TODO: Figure out if/how this actually works
async fn jwks(State(skey): State<SigningKey>) -> Result<Json<Value>> {
    let did_string = skey.did();

    // Extract the public key data from the DID string
    let (_, public_key_bytes) =
        atrium_crypto::did::parse_did_key(&did_string).context("failed to parse did key")?;

    // Secp256k1 uncompressed public keys should be 65 bytes: 0x04 + x(32 bytes) + y(32 bytes)
    if public_key_bytes.len() != 65 || public_key_bytes.first().copied() != Some(0x04) {
        return Err(Error::with_status(
            StatusCode::INTERNAL_SERVER_ERROR,
            anyhow!("unexpected public key format"),
        ));
    }

    // Extract and encode the X and Y coordinates
    let x_coord = public_key_bytes
        .get(1..33)
        .map(|slice| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(slice))
        .context("failed to extract X coordinate")?;

    let y_coord = public_key_bytes
        .get(33..65)
        .map(|slice| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(slice))
        .context("failed to extract Y coordinate")?;

    // Create a stable key ID based on the DID
    let key_id = did_string.strip_prefix("did:key:").unwrap_or(&did_string);

    let jwk = json!({
        "kty": "EC",
        "crv": "secp256k1",
        "kid": key_id,
        "use": "sig",
        "alg": "ES256K",
        "x": x_coord,
        "y": y_coord
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
    State(db): State<Pool>,
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
    use crate::schema::pds::oauth_refresh_tokens::dsl as RefreshTokenSchema;
    let token_cloned = token.to_owned();
    _ = db
        .get()
        .await
        .expect("Failed to get database connection")
        .interact(move |conn| {
            update(RefreshTokenSchema::oauth_refresh_tokens)
                .filter(RefreshTokenSchema::token.eq(token_cloned))
                .set(RefreshTokenSchema::revoked.eq(true))
                .execute(conn)
        })
        .await
        .expect("Failed to revoke token")
        .expect("Failed to revoke token");

    // RFC7009 requires a 200 OK with an empty response
    Ok(Json(json!({})))
}

/// Token Introspection endpoint
/// - POST `/oauth/introspect`
///
/// Implements RFC7662 for introspecting tokens
async fn introspect(
    State(db): State<Pool>,
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
        use crate::schema::pds::oauth_refresh_tokens::dsl as RefreshTokenSchema;
        let token_cloned = token.to_owned();
        let is_revoked = db
            .get()
            .await
            .expect("Failed to get database connection")
            .interact(move |conn| {
                RefreshTokenSchema::oauth_refresh_tokens
                    .filter(RefreshTokenSchema::token.eq(token_cloned))
                    .select(RefreshTokenSchema::revoked)
                    .first::<bool>(conn)
                    .optional()
            })
            .await
            .expect("Failed to query token")
            .expect("Failed to query token")
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
