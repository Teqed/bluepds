//! OAuth endpoints

use axum::{
    Json, Router,
    routing::{get, post},
};
use serde_json::json;

use crate::{AppState, Result};

/// Authorization Server Metadata
/// - GET `/.well-known/oauth-protected-resource`
async fn protected_resource() -> Result<Json<serde_json::Value>> {
    Ok(Json(json!({
        "resource": "https://pds.shatteredsky.net",
        "authorization_servers": ["https://pds.shatteredsky.net"],
        "scopes_supported": [],
        "bearer_methods_supported": ["header"],
        "resource_documentation": "https://atproto.com",
    })))
}

/// - GET `/.well-known/oauth-authorization-server`
async fn authorization_server() -> Result<Json<serde_json::Value>> {
    Ok(Json(serde_json::json!({
        "issuer": "https://pds.shatteredsky.net",
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
        "jwks_uri": "https://pds.shatteredsky.net/oauth/jwks",
        "authorization_endpoint": "https://pds.shatteredsky.net/oauth/authorize",
        "token_endpoint": "https://pds.shatteredsky.net/oauth/token",
        "token_endpoint_auth_methods_supported": ["none", "private_key_jwt"],
        "token_endpoint_auth_signing_alg_values_supported": ["RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES256K", "ES384", "ES512"],
        "revocation_endpoint": "https://pds.shatteredsky.net/oauth/revoke",
        "introspection_endpoint": "https://pds.shatteredsky.net/oauth/introspect",
        "pushed_authorization_request_endpoint": "https://pds.shatteredsky.net/oauth/par",
        "require_pushed_authorization_requests": true,
        "dpop_signing_alg_values_supported": ["RS256", "RS384", "RS512", "PS256", "PS384", "PS512", "ES256", "ES256K", "ES384", "ES512"],
        "client_id_metadata_document_supported": true
    })))
}

/// Pushed Authorization Request
/// `/par`
/// ## Request
/// - redirect_uri - https://pdsls.dev/
/// - code_challenge - PDyHrK2ECSnOuV0ZbGm9gkJB2xEfYBOPr2tXcfa6GdU
/// - code_challenge_method - S256
/// - state - 3ebWb5RplSf1IXVNbbcLKw
/// - login_hint - quilling.dev
/// - response_mode - fragment
/// - response_type - code
/// - display - page
/// - scope - atproto transition:generic
/// - client_id - https://pdsls.dev/client-metadata.json
/// ## Response
/// - request_uri - urn:ietf:params:oauth:request_uri:req-76eb3e0feec73938a8965ef0f1167235
/// - expires_in - 299
/// TODO: Implement
async fn par() -> Result<Json<serde_json::Value>> {
    // For now, we'll just return a dummy response
    Ok(Json(json!({
        "request_uri": "urn:ietf:params:oauth:request_uri:req-76eb3e0feec73938a8596ef0f1167235",
        "expires_in": 299
    })))
}

/// - GET `/oauth/authorize`
/// # Parameters
/// - client_id - https://pdsls.dev/client-metadata.json
/// - request_uri - urn:ietf:params:oauth:request_uri:req-76eb3e0feec73938a8596ef0f1167235
async fn authorize() -> Result<()> {
    // TODO: Implement
    Ok(())
}

/// `/oauth/authorize/sign-in`
/// # Request body
/// {"username":"quilling.dev","password":"hunter12","remember":false,"locale":"en"}
/// # Response
/// {"account":{"sub":"did:plc:jrtgsidnmxaen4offglr5lsh","aud":"did:web:shimeji.us-east.host.bsky.network","email":"teqed@shatteredsky.net","email_verified":true,"preferred_username":"quilling.dev"},"consentRequired":true}
/// TODO: Implement
async fn authorize_signin() -> Result<Json<serde_json::Value>> {
    // TODO: Implement
    Ok(Json(json!({
        "account": {
            "sub": "did:plc:jrtgsidnmxaen4offglr5lsh",
            "aud": "did:web:shimeji.us-east.host.bsky.network",
            "email": "teqed@shatteredsky.net",
            "email_verified": true,
            "preferred_username": "quilling.dev"
        },
        "consentRequired": true
    })))
}

// Making a request against client_id will net you:
// {
//   "client_id": "https://pdsls.dev/client-metadata.json",
//   "client_name": "pdsls",
//   "client_uri": "https://pdsls.dev",
//   "redirect_uris": ["https://pdsls.dev/"],
//   "scope": "atproto transition:generic",
//   "grant_types": ["authorization_code", "refresh_token"],
//   "response_types": ["code"],
//   "token_endpoint_auth_method": "none",
//   "application_type": "web",
//   "dpop_bound_access_tokens": true
// }

/// POST https://entryway.example.com/oauth/token
/// ## Request
/// Content-Type: application/x-www-form-urlencoded
/// DPoP: <DPOP_PROOF_JWT>
/// grant_type=authorization_code
/// &code=<AUTHORIZATION_CODE>
/// &code_verifier=dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk
/// &client_id=https%3A%2F%2Fapp.example.com%2Fclient-metadata.json
/// &redirect_uri=https%3A%2F%2Fapp.example.com%2Fmy-app%2Foauth-callback
/// # Response
/// HTTP/1.1 200 OK
/// Content-Type: application/json
/// Cache-Control: no-store
/// {
///  "access_token": "Kz~8mXK1EalYznwH-LC-1fBAo.4Ljp~zsPE_NeO.gxU",
///  "token_type": "DPoP",
///  "expires_in": 2677,
///  "refresh_token": "Q..Zkm29lexi8VnWg2zPW1x-tgGad0Ibc3s3EwM_Ni4-g",
///  "scope": "atproto transition:generic",
///  "sub": "did:plc:jrtgsidnmxaen4offglr5lsh"
/// }
/// TODO
async fn token() -> Result<Json<serde_json::Value>> {
    // TODO: Implement
    Ok(Json(json!({
        "access_token": "Kz~8mXK1EalYznwH-LC-1fBAo.4Ljp~zsPE_NeO.gxU",
        "token_type": "DPoP",
        "expires_in": 2677,
        "refresh_token": "Q..Zkm29lexi8VnWg2zPW1x-tgGad0Ibc3s3EwM_Ni4-g",
        "scope": "atproto transition:generic",
        "sub": "did:plc:jrtgsidnmxaen4offglr5lsh"
    })))
}

/// Register oauth routes
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
        .route("/par", post(par))
        .route("/oauth/authorize", get(authorize))
        .route("/oauth/authorize/sign-in", post(authorize_signin))
        .route("/oauth/token", post(token))
}
