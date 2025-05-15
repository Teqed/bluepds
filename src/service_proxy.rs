/// Service proxy.
///
/// Reference: <https://atproto.com/specs/xrpc#service-proxying>
use anyhow::{Context as _, anyhow};
use atrium_api::types::string::Did;
use atrium_crypto::keypair::{Export as _, Secp256k1Keypair};
use axum::{
    Router,
    body::Body,
    extract::{FromRef, Request, State},
    http::{self, HeaderMap, Response, StatusCode, Uri},
    response::IntoResponse,
    routing::get,
};
use azure_core::credentials::TokenCredential;
use clap::Parser;
use clap_verbosity_flag::{InfoLevel, Verbosity, log::LevelFilter};
use deadpool_diesel::sqlite::Pool;
use diesel::prelude::*;
use diesel_migrations::{EmbeddedMigrations, embed_migrations};
use figment::{Figment, providers::Format as _};
use http_cache_reqwest::{CacheMode, HttpCacheOptions, MokaManager};
use rand::Rng as _;
use serde::{Deserialize, Serialize};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    str::FromStr as _,
    sync::Arc,
};
use tokio::net::TcpListener;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::{info, warn};
use uuid::Uuid;

use super::{Client, Error, Result};
use crate::{AuthenticatedUser, SigningKey};

pub(super) async fn service_proxy(
    uri: Uri,
    user: AuthenticatedUser,
    State(skey): State<SigningKey>,
    State(client): State<reqwest::Client>,
    headers: HeaderMap,
    request: Request<Body>,
) -> Result<Response<Body>> {
    let url_path = uri.path_and_query().context("invalid service proxy url")?;
    let lxm = url_path
        .path()
        .strip_prefix("/")
        .with_context(|| format!("invalid service proxy url prefix: {}", url_path.path()))?;

    let user_did = user.did();
    let (did, id) = match headers.get("atproto-proxy") {
        Some(val) => {
            let val =
                std::str::from_utf8(val.as_bytes()).context("proxy header not valid utf-8")?;

            let (did, id) = val.split_once('#').context("invalid proxy header")?;

            let did =
                Did::from_str(did).map_err(|e| anyhow!("atproto proxy not a valid DID: {e}"))?;

            (did, format!("#{id}"))
        }
        // HACK: Assume the bluesky appview by default.
        None => (
            Did::new("did:web:api.bsky.app".to_owned())
                .expect("service proxy should be a valid DID"),
            "#bsky_appview".to_owned(),
        ),
    };

    let did_doc = super::did::resolve(&Client::new(client.clone(), []), did.clone())
        .await
        .with_context(|| format!("failed to resolve did document {}", did.as_str()))?;

    let Some(service) = did_doc.service.iter().find(|s| s.id == id) else {
        return Err(Error::with_status(
            StatusCode::BAD_REQUEST,
            anyhow!("could not find resolve service #{id}"),
        ));
    };

    let target_url: url::Url = service
        .service_endpoint
        .join(&format!("/xrpc{url_path}"))
        .context("failed to construct target url")?;

    let exp = (chrono::Utc::now().checked_add_signed(chrono::Duration::minutes(1)))
        .context("should be valid expiration datetime")?
        .timestamp();
    let jti = rand::thread_rng()
        .sample_iter(rand::distributions::Alphanumeric)
        .take(10)
        .map(char::from)
        .collect::<String>();

    // Mint a bearer token by signing a JSON web token.
    // https://github.com/DavidBuchanan314/millipds/blob/5c7529a739d394e223c0347764f1cf4e8fd69f94/src/millipds/appview_proxy.py#L47-L59
    let token = super::auth::sign(
        &skey,
        "JWT",
        &serde_json::json!({
            "iss": user_did.as_str(),
            "aud": did.as_str(),
            "lxm": lxm,
            "exp": exp,
            "jti": jti,
        }),
    )
    .context("failed to sign jwt")?;

    let mut h = HeaderMap::new();
    if let Some(hdr) = request.headers().get("atproto-accept-labelers") {
        drop(h.insert("atproto-accept-labelers", hdr.clone()));
    }
    if let Some(hdr) = request.headers().get(http::header::CONTENT_TYPE) {
        drop(h.insert(http::header::CONTENT_TYPE, hdr.clone()));
    }

    let r = client
        .request(request.method().clone(), target_url)
        .headers(h)
        .header(http::header::AUTHORIZATION, format!("Bearer {token}"))
        .body(reqwest::Body::wrap_stream(
            request.into_body().into_data_stream(),
        ))
        .send()
        .await
        .context("failed to send request")?;

    let mut resp = Response::builder().status(r.status());
    if let Some(hdrs) = resp.headers_mut() {
        *hdrs = r.headers().clone();
    }

    let resp = resp
        .body(Body::from_stream(r.bytes_stream()))
        .context("failed to construct response")?;

    Ok(resp)
}
