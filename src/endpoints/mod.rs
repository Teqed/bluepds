//! Root module for all endpoints.
mod identity;
mod oauth;
mod repo;
mod server;
mod sync;

use axum::{Json, Router, routing::get};
use serde_json::json;

use crate::{AppState, Result};

/// Health check endpoint. Returns name and version of the service.
pub(crate) async fn health() -> Result<Json<serde_json::Value>> {
    Ok(Json(json!({
        "version": concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION")),
    })))
}

/// Register all root routes.
pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/_health", get(health))
        .merge(identity::routes()) // com.atproto.identity
        .merge(oauth::routes()) // oauth
        .merge(repo::routes()) // com.atproto.repo
        .merge(server::routes()) // com.atproto.server
        .merge(sync::routes()) // com.atproto.sync
}
