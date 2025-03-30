use axum::{Json, Router, routing::get};
use serde_json::json;

use crate::{AppState, Result};

mod identity;
mod repo;
mod server;
mod sync;

pub(crate) async fn health() -> Result<Json<serde_json::Value>> {
    Ok(Json(json!({
        "version": "bluepds"
    })))
}

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/_health", get(health))
        .merge(identity::routes()) // com.atproto.identity
        .merge(repo::routes()) // com.atproto.repo
        .merge(server::routes()) // com.atproto.server
        .merge(sync::routes()) // com.atproto.sync
}
