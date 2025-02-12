use axum::{routing::get, Json, Router};
use serde_json::json;

use crate::{AppState, Result};

mod repo;
mod server;
mod sync;

pub async fn health() -> Result<Json<serde_json::Value>> {
    Ok(Json(json!({
        "version": "bluepds"
    })))
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/_health", get(health))
        .merge(repo::routes()) // com.atproto.repo
        .merge(server::routes()) // com.atproto.server
        .merge(sync::routes()) // com.atproto.sync
}
