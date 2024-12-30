use axum::Router;

use crate::AppState;

mod repo;
mod server;

pub fn routes() -> Router<AppState> {
    Router::new()
        .merge(repo::routes()) // com.atproto.repo
        .merge(server::routes()) // com.atproto.server
}
