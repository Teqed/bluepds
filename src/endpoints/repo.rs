use atrium_api::{com::atproto::server, types::string::Did};
use axum::{routing::get, Json, Router};

use crate::{AppState, Result};

pub fn routes() -> Router<AppState> {
    Router::new()
}
