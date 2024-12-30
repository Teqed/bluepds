use atrium_api::com::atproto::server;
use axum::{extract::State, routing::get, Json, Router};

use crate::{config::AppConfig, AppState, Result};

async fn describe_server(
    State(config): State<AppConfig>,
) -> Result<Json<server::describe_server::Output>> {
    Ok(Json(
        server::describe_server::OutputData {
            available_user_domains: vec![],
            contact: None,
            did: config.did,
            invite_code_required: Some(true),
            links: None,
            phone_verification_required: Some(true), // email verification
        }
        .into(),
    ))
}

pub fn routes() -> Router<AppState> {
    Router::new().route("/com.atproto.server.describeServer", get(describe_server))
}
