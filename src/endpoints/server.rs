use atrium_api::{com::atproto::server, types::string::Did};
use axum::{routing::get, Json, Router};

use crate::{AppState, Result};

async fn describe_server() -> Result<Json<server::describe_server::Output>> {
    Ok(Json(
        server::describe_server::OutputData {
            available_user_domains: vec![],
            contact: None,
            did: Did::new("did:web:pds.justinm.one".into()).unwrap(),
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
