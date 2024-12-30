use atrium_api::{
    com::atproto::repo,
    types::string::{Cid, Did},
};
use axum::{extract::State, routing::get, Json, Router};
use constcat::concat;

use crate::{config::AppConfig, AppState, Result};

async fn create_record(
    State(config): State<AppConfig>,
) -> Result<Json<repo::create_record::Output>> {
    Ok(Json(
        repo::create_record::OutputData {
            cid: todo!(),
            commit: None,
            uri: "".to_string(),
            validation_status: Some("unknown".to_string()),
        }
        .into(),
    ))
}

pub fn routes() -> Router<AppState> {
    Router::new().route(concat!("/", repo::create_record::NSID), get(create_record))
}
