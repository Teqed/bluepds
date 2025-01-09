use anyhow::Context;
use atrium_api::{
    com::atproto::repo,
    types::string::{Cid, Did},
};
use atrium_repo::mst::Tree;
use axum::{extract::State, routing::get, Json, Router};
use constcat::concat;

use crate::{auth::AuthenticatedUser, config::AppConfig, AppState, Cred, Result};

async fn create_record(
    user: AuthenticatedUser,
    Json(input): Json<repo::create_record::Input>,
) -> Result<Json<repo::create_record::Output>> {
    let storage = user
        .storage()
        .await
        .context("failed to open user storage")?;

    // let mst = Tree::open(&mut storage, &Cid::default());

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
