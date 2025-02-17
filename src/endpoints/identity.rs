use anyhow::Context;
use atrium_api::com::atproto::identity;
use axum::{
    extract::{Query, State},
    routing::{get, post},
    Json, Router,
};
use constcat::concat;

use crate::{
    auth::AuthenticatedUser, config::AppConfig, firehose::FirehoseProducer, AppState, Db, Result,
    SigningKey,
};

async fn resolve_handle(
    State(db): State<Db>,
    Query(input): Query<identity::resolve_handle::Parameters>,
) -> Result<Json<identity::resolve_handle::Output>> {
    let handle = input.handle.as_str();
    let did = sqlx::query_scalar!(r#"SELECT did FROM handles WHERE handle = ?"#, handle)
        .fetch_one(&db)
        .await
        .context("failed to query did")?;

    let did = atrium_api::types::string::Did::new(did).unwrap();
    Ok(Json(identity::resolve_handle::OutputData { did }.into()))
}

async fn update_handle(
    user: AuthenticatedUser,
    State(skey): State<SigningKey>,
    State(config): State<AppConfig>,
    State(db): State<Db>,
    State(fhp): State<FirehoseProducer>,
    Json(input): Json<identity::update_handle::Input>,
) -> Result<()> {
    todo!()
}

#[rustfmt::skip]
pub fn routes() -> Router<AppState> {
    // UG /xrpc/com.atproto.identity.resolveHandle
    // AP /xrpc/com.atproto.identity.updateHandle
    Router::new()
        .route(concat!("/", identity::resolve_handle::NSID), get(resolve_handle))
        .route(concat!("/", identity::update_handle::NSID),  post(update_handle))
}
