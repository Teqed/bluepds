use std::str::FromStr;

use anyhow::Context;
use atrium_api::com::atproto::sync;
use atrium_repo::{blockstore::AsyncBlockStoreRead, Cid};
use axum::{
    body::Bytes,
    extract::{Query, State, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use constcat::concat;

use crate::{config::AppConfig, firehose::FirehoseProducer, storage, AppState, Db, Result};

async fn get_blob(
    State(config): State<AppConfig>,
    Query(input): Query<sync::get_blob::Parameters>,
) -> Result<Bytes> {
    let mut repo = storage::open_store(&config.repo, &input.did)
        .await
        .context("failed to open repo")?;

    let blob = repo
        .read_block(input.cid.as_ref())
        .await
        .context("failed to find blob")?;

    Ok(Bytes::copy_from_slice(&blob))
}

async fn get_latest_commit(
    State(config): State<AppConfig>,
    State(db): State<Db>,
    Query(input): Query<sync::get_latest_commit::Parameters>,
) -> Result<Json<sync::get_latest_commit::Output>> {
    let did = input.did.as_str();

    let cid = sqlx::query_scalar!(
        r#"
        SELECT root FROM accounts
        WHERE did = ?
        "#,
        did
    )
    .fetch_one(&db)
    .await
    .context("failed to query database")?;

    let cid = Cid::from_str(&cid).expect("cid conversion unexpectedly failed");

    let repo = storage::open_repo(&config.repo, did, cid)
        .await
        .context("failed to open repo")?;

    let commit = repo.commit();

    Ok(Json(
        sync::get_latest_commit::OutputData {
            cid: atrium_api::types::string::Cid::new(cid),
            rev: commit.rev().to_string(),
        }
        .into(),
    ))
}

async fn get_record(
    State(config): State<AppConfig>,
    State(db): State<Db>,
    Query(input): Query<sync::get_record::Parameters>,
) -> Result<Bytes> {
    todo!()
}

async fn subscribe_repos(
    ws: WebSocketUpgrade,
    State(fh): State<FirehoseProducer>,
) -> impl IntoResponse {
    ws.on_upgrade(|ws| async move {
        fh.connect(ws).await;
    })
}

pub fn routes() -> axum::Router<AppState> {
    // UG /xrpc/com.atproto.sync.getBlob
    // UG /xrpc/com.atproto.sync.getBlocks
    // UG /xrpc/com.atproto.sync.getLatestCommit
    // UG /xrpc/com.atproto.sync.getRecord
    // UG /xrpc/com.atproto.sync.getRepoStatus
    // UG /xrpc/com.atproto.sync.getRepo
    // UG /xrpc/com.atproto.sync.listBlobs
    // UG /xrpc/com.atproto.sync.listRepos
    // UG /xrpc/com.atproto.sync.subscribeRepos
    Router::new()
        .route(concat!("/", sync::get_blob::NSID), get(get_blob))
        .route(
            concat!("/", sync::get_latest_commit::NSID),
            get(get_latest_commit),
        )
        .route(concat!("/", sync::get_record::NSID), get(get_record))
        .route(
            concat!("/", sync::subscribe_repos::NSID),
            get(subscribe_repos),
        )
}
