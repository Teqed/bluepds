use std::{collections::HashSet, str::FromStr};

use anyhow::Context;
use atrium_api::{com::atproto::sync, types::string::Did};
use atrium_repo::{
    blockstore::{AsyncBlockStoreRead, AsyncBlockStoreWrite, CarStore, DAG_CBOR, SHA2_256},
    Cid, Repository,
};
use axum::{
    body::Bytes,
    extract::{Query, State, WebSocketUpgrade},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use constcat::concat;
use futures::stream::TryStreamExt;
use tracing::info;

use crate::{
    config::{self, AppConfig},
    firehose::FirehoseProducer,
    storage, AppState, Db, Result,
};

async fn open_store<'a, 'b>(
    config: &config::RepoConfig,
    db: &Db,
    did: impl Into<String>,
) -> anyhow::Result<(impl AsyncBlockStoreRead + AsyncBlockStoreWrite, Cid)> {
    let did = did.into();
    let cid = sqlx::query_scalar!(
        r#"
        SELECT root FROM accounts
        WHERE did = ?
        "#,
        did
    )
    .fetch_one(db)
    .await
    .context("failed to query database")?;

    let cid = Cid::from_str(&cid).expect("cid conversion unexpectedly failed");

    let store = storage::open_store(&config, did)
        .await
        .context("failed to open store")?;

    Ok((store, cid))
}

async fn open_repo<'a, 'b>(
    config: &config::RepoConfig,
    db: &Db,
    did: impl Into<String>,
) -> anyhow::Result<Repository<impl AsyncBlockStoreRead + AsyncBlockStoreWrite>> {
    let did = did.into();
    let (store, root) = open_store(config, db, did.clone()).await?;
    let repo = Repository::open(store, root).await?;

    Ok(repo)
}

async fn get_blob(
    State(config): State<AppConfig>,
    Query(input): Query<sync::get_blob::Parameters>,
) -> Result<Bytes> {
    todo!()
}

async fn get_latest_commit(
    State(config): State<AppConfig>,
    State(db): State<Db>,
    Query(input): Query<sync::get_latest_commit::Parameters>,
) -> Result<Json<sync::get_latest_commit::Output>> {
    let repo = open_repo(&config.repo, &db, input.did.as_str())
        .await
        .context("failed to open repository")?;

    let cid = repo.root();
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
    let (mut store, root) = open_store(&config.repo, &db, input.did.as_str())
        .await
        .context("failed to open store")?;

    let mut repo = Repository::open(&mut store, root)
        .await
        .context("failed to open repo")?;

    let key = format!("{}/{}", input.collection.as_str(), input.rkey);

    let mut contents = Vec::new();
    let mut ret_store = CarStore::create_with_roots(std::io::Cursor::new(&mut contents), [root])
        .await
        .context("failed to create car store")?;

    repo.extract_raw_into(&key, &mut ret_store)
        .await
        .context("failed to extract records")?;

    Ok(Bytes::copy_from_slice(contents.as_slice()))
}

// HACK: `limit` may be passed as a string, so we must treat it as one.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ListReposParameters {
    #[serde(skip_serializing_if = "core::option::Option::is_none")]
    pub cursor: core::option::Option<String>,
    #[serde(skip_serializing_if = "core::option::Option::is_none")]
    pub limit: core::option::Option<String>,
}

async fn list_repos(
    State(db): State<Db>,
    Query(input): Query<ListReposParameters>,
) -> Result<Json<sync::list_repos::Output>> {
    struct Record {
        did: String,
        root: String,
        rev: String,
    }

    let r = if let Some(cursor) = &input.cursor {
        let r = sqlx::query_as!(
            Record,
            r#"SELECT did, root, rev FROM accounts WHERE did > ? LIMIT 1000"#,
            cursor
        )
        .fetch(&db);

        r.try_collect::<Vec<_>>()
            .await
            .context("failed to fetch profiles")?
    } else {
        let r =
            sqlx::query_as!(Record, r#"SELECT did, root, rev FROM accounts LIMIT 1000"#).fetch(&db);

        r.try_collect::<Vec<_>>()
            .await
            .context("failed to fetch profiles")?
    };

    let cursor = r.last().map(|r| r.did.clone());
    let repos = r
        .into_iter()
        .map(|r| {
            sync::list_repos::RepoData {
                active: Some(true),
                did: Did::new(r.did).unwrap(),
                head: atrium_api::types::string::Cid::new(Cid::from_str(&r.root).unwrap()),
                rev: r.rev,
                status: None,
            }
            .into()
        })
        .collect::<Vec<_>>();

    Ok(Json(sync::list_repos::OutputData { cursor, repos }.into()))
}

async fn subscribe_repos(
    ws: WebSocketUpgrade,
    State(fh): State<FirehoseProducer>,
) -> impl IntoResponse {
    ws.on_upgrade(|ws| async move {
        fh.client_connection(ws).await;
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
        .route(concat!("/", sync::list_repos::NSID), get(list_repos))
        .route(
            concat!("/", sync::subscribe_repos::NSID),
            get(subscribe_repos),
        )
}
