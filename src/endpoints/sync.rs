use std::str::FromStr;

use anyhow::{anyhow, Context};
use atrium_api::{
    com::atproto::sync,
    types::{string::Did, LimitedNonZeroU16},
};
use atrium_repo::{
    blockstore::{AsyncBlockStoreRead, AsyncBlockStoreWrite, CarStore, DAG_CBOR, SHA2_256},
    Cid,
};
use axum::{
    body::Body,
    extract::{Query, State, WebSocketUpgrade},
    http::{self, Response, StatusCode},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use constcat::concat;
use futures::stream::TryStreamExt;
use tokio_util::io::ReaderStream;

use crate::{
    config::AppConfig,
    firehose::FirehoseProducer,
    storage::{open_repo_db, open_store},
    AppState, Db, Error, Result,
};

async fn get_blob(
    State(config): State<AppConfig>,
    Query(input): Query<sync::get_blob::ParametersData>,
) -> Result<Response<Body>> {
    let blob = config
        .blob
        .path
        .join(format!("{}.blob", input.cid.as_ref().to_string()));

    let f = tokio::fs::File::open(blob)
        .await
        .context("blob not found")?;
    let len = f
        .metadata()
        .await
        .context("failed to query file metadata")?
        .len();

    let s = ReaderStream::new(f);

    Ok(Response::builder()
        .header(http::header::CONTENT_LENGTH, format!("{}", len))
        .body(Body::from_stream(s))
        .context("failed to construct response")?)
}

async fn get_blocks(
    State(config): State<AppConfig>,
    Query(input): Query<sync::get_blocks::ParametersData>,
) -> Result<Response<Body>> {
    let mut repo = open_store(&config.repo, input.did.as_str())
        .await
        .context("failed to open repository")?;

    let mut mem = Vec::new();
    let mut store = CarStore::create(std::io::Cursor::new(&mut mem))
        .await
        .expect("failed to create intermediate carstore");

    for cid in &input.cids {
        // SEC: This can potentially fetch stale blocks from a repository (e.g. those that were deleted).
        // We'll want to prevent accesses to stale blocks eventually just to respect a user's right to be forgotten.
        let _ = store
            .write_block(
                DAG_CBOR,
                SHA2_256,
                &repo
                    .read_block(*cid.as_ref())
                    .await
                    .context("failed to read block")?,
            )
            .await
            .context("failed to write block")?;
    }

    Ok(Response::builder()
        .header(http::header::CONTENT_TYPE, "application/vnd.ipld.car")
        .body(Body::from(mem))
        .context("failed to construct response")?)
}

async fn get_latest_commit(
    State(config): State<AppConfig>,
    State(db): State<Db>,
    Query(input): Query<sync::get_latest_commit::ParametersData>,
) -> Result<Json<sync::get_latest_commit::Output>> {
    let repo = open_repo_db(&config.repo, &db, input.did.as_str())
        .await
        .context("failed to open repository")?;

    let cid = repo.root();
    let commit = repo.commit();

    Ok(Json(
        sync::get_latest_commit::OutputData {
            cid: atrium_api::types::string::Cid::new(cid),
            rev: commit.rev(),
        }
        .into(),
    ))
}

async fn get_record(
    State(config): State<AppConfig>,
    State(db): State<Db>,
    Query(input): Query<sync::get_record::ParametersData>,
) -> Result<Response<Body>> {
    let mut repo = open_repo_db(&config.repo, &db, input.did.as_str())
        .await
        .context("failed to open repo")?;

    let key = format!("{}/{}", input.collection.as_str(), input.rkey.as_str());

    let mut contents = Vec::new();
    let mut ret_store =
        CarStore::create_with_roots(std::io::Cursor::new(&mut contents), [repo.root()])
            .await
            .context("failed to create car store")?;

    repo.extract_raw_into(&key, &mut ret_store)
        .await
        .context("failed to extract records")?;

    Ok(Response::builder()
        .header(http::header::CONTENT_TYPE, "application/vnd.ipld.car")
        .body(Body::from(contents))
        .context("failed to construct response")?)
}

async fn get_repo_status(
    State(db): State<Db>,
    Query(input): Query<sync::get_repo::ParametersData>,
) -> Result<Json<sync::get_repo_status::Output>> {
    let did = input.did.as_str();
    let r = sqlx::query!(r#"SELECT rev, status FROM accounts WHERE did = ?"#, did)
        .fetch_optional(&db)
        .await
        .context("failed to execute query")?;

    let r = if let Some(r) = r {
        r
    } else {
        return Err(Error::with_status(
            StatusCode::NOT_FOUND,
            anyhow!("account not found"),
        ))?;
    };

    let active = r.status == "active";
    let status = if active { None } else { Some(r.status) };

    Ok(Json(
        sync::get_repo_status::OutputData {
            active,
            status,
            did: input.did.clone(),
            rev: Some(atrium_api::types::string::Tid::new(r.rev).unwrap()),
        }
        .into(),
    ))
}

async fn get_repo(
    State(config): State<AppConfig>,
    State(db): State<Db>,
    Query(input): Query<sync::get_repo::ParametersData>,
) -> Result<Response<Body>> {
    let mut repo = open_repo_db(&config.repo, &db, input.did.as_str())
        .await
        .context("failed to open repo")?;

    let mut contents = Vec::new();
    let mut store = CarStore::create_with_roots(std::io::Cursor::new(&mut contents), [repo.root()])
        .await
        .context("failed to create car store")?;

    repo.export_into(&mut store)
        .await
        .context("failed to extract records")?;

    Ok(Response::builder()
        .header(http::header::CONTENT_TYPE, "application/vnd.ipld.car")
        .body(Body::from(contents))
        .context("failed to construct response")?)
}

async fn list_blobs(
    State(db): State<Db>,
    Query(input): Query<sync::list_blobs::ParametersData>,
) -> Result<Json<sync::list_blobs::Output>> {
    let did_str = input.did.as_str();

    // TODO: `input.since`
    // TODO: `input.limit`
    // TODO: `input.cursor`

    let cids = sqlx::query_scalar!(r#"SELECT cid FROM blob_ref WHERE did = ?"#, did_str)
        .fetch_all(&db)
        .await
        .context("failed to query blobs")?;

    let cids = cids
        .into_iter()
        .map(|c| {
            atrium_repo::Cid::from_str(&c)
                .map(|s| atrium_api::types::string::Cid::new(s))
                .map_err(anyhow::Error::new)
        })
        .collect::<anyhow::Result<Vec<_>>>()
        .context("failed to convert cids")?;

    Ok(Json(
        sync::list_blobs::OutputData { cursor: None, cids }.into(),
    ))
}

async fn list_repos(
    State(db): State<Db>,
    Query(input): Query<sync::list_repos::ParametersData>,
) -> Result<Json<sync::list_repos::Output>> {
    struct Record {
        did: String,
        root: String,
        rev: String,
    }

    let limit: u16 = input.limit.unwrap_or(LimitedNonZeroU16::MAX).into();

    let r = if let Some(cursor) = &input.cursor {
        let r = sqlx::query_as!(
            Record,
            r#"SELECT did, root, rev FROM accounts WHERE did > ? LIMIT ?"#,
            cursor,
            limit
        )
        .fetch(&db);

        r.try_collect::<Vec<_>>()
            .await
            .context("failed to fetch profiles")?
    } else {
        let r = sqlx::query_as!(
            Record,
            r#"SELECT did, root, rev FROM accounts LIMIT ?"#,
            limit
        )
        .fetch(&db);

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
                rev: atrium_api::types::string::Tid::new(r.rev).unwrap(),
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
    Query(input): Query<sync::subscribe_repos::ParametersData>,
) -> impl IntoResponse {
    ws.on_upgrade(move |ws| async move {
        fh.client_connection(ws, input.cursor).await;
    })
}

#[rustfmt::skip]
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
        .route(concat!("/", sync::get_blob::NSID),          get(get_blob))
        .route(concat!("/", sync::get_blocks::NSID),        get(get_blocks))
        .route(concat!("/", sync::get_latest_commit::NSID), get(get_latest_commit))
        .route(concat!("/", sync::get_record::NSID),        get(get_record))
        .route(concat!("/", sync::get_repo_status::NSID),   get(get_repo_status))
        .route(concat!("/", sync::get_repo::NSID),          get(get_repo))
        .route(concat!("/", sync::list_blobs::NSID),        get(list_blobs))
        .route(concat!("/", sync::list_repos::NSID),        get(list_repos))
        .route(concat!("/", sync::subscribe_repos::NSID),   get(subscribe_repos))
}
