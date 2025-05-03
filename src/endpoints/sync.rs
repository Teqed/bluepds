//! Endpoints for the `ATProto` sync API. (/xrpc/com.atproto.sync.*)
use std::str::FromStr as _;

use anyhow::{Context as _, anyhow};
use atrium_api::{
    com::atproto::sync,
    types::{LimitedNonZeroU16, string::Did},
};
use atrium_repo::{
    Cid,
    blockstore::{
        AsyncBlockStoreRead as _, AsyncBlockStoreWrite as _, CarStore, DAG_CBOR, SHA2_256,
    },
};
use axum::{
    Json, Router,
    body::Body,
    extract::{Query, State, WebSocketUpgrade},
    http::{self, Response, StatusCode},
    response::IntoResponse,
    routing::get,
};
use constcat::concat;
use futures::stream::TryStreamExt as _;
use tokio_util::io::ReaderStream;

use crate::{
    AppState, Db, Error, Result,
    config::AppConfig,
    firehose::FirehoseProducer,
    storage::{open_repo_db, open_store},
};

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
/// Parameters for `/xrpc/com.atproto.sync.listBlobs` \
/// HACK: `limit` may be passed as a string, so we must treat it as one.
pub(super) struct ListBlobsParameters {
    #[serde(skip_serializing_if = "core::option::Option::is_none")]
    /// Optional cursor to paginate through blobs.
    pub cursor: Option<String>,
    ///The DID of the repo.
    pub did: Did,
    #[serde(skip_serializing_if = "core::option::Option::is_none")]
    /// Optional limit of blobs to return.
    pub limit: Option<String>,
    ///Optional revision of the repo to list blobs since.
    #[serde(skip_serializing_if = "core::option::Option::is_none")]
    pub since: Option<String>,
}
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
/// Parameters for `/xrpc/com.atproto.sync.listRepos` \
/// HACK: `limit` may be passed as a string, so we must treat it as one.
pub(super) struct ListReposParameters {
    #[serde(skip_serializing_if = "core::option::Option::is_none")]
    /// Optional cursor to paginate through repos.
    pub cursor: Option<String>,
    #[serde(skip_serializing_if = "core::option::Option::is_none")]
    /// Optional limit of repos to return.
    pub limit: Option<String>,
}
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
/// Parameters for `/xrpc/com.atproto.sync.subscribeRepos` \
/// HACK: `cursor` may be passed as a string, so we must treat it as one.
pub(super) struct SubscribeReposParametersData {
    ///The last known event seq number to backfill from.
    #[serde(skip_serializing_if = "core::option::Option::is_none")]
    pub cursor: Option<String>,
}

async fn get_blob(
    State(config): State<AppConfig>,
    Query(input): Query<sync::get_blob::ParametersData>,
) -> Result<Response<Body>> {
    let blob = config
        .blob
        .path
        .join(format!("{}.blob", input.cid.as_ref()));

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
        .header(http::header::CONTENT_LENGTH, format!("{len}"))
        .body(Body::from_stream(s))
        .context("failed to construct response")?)
}

/// Enumerates which accounts the requesting account is currently blocking. Requires auth.
/// - GET /xrpc/com.atproto.sync.getBlocks
/// ### Query Parameters
/// - `limit`: integer, optional, default: 50, >=1 and <=100
/// - `cursor`: string, optional
/// ### Responses
/// - 200 OK: ...
/// - 400 Bad Request: {error:[`InvalidRequest`, `ExpiredToken`, `InvalidToken`]}
/// - 401 Unauthorized
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
        .context("failed to create intermediate carstore")?;

    for cid in &input.cids {
        // SEC: This can potentially fetch stale blocks from a repository (e.g. those that were deleted).
        // We'll want to prevent accesses to stale blocks eventually just to respect a user's right to be forgotten.
        _ = store
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

/// Get the current commit CID & revision of the specified repo. Does not require auth.
/// ### Query Parameters
/// - `did`: The DID of the repo.
/// ### Responses
/// - 200 OK: {"cid": "string","rev": "string"}
/// - 400 Bad Request: {error:[`InvalidRequest`, `ExpiredToken`, `InvalidToken`, `RepoTakendown`, `RepoSuspended`, `RepoDeactivated`]}
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

/// Get data blocks needed to prove the existence or non-existence of record in the current version of repo. Does not require auth.
/// ### Query Parameters
/// - `did`: The DID of the repo.
/// - `collection`: nsid
/// - `rkey`: record-key
/// ### Responses
/// - 200 OK: ...
/// - 400 Bad Request: {error:[`InvalidRequest`, `ExpiredToken`, `InvalidToken`, `RecordNotFound`, `RepoNotFound`, `RepoTakendown`,
///   `RepoSuspended`, `RepoDeactivated`]}
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

/// Get the hosting status for a repository, on this server. Expected to be implemented by PDS and Relay.
/// ### Query Parameters
/// - `did`: The DID of the repo.
/// ### Responses
/// - 200 OK: {"did": "string","active": true,"status": "takendown","rev": "string"}
/// - 400 Bad Request: {error:[`InvalidRequest`, `ExpiredToken`, `InvalidToken`, `RepoNotFound`]}
async fn get_repo_status(
    State(db): State<Db>,
    Query(input): Query<sync::get_repo::ParametersData>,
) -> Result<Json<sync::get_repo_status::Output>> {
    let did = input.did.as_str();
    let r = sqlx::query!(r#"SELECT rev, status FROM accounts WHERE did = ?"#, did)
        .fetch_optional(&db)
        .await
        .context("failed to execute query")?;

    let Some(r) = r else {
        return Err(Error::with_status(
            StatusCode::NOT_FOUND,
            anyhow!("account not found"),
        ));
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

/// Download a repository export as CAR file. Optionally only a 'diff' since a previous revision.
/// Does not require auth; implemented by PDS.
/// ### Query Parameters
/// - `did`: The DID of the repo.
/// - `since`: The revision ('rev') of the repo to create a diff from.
/// ### Responses
/// - 200 OK: ...
/// - 400 Bad Request: {error:[`InvalidRequest`, `ExpiredToken`, `InvalidToken`, `RepoNotFound`,
///   `RepoTakendown`, `RepoSuspended`, `RepoDeactivated`]}
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

/// List blob CIDs for an account, since some repo revision. Does not require auth; implemented by PDS.
/// ### Query Parameters
/// - `did`: The DID of the repo. Required.
/// - `since`: Optional revision of the repo to list blobs since.
/// - `limit`: >= 1 and <= 1000, default 500
/// - `cursor`: string
/// ### Responses
/// - 200 OK: {"cursor": "string","cids": [string]}
/// - 400 Bad Request: {error:[`InvalidRequest`, `ExpiredToken`, `InvalidToken`, `RepoNotFound`, `RepoTakendown`,
///   `RepoSuspended`, `RepoDeactivated`]}
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
            Cid::from_str(&c)
                .map(atrium_api::types::string::Cid::new)
                .map_err(anyhow::Error::new)
        })
        .collect::<anyhow::Result<Vec<_>>>()
        .context("failed to convert cids")?;

    Ok(Json(
        sync::list_blobs::OutputData { cursor: None, cids }.into(),
    ))
}

/// Enumerates all the DID, rev, and commit CID for all repos hosted by this service.
/// Does not require auth; implemented by PDS and Relay.
/// ### Query Parameters
/// - `limit`: >= 1 and <= 1000, default 500
/// - `cursor`: string
/// ### Responses
/// - 200 OK: {"cursor": "string","repos": [{"did": "string","head": "string","rev": "string","active": true,"status": "takendown"}]}
/// - 400 Bad Request: {error:[`InvalidRequest`, `ExpiredToken`, `InvalidToken`]}
async fn list_repos(
    State(db): State<Db>,
    Query(input): Query<sync::list_repos::ParametersData>,
) -> Result<Json<sync::list_repos::Output>> {
    struct Record {
        /// The DID of the repo.
        did: String,
        /// The commit CID of the repo.
        rev: String,
        /// The root CID of the repo.
        root: String,
    }

    let limit: u16 = input.limit.unwrap_or(LimitedNonZeroU16::MAX).into();

    let r = if let Some(ref cursor) = input.cursor {
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
                did: Did::new(r.did).expect("should be a valid DID"),
                head: atrium_api::types::string::Cid::new(
                    Cid::from_str(&r.root).expect("should be a valid CID"),
                ),
                rev: atrium_api::types::string::Tid::new(r.rev).unwrap(),
                status: None,
            }
            .into()
        })
        .collect::<Vec<_>>();

    Ok(Json(sync::list_repos::OutputData { cursor, repos }.into()))
}

/// Repository event stream, aka Firehose endpoint. Outputs repo commits with diff data, and identity update events,
///  for all repositories on the current server. See the atproto specifications for details around stream sequencing,
///  repo versioning, CAR diff format, and more. Public and does not require auth; implemented by PDS and Relay.
/// ### Query Parameters
/// - `cursor`: The last known event seq number to backfill from.
/// ### Responses
/// - 200 OK: ...
async fn subscribe_repos(
    ws_up: WebSocketUpgrade,
    State(fh): State<FirehoseProducer>,
    Query(input): Query<sync::subscribe_repos::ParametersData>,
) -> impl IntoResponse {
    ws_up.on_upgrade(async move |ws| {
        fh.client_connection(ws, input.cursor).await;
    })
}

#[rustfmt::skip]
/// These endpoints are part of the atproto repository synchronization APIs. Requests usually do not require authentication,
///  and can be made to PDS intances or Relay instances.
/// ### Routes
/// - `GET /xrpc/com.atproto.sync.getBlob` -> [`get_blob`]
/// - `GET /xrpc/com.atproto.sync.getBlocks` -> [`get_blocks`]
/// - `GET /xrpc/com.atproto.sync.getLatestCommit` -> [`get_latest_commit`]
/// - `GET /xrpc/com.atproto.sync.getRecord` -> [`get_record`]
/// - `GET /xrpc/com.atproto.sync.getRepoStatus` -> [`get_repo_status`]
/// - `GET /xrpc/com.atproto.sync.getRepo` -> [`get_repo`]
/// - `GET /xrpc/com.atproto.sync.listBlobs` -> [`list_blobs`]
/// - `GET /xrpc/com.atproto.sync.listRepos` -> [`list_repos`]
/// - `GET /xrpc/com.atproto.sync.subscribeRepos` -> [`subscribe_repos`]
pub(super) fn routes() -> Router<AppState> {
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
