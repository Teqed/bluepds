//! PDS repository endpoints /xrpc/com.atproto.repo.*)
mod apply_writes;
pub(crate) use apply_writes::apply_writes;

use std::{collections::HashSet, str::FromStr};

use anyhow::{Context as _, anyhow};
use atrium_api::com::atproto::repo::apply_writes::{
    self as atrium_apply_writes, InputWritesItem, OutputResultsItem,
};
use atrium_api::{
    com::atproto::repo::{self, defs::CommitMetaData},
    types::{
        LimitedU32, Object, TryFromUnknown as _, TryIntoUnknown as _, Unknown,
        string::{AtIdentifier, Nsid, Tid},
    },
};
use atrium_repo::{Cid, blockstore::CarStore};
use axum::{
    Json, Router,
    body::Body,
    extract::{Query, Request, State},
    http::{self, StatusCode},
    routing::{get, post},
};
use constcat::concat;
use futures::TryStreamExt as _;
use metrics::counter;
use rsky_syntax::aturi::AtUri;
use serde::Deserialize;
use tokio::io::AsyncWriteExt as _;

use crate::repo::block_map::cid_for_cbor;
use crate::repo::types::PreparedCreateOrUpdate;
use crate::{
    AppState, Db, Error, Result, SigningKey,
    actor_store::{ActorStoreTransactor, ActorStoreWriter},
    auth::AuthenticatedUser,
    config::AppConfig,
    error::ErrorMessage,
    firehose::{self, FirehoseProducer, RepoOp},
    metrics::{REPO_COMMITS, REPO_OP_CREATE, REPO_OP_DELETE, REPO_OP_UPDATE},
    repo::types::{PreparedWrite, WriteOpAction},
    storage,
};

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
/// Parameters for [`list_records`].
pub(super) struct ListRecordsParameters {
    ///The NSID of the record type.
    pub collection: Nsid,
    /// The cursor to start from.
    #[serde(skip_serializing_if = "core::option::Option::is_none")]
    pub cursor: Option<String>,
    ///The number of records to return.
    #[serde(skip_serializing_if = "core::option::Option::is_none")]
    pub limit: Option<String>,
    ///The handle or DID of the repo.
    pub repo: AtIdentifier,
    ///Flag to reverse the order of the returned records.
    #[serde(skip_serializing_if = "core::option::Option::is_none")]
    pub reverse: Option<bool>,
    ///DEPRECATED: The highest sort-ordered rkey to stop at (exclusive)
    #[serde(skip_serializing_if = "core::option::Option::is_none")]
    pub rkey_end: Option<String>,
    ///DEPRECATED: The lowest sort-ordered rkey to start from (exclusive)
    #[serde(skip_serializing_if = "core::option::Option::is_none")]
    pub rkey_start: Option<String>,
}

/// Resolve DID to DID document. Does not bi-directionally verify handle.
/// - GET /xrpc/com.atproto.repo.resolveDid
/// ### Query Parameters
/// - `did`: DID to resolve.
/// ### Responses
/// - 200 OK: {`did_doc`: `did_doc`}
/// - 400 Bad Request: {error:[`InvalidRequest`, `ExpiredToken`, `InvalidToken`, `DidNotFound`, `DidDeactivated`]}
async fn resolve_did(
    db: &Db,
    identifier: &AtIdentifier,
) -> anyhow::Result<(
    atrium_api::types::string::Did,
    atrium_api::types::string::Handle,
)> {
    let (handle, did) = match *identifier {
        AtIdentifier::Handle(ref handle) => {
            let handle_as_str = &handle.as_str();
            (
                &handle.to_owned(),
                &atrium_api::types::string::Did::new(
                    sqlx::query_scalar!(
                        r#"SELECT did FROM handles WHERE handle = ?"#,
                        handle_as_str
                    )
                    .fetch_one(db)
                    .await
                    .context("failed to query did")?,
                )
                .expect("should be valid DID"),
            )
        }
        AtIdentifier::Did(ref did) => {
            let did_as_str = &did.as_str();
            (
                &atrium_api::types::string::Handle::new(
                    sqlx::query_scalar!(r#"SELECT handle FROM handles WHERE did = ?"#, did_as_str)
                        .fetch_one(db)
                        .await
                        .context("failed to query did")?,
                )
                .expect("should be valid handle"),
                &did.to_owned(),
            )
        }
    };

    Ok((did.to_owned(), handle.to_owned()))
}

/// Create a single new repository record. Requires auth, implemented by PDS.
/// - POST /xrpc/com.atproto.repo.createRecord
/// ### Request Body
/// - `repo`: `at-identifier` // The handle or DID of the repo (aka, current account).
/// - `collection`: `nsid` // The NSID of the record collection.
/// - `rkey`: `string` // The record key. <= 512 characters.
/// - `validate`: `boolean` // Can be set to 'false' to skip Lexicon schema validation of record data, 'true' to require it, or leave unset to validate only for known Lexicons.
/// - `record`
/// - `swap_commit`: `cid` // Compare and swap with the previous commit by CID.
/// ### Responses
/// - 200 OK: {`cid`: `cid`, `uri`: `at-uri`, `commit`: {`cid`: `cid`, `rev`: `tid`}, `validation_status`: [`valid`, `unknown`]}
/// - 400 Bad Request: {error:[`InvalidRequest`, `ExpiredToken`, `InvalidToken`, `InvalidSwap`]}
/// - 401 Unauthorized
async fn create_record(
    user: AuthenticatedUser,
    State(actor_store): State<ActorStore>,
    State(skey): State<SigningKey>,
    State(config): State<AppConfig>,
    State(db): State<Db>,
    State(fhp): State<FirehoseProducer>,
    Json(input): Json<repo::create_record::Input>,
) -> Result<Json<repo::create_record::Output>> {
    todo!();
    // let write_result = apply_writes::apply_writes(
    //     user,
    //     State(actor_store),
    //     State(skey),
    //     State(config),
    //     State(db),
    //     State(fhp),
    //     Json(
    //         repo::apply_writes::InputData {
    //             repo: input.repo.clone(),
    //             validate: input.validate,
    //             swap_commit: input.swap_commit.clone(),
    //             writes: vec![repo::apply_writes::InputWritesItem::Create(Box::new(
    //                 repo::apply_writes::CreateData {
    //                     collection: input.collection.clone(),
    //                     rkey: input.rkey.clone(),
    //                     value: input.record.clone(),
    //                 }
    //                 .into(),
    //             ))],
    //         }
    //         .into(),
    //     ),
    // )
    // .await
    // .context("failed to apply writes")?;

    // let create_result = if let repo::apply_writes::OutputResultsItem::CreateResult(create_result) =
    //     write_result
    //         .results
    //         .clone()
    //         .and_then(|result| result.first().cloned())
    //         .context("unexpected output from apply_writes")?
    // {
    //     Some(create_result)
    // } else {
    //     None
    // }
    // .context("unexpected result from apply_writes")?;

    // Ok(Json(
    //     repo::create_record::OutputData {
    //         cid: create_result.cid.clone(),
    //         commit: write_result.commit.clone(),
    //         uri: create_result.uri.clone(),
    //         validation_status: Some("unknown".to_owned()),
    //     }
    //     .into(),
    // ))
}

/// Write a repository record, creating or updating it as needed. Requires auth, implemented by PDS.
/// - POST /xrpc/com.atproto.repo.putRecord
/// ### Request Body
/// - `repo`: `at-identifier` // The handle or DID of the repo (aka, current account).
/// - `collection`: `nsid` // The NSID of the record collection.
/// - `rkey`: `string` // The record key. <= 512 characters.
/// - `validate`: `boolean` // Can be set to 'false' to skip Lexicon schema validation of record data, 'true' to require it, or leave unset to validate only for known Lexicons.
/// - `record`
/// - `swap_record`: `boolean` // Compare and swap with the previous record by CID. WARNING: nullable and optional field; may cause problems with golang implementation
/// - `swap_commit`: `cid` // Compare and swap with the previous commit by CID.
/// ### Responses
/// - 200 OK: {"uri": "string","cid": "string","commit": {"cid": "string","rev": "string"},"validationStatus": "valid | unknown"}
/// - 400 Bad Request: {error:"`InvalidRequest` | `ExpiredToken` | `InvalidToken` | `InvalidSwap`"}
/// - 401 Unauthorized
async fn put_record(
    user: AuthenticatedUser,
    State(actor_store): State<ActorStore>,
    State(skey): State<SigningKey>,
    State(config): State<AppConfig>,
    State(db): State<Db>,
    State(fhp): State<FirehoseProducer>,
    Json(input): Json<repo::put_record::Input>,
) -> Result<Json<repo::put_record::Output>> {
    todo!();
    // // TODO: `input.swap_record`
    // // FIXME: "put" implies that we will create the record if it does not exist.
    // // We currently only update existing records and/or throw an error if one doesn't exist.
    // let input = (*input).clone();
    // let input = repo::apply_writes::InputData {
    //     repo: input.repo,
    //     validate: input.validate,
    //     swap_commit: input.swap_commit,
    //     writes: vec![repo::apply_writes::InputWritesItem::Update(Box::new(
    //         repo::apply_writes::UpdateData {
    //             collection: input.collection,
    //             rkey: input.rkey,
    //             value: input.record,
    //         }
    //         .into(),
    //     ))],
    // }
    // .into();

    // let write_result = apply_writes::apply_writes(
    //     user,
    //     State(actor_store),
    //     State(skey),
    //     State(config),
    //     State(db),
    //     State(fhp),
    //     Json(input),
    // )
    // .await
    // .context("failed to apply writes")?;

    // let update_result = write_result
    //     .results
    //     .clone()
    //     .and_then(|result| result.first().cloned())
    //     .context("unexpected output from apply_writes")?;
    // let (cid, uri) = match update_result {
    //     repo::apply_writes::OutputResultsItem::CreateResult(create_result) => (
    //         Some(create_result.cid.clone()),
    //         Some(create_result.uri.clone()),
    //     ),
    //     repo::apply_writes::OutputResultsItem::UpdateResult(update_result) => (
    //         Some(update_result.cid.clone()),
    //         Some(update_result.uri.clone()),
    //     ),
    //     repo::apply_writes::OutputResultsItem::DeleteResult(_) => (None, None),
    // };
    // Ok(Json(
    //     repo::put_record::OutputData {
    //         cid: cid.context("missing cid")?,
    //         commit: write_result.commit.clone(),
    //         uri: uri.context("missing uri")?,
    //         validation_status: Some("unknown".to_owned()),
    //     }
    //     .into(),
    // ))
}

/// Delete a repository record, or ensure it doesn't exist. Requires auth, implemented by PDS.
/// - POST /xrpc/com.atproto.repo.deleteRecord
/// ### Request Body
/// - `repo`: `at-identifier` // The handle or DID of the repo (aka, current account).
/// - `collection`: `nsid` // The NSID of the record collection.
/// - `rkey`: `string` // The record key. <= 512 characters.
/// - `swap_record`: `boolean` // Compare and swap with the previous record by CID.
/// - `swap_commit`: `cid` // Compare and swap with the previous commit by CID.
/// ### Responses
/// - 200 OK: {"commit": {"cid": "string","rev": "string"}}
/// - 400 Bad Request: {error:[`InvalidRequest`, `ExpiredToken`, `InvalidToken`, `InvalidSwap`]}
/// - 401 Unauthorized
async fn delete_record(
    user: AuthenticatedUser,
    State(actor_store): State<ActorStore>,
    State(skey): State<SigningKey>,
    State(config): State<AppConfig>,
    State(db): State<Db>,
    State(fhp): State<FirehoseProducer>,
    Json(input): Json<repo::delete_record::Input>,
) -> Result<Json<repo::delete_record::Output>> {
    todo!();
    // // TODO: `input.swap_record`

    // Ok(Json(
    //     repo::delete_record::OutputData {
    //         commit: apply_writes::apply_writes(
    //             user,
    //             State(actor_store),
    //             State(skey),
    //             State(config),
    //             State(db),
    //             State(fhp),
    //             Json(
    //                 repo::apply_writes::InputData {
    //                     repo: input.repo.clone(),
    //                     swap_commit: input.swap_commit.clone(),
    //                     validate: None,
    //                     writes: vec![repo::apply_writes::InputWritesItem::Delete(Box::new(
    //                         repo::apply_writes::DeleteData {
    //                             collection: input.collection.clone(),
    //                             rkey: input.rkey.clone(),
    //                         }
    //                         .into(),
    //                     ))],
    //                 }
    //                 .into(),
    //             ),
    //         )
    //         .await
    //         .context("failed to apply writes")?
    //         .commit
    //         .clone(),
    //     }
    //     .into(),
    // ))
}

/// Get information about an account and repository, including the list of collections. Does not require auth.
/// - GET /xrpc/com.atproto.repo.describeRepo
/// ### Query Parameters
/// - `repo`: `at-identifier` // The handle or DID of the repo.
/// ### Responses
/// - 200 OK: {"handle": "string","did": "string","didDoc": {},"collections": [string],"handleIsCorrect": true} \
///   handeIsCorrect - boolean - Indicates if handle is currently valid (resolves bi-directionally)
/// - 400 Bad Request: {error:[`InvalidRequest`, `ExpiredToken`, `InvalidToken`]}
/// - 401 Unauthorized
async fn describe_repo(
    State(actor_store): State<ActorStore>,
    State(config): State<AppConfig>,
    State(db): State<Db>,
    Query(input): Query<repo::describe_repo::ParametersData>,
) -> Result<Json<repo::describe_repo::Output>> {
    // Lookup the DID by the provided handle.
    let (did, handle) = resolve_did(&db, &input.repo)
        .await
        .context("failed to resolve handle")?;

    // Use Actor Store to get the collections
    todo!();
}

/// Get a single record from a repository. Does not require auth.
/// - GET /xrpc/com.atproto.repo.getRecord
/// ### Query Parameters
/// - `repo`: `at-identifier` // The handle or DID of the repo.
/// - `collection`: `nsid` // The NSID of the record collection.
/// - `rkey`: `string` // The record key. <= 512 characters.
/// - `cid`: `cid` // The CID of the version of the record. If not specified, then return the most recent version.
/// ### Responses
/// - 200 OK: {"uri": "string","cid": "string","value": {}}
/// - 400 Bad Request: {error:[`InvalidRequest`, `ExpiredToken`, `InvalidToken`, `RecordNotFound`]}
/// - 401 Unauthorized
async fn get_record(
    State(actor_store): State<ActorStore>,
    State(config): State<AppConfig>,
    State(db): State<Db>,
    Query(input): Query<repo::get_record::ParametersData>,
) -> Result<Json<repo::get_record::Output>> {
    if input.cid.is_some() {
        return Err(Error::unimplemented(anyhow!(
            "looking up old records is unsupported"
        )));
    }

    // Lookup the DID by the provided handle.
    let (did, _handle) = resolve_did(&db, &input.repo)
        .await
        .context("failed to resolve handle")?;

    // Create a URI from the parameters
    let uri = format!(
        "at://{}/{}/{}",
        did.as_str(),
        input.collection.as_str(),
        input.rkey.as_str()
    );

    // Use Actor Store to get the record
    todo!();
}

/// List a range of records in a repository, matching a specific collection. Does not require auth.
/// - GET /xrpc/com.atproto.repo.listRecords
/// ### Query Parameters
/// - `repo`: `at-identifier` // The handle or DID of the repo.
/// - `collection`: `nsid` // The NSID of the record type.
/// - `limit`: `integer` // The maximum number of records to return. Default 50, >=1 and <=100.
/// - `cursor`: `string`
/// - `reverse`: `boolean` // Flag to reverse the order of the returned records.
/// ### Responses
/// - 200 OK: {"cursor": "string","records": [{"uri": "string","cid": "string","value": {}}]}
/// - 400 Bad Request: {error:[`InvalidRequest`, `ExpiredToken`, `InvalidToken`]}
/// - 401 Unauthorized
async fn list_records(
    State(actor_store): State<ActorStore>,
    State(config): State<AppConfig>,
    State(db): State<Db>,
    Query(input): Query<Object<ListRecordsParameters>>,
) -> Result<Json<repo::list_records::Output>> {
    // Lookup the DID by the provided handle.
    let (did, _handle) = resolve_did(&db, &input.repo)
        .await
        .context("failed to resolve handle")?;

    // Use Actor Store to list records for the collection
    todo!();
}

/// Upload a new blob, to be referenced from a repository record. \
/// The blob will be deleted if it is not referenced within a time window (eg, minutes). \
/// Blob restrictions (mimetype, size, etc) are enforced when the reference is created. \
/// Requires auth, implemented by PDS.
/// - POST /xrpc/com.atproto.repo.uploadBlob
/// ### Request Body
/// ### Responses
/// - 200 OK: {"blob": "binary"}
/// - 400 Bad Request: {error:[`InvalidRequest`, `ExpiredToken`, `InvalidToken`]}
/// - 401 Unauthorized
async fn upload_blob(
    user: AuthenticatedUser,
    State(actor_store): State<ActorStore>,
    State(config): State<AppConfig>,
    State(db): State<Db>,
    request: Request<Body>,
) -> Result<Json<repo::upload_blob::Output>> {
    let length = request
        .headers()
        .get(http::header::CONTENT_LENGTH)
        .context("no content length provided")?
        .to_str()
        .map_err(anyhow::Error::from)
        .and_then(|content_length| content_length.parse::<u64>().map_err(anyhow::Error::from))
        .context("invalid content-length header")?;
    let mime = request
        .headers()
        .get(http::header::CONTENT_TYPE)
        .context("no content-type provided")?
        .to_str()
        .context("invalid content-type provided")?
        .to_owned();

    if length > config.blob.limit {
        return Err(Error::with_status(
            StatusCode::PAYLOAD_TOO_LARGE,
            anyhow!("size {} above limit {}", length, config.blob.limit),
        ));
    }

    // Read the blob data
    let mut body_data = Vec::new();
    let mut stream = request.into_body().into_data_stream();
    while let Some(bytes) = stream.try_next().await.context("failed to receive file")? {
        body_data.extend_from_slice(&bytes);

        // Check size limit incrementally
        if body_data.len() as u64 > config.blob.limit {
            return Err(Error::with_status(
                StatusCode::PAYLOAD_TOO_LARGE,
                anyhow!("size above limit and content-length header was wrong"),
            ));
        }
    }

    // Use Actor Store to upload the blob
    todo!();
}

async fn todo() -> Result<()> {
    Err(Error::unimplemented(anyhow!("not implemented")))
}

/// These endpoints are part of the atproto PDS repository management APIs. \
/// Requests usually require authentication (unlike the com.atproto.sync.* endpoints), and are made directly to the user's own PDS instance.
/// ### Routes
/// - AP /xrpc/com.atproto.repo.applyWrites     -> [`apply_writes`]
/// - AP /xrpc/com.atproto.repo.createRecord    -> [`create_record`]
/// - AP /xrpc/com.atproto.repo.putRecord       -> [`put_record`]
/// - AP /xrpc/com.atproto.repo.deleteRecord    -> [`delete_record`]
/// - AP /xrpc/com.atproto.repo.uploadBlob      -> [`upload_blob`]
/// - UG /xrpc/com.atproto.repo.describeRepo    -> [`describe_repo`]
/// - UG /xrpc/com.atproto.repo.getRecord       -> [`get_record`]
/// - UG /xrpc/com.atproto.repo.listRecords     -> [`list_records`]
///     - [ ] xx /xrpc/com.atproto.repo.importRepo
// - [ ] xx /xrpc/com.atproto.repo.listMissingBlobs
pub(super) fn routes() -> Router<AppState> {
    Router::new()
        .route(concat!("/", repo::apply_writes::NSID), post(apply_writes))
        // .route(concat!("/", repo::create_record::NSID), post(create_record))
        // .route(concat!("/", repo::put_record::NSID), post(put_record))
        // .route(concat!("/", repo::delete_record::NSID), post(delete_record))
        // .route(concat!("/", repo::upload_blob::NSID), post(upload_blob))
        // .route(concat!("/", repo::describe_repo::NSID), get(describe_repo))
        // .route(concat!("/", repo::get_record::NSID), get(get_record))
        .route(concat!("/", repo::import_repo::NSID), post(todo))
        .route(concat!("/", repo::list_missing_blobs::NSID), get(todo))
    // .route(concat!("/", repo::list_records::NSID), get(list_records))
}
