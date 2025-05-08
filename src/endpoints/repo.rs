//! PDS repository endpoints /xrpc/com.atproto.repo.*)
use std::{collections::HashSet, str::FromStr as _, sync::Arc};

use anyhow::{Context as _, anyhow};
use atrium_api::{
    com::atproto::repo::{self, defs::CommitMetaData},
    types::{
        LimitedU32, Object, TryFromUnknown as _, TryIntoUnknown as _, Unknown,
        string::{AtIdentifier, Nsid, Tid},
    },
};
use atrium_repo::Cid;
use axum::{
    Json, Router,
    body::Body,
    extract::{Query, State, multipart::Field},
    http::{self, StatusCode},
    routing::{get, post},
};
use constcat::concat;
use futures::TryStreamExt as _;
use k256::Secp256k1;
use metrics::counter;
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncWriteExt as _;

use crate::{
    AppState, Db, Error, Result,
    actor_store::{ActorStore, ActorStoreTransactor},
    auth::AuthenticatedUser,
    config::AppConfig,
    error::ErrorMessage,
    firehose::{self, FirehoseProducer, RepoOp},
    metrics::{REPO_COMMITS, REPO_OP_CREATE, REPO_OP_DELETE, REPO_OP_UPDATE},
};

/// Parameters for [`list_records`].
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(super) struct ListRecordsParameters {
    pub collection: Nsid,
    pub cursor: Option<String>,
    pub limit: Option<String>,
    pub repo: AtIdentifier,
    pub reverse: Option<bool>,
    pub rkey_end: Option<String>,
    pub rkey_start: Option<String>,
}

/// Resolves DID from an identifier
async fn resolve_did(
    db: &Db,
    identifier: &AtIdentifier,
) -> anyhow::Result<(
    atrium_api::types::string::Did,
    atrium_api::types::string::Handle,
)> {
    let (handle, did) = match *identifier {
        AtIdentifier::Handle(ref handle) => {
            let handle_str = handle.as_str();
            let did_str =
                sqlx::query_scalar!(r#"SELECT did FROM handles WHERE handle = ?"#, handle_str)
                    .fetch_one(db)
                    .await
                    .context("failed to query did")?;

            (
                handle.clone(),
                atrium_api::types::string::Did::new(did_str).expect("should be valid DID format"),
            )
        }
        AtIdentifier::Did(ref did) => {
            let did_str = did.as_str();
            let handle_str =
                sqlx::query_scalar!(r#"SELECT handle FROM handles WHERE did = ?"#, did_str)
                    .fetch_one(db)
                    .await
                    .context("failed to query handle")?;

            (
                atrium_api::types::string::Handle::new(handle_str).expect("should be valid handle"),
                did.clone(),
            )
        }
    };

    Ok((did, handle))
}

/// Extract blobs from a record
fn extract_blobs(record: &Unknown) -> anyhow::Result<Vec<Cid>> {
    let val = serde_json::Value::try_from_unknown(record.clone())?;
    let mut cids = Vec::new();
    let mut stack = vec![val];

    while let Some(value) = stack.pop() {
        match value {
            serde_json::Value::Object(map) => {
                if let (Some(typ), Some(blob_ref)) = (map.get("$type"), map.get("ref")) {
                    if typ == "blob" {
                        if let Some(link) = blob_ref.get("$link").and_then(|v| v.as_str()) {
                            cids.push(Cid::from_str(link)?);
                        }
                    }
                }
                stack.extend(map.values().cloned());
            }
            serde_json::Value::Array(arr) => {
                stack.extend(arr);
            }
            _ => {}
        }
    }

    Ok(cids)
}

/// Apply writes to a repository
async fn apply_writes(
    user: AuthenticatedUser,
    State(config): State<AppConfig>,
    State(db): State<Db>,
    State(fhp): State<FirehoseProducer>,
    Json(input): Json<repo::apply_writes::Input>,
) -> Result<Json<repo::apply_writes::Output>> {
    use atrium_api::com::atproto::repo::apply_writes::{self, InputWritesItem, OutputResultsItem};

    // Resolve target DID
    let (target_did, _) = resolve_did(&db, &input.repo).await?;
    if target_did.as_str() != user.did() {
        return Err(Error::with_status(
            StatusCode::BAD_REQUEST,
            anyhow!("repo did not match the authenticated user"),
        ));
    }

    let actor_store = ActorStore::new(Arc::new(config));

    // Convert input writes to internal format
    let result = actor_store
        .transact(user.did(), |txn| {
            let mut results = Vec::new();
            let mut blob_info = Vec::new();
            let mut ops = Vec::new();

            for write in &input.writes {
                match write {
                    InputWritesItem::Create(create) => {
                        let collection = create.collection.as_str();
                        let rkey = create
                            .rkey
                            .as_deref()
                            .unwrap_or_else(|| Tid::now(LimitedU32::MIN).as_str());
                        let uri = format!("at://{}/{}/{}", user.did(), collection, rkey);

                        // Process blobs in record
                        for blob_cid in extract_blobs(&create.value)? {
                            blob_info.push((blob_cid, uri.clone()));
                        }

                        // Create the record
                        let cid = txn.repo().create_record(collection, rkey, &create.value)?;

                        ops.push(RepoOp::Create {
                            cid,
                            path: format!("{}/{}", collection, rkey),
                        });

                        results.push(OutputResultsItem::CreateResult(Box::new(
                            apply_writes::CreateResultData {
                                cid: atrium_api::types::string::Cid::new(cid),
                                uri,
                                validation_status: None,
                            }
                            .into(),
                        )));
                    }
                    InputWritesItem::Update(update) => {
                        let collection = update.collection.as_str();
                        let rkey = update.rkey.as_str();
                        let uri = format!("at://{}/{}/{}", user.did(), collection, rkey);
                        let record_path = format!("{}/{}", collection, rkey);

                        // Process blobs in record
                        for blob_cid in extract_blobs(&update.value)? {
                            blob_info.push((blob_cid, uri.clone()));
                        }

                        // Get current record
                        let record = txn.record().get_record(&record_path)?;

                        if let Some(existing) = record {
                            // Update existing record
                            let prev = Cid::from_str(&existing.cid)?;
                            let cid = txn.repo().update_record(collection, rkey, &update.value)?;

                            ops.push(RepoOp::Update {
                                cid,
                                path: record_path,
                                prev,
                            });

                            results.push(OutputResultsItem::UpdateResult(Box::new(
                                apply_writes::UpdateResultData {
                                    cid: atrium_api::types::string::Cid::new(cid),
                                    uri,
                                    validation_status: None,
                                }
                                .into(),
                            )));
                        } else {
                            // Create record if doesn't exist
                            let cid = txn.repo().create_record(collection, rkey, &update.value)?;

                            ops.push(RepoOp::Create {
                                cid,
                                path: record_path,
                            });

                            results.push(OutputResultsItem::CreateResult(Box::new(
                                apply_writes::CreateResultData {
                                    cid: atrium_api::types::string::Cid::new(cid),
                                    uri,
                                    validation_status: None,
                                }
                                .into(),
                            )));
                        }
                    }
                    InputWritesItem::Delete(delete) => {
                        let collection = delete.collection.as_str();
                        let rkey = delete.rkey.as_str();
                        let record_path = format!("{}/{}", collection, rkey);

                        // Get current record
                        let record = txn.record().get_record(&record_path)?;

                        if let Some(existing) = record {
                            let prev = Cid::from_str(&existing.cid)?;

                            // Delete record
                            txn.repo().delete_record(collection, rkey)?;

                            ops.push(RepoOp::Delete {
                                path: record_path,
                                prev,
                            });

                            results.push(OutputResultsItem::DeleteResult(Box::new(
                                apply_writes::DeleteResultData {}.into(),
                            )));
                        }
                    }
                }
            }

            // Process blob associations
            for (blob_cid, record_uri) in &blob_info {
                txn.blob()
                    .register_blob(blob_cid.to_string(), "application/octet-stream", 0)?;
                txn.blob()
                    .associate_blob(&blob_cid.to_string(), record_uri)?;
            }

            // Get updated repo root
            let repo_root = txn.repo().get_root()?;

            // Update metrics
            counter!(REPO_COMMITS).increment(1);
            for op in &ops {
                match op {
                    RepoOp::Create { .. } => counter!(REPO_OP_CREATE).increment(1),
                    RepoOp::Update { .. } => counter!(REPO_OP_UPDATE).increment(1),
                    RepoOp::Delete { .. } => counter!(REPO_OP_DELETE).increment(1),
                }
            }

            // Send ops to firehose
            // (In real impl, we'd construct a firehose event here)

            Ok(apply_writes::OutputData {
                results: Some(results),
                commit: Some(
                    CommitMetaData {
                        cid: atrium_api::types::string::Cid::new(Cid::from_str(&repo_root.cid)?),
                        rev: Tid::from_str(&repo_root.rev)?,
                    }
                    .into(),
                ),
            })
        })
        .await?;

    Ok(Json(result.into()))
}

/// Create a single new repository record
async fn create_record(
    user: AuthenticatedUser,
    state: State<AppConfig>,
    db_state: State<Db>,
    fhp_state: State<FirehoseProducer>,
    Json(input): Json<repo::create_record::Input>,
) -> Result<Json<repo::create_record::Output>> {
    // Convert the create_record operation to apply_writes
    let apply_writes_input = repo::apply_writes::InputData {
        repo: input.repo.clone(),
        validate: input.validate,
        swap_commit: input.swap_commit.clone(),
        writes: vec![repo::apply_writes::InputWritesItem::Create(Box::new(
            repo::apply_writes::CreateData {
                collection: input.collection.clone(),
                rkey: input.rkey.clone(),
                value: input.record.clone(),
            }
            .into(),
        ))],
    }
    .into();

    let write_result =
        apply_writes(user, state, db_state, fhp_state, Json(apply_writes_input)).await?;

    // Extract the first result
    let create_result = match write_result
        .results
        .and_then(|results| results.first().cloned())
    {
        Some(repo::apply_writes::OutputResultsItem::CreateResult(res)) => res,
        _ => {
            return Err(Error::with_status(
                StatusCode::INTERNAL_SERVER_ERROR,
                anyhow!("unexpected result type from apply_writes"),
            ));
        }
    };

    Ok(Json(
        repo::create_record::OutputData {
            cid: create_result.cid.clone(),
            commit: write_result.commit.clone(),
            uri: create_result.uri.clone(),
            validation_status: Some("unknown".to_owned()),
        }
        .into(),
    ))
}

/// Update or create a repository record
async fn put_record(
    user: AuthenticatedUser,
    state: State<AppConfig>,
    db_state: State<Db>,
    fhp_state: State<FirehoseProducer>,
    Json(input): Json<repo::put_record::Input>,
) -> Result<Json<repo::put_record::Output>> {
    // Convert put_record operation to apply_writes
    let apply_writes_input = repo::apply_writes::InputData {
        repo: input.repo,
        validate: input.validate,
        swap_commit: input.swap_commit,
        writes: vec![repo::apply_writes::InputWritesItem::Update(Box::new(
            repo::apply_writes::UpdateData {
                collection: input.collection,
                rkey: input.rkey,
                value: input.record,
            }
            .into(),
        ))],
    }
    .into();

    let write_result =
        apply_writes(user, state, db_state, fhp_state, Json(apply_writes_input)).await?;

    // Extract result with appropriate handling for create/update
    let (cid, uri) = match write_result.results.and_then(|r| r.first().cloned()) {
        Some(repo::apply_writes::OutputResultsItem::CreateResult(create)) => {
            (create.cid, create.uri)
        }
        Some(repo::apply_writes::OutputResultsItem::UpdateResult(update)) => {
            (update.cid, update.uri)
        }
        _ => {
            return Err(Error::with_status(
                StatusCode::INTERNAL_SERVER_ERROR,
                anyhow!("unexpected result type from apply_writes"),
            ));
        }
    };

    Ok(Json(
        repo::put_record::OutputData {
            cid,
            commit: write_result.commit,
            uri,
            validation_status: Some("unknown".to_owned()),
        }
        .into(),
    ))
}

/// Delete a repository record
async fn delete_record(
    user: AuthenticatedUser,
    state: State<AppConfig>,
    db_state: State<Db>,
    fhp_state: State<FirehoseProducer>,
    Json(input): Json<repo::delete_record::Input>,
) -> Result<Json<repo::delete_record::Output>> {
    // Convert delete_record operation to apply_writes
    let apply_writes_input = repo::apply_writes::InputData {
        repo: input.repo,
        validate: None,
        swap_commit: input.swap_commit,
        writes: vec![repo::apply_writes::InputWritesItem::Delete(Box::new(
            repo::apply_writes::DeleteData {
                collection: input.collection,
                rkey: input.rkey,
            }
            .into(),
        ))],
    }
    .into();

    let write_result =
        apply_writes(user, state, db_state, fhp_state, Json(apply_writes_input)).await?;

    Ok(Json(
        repo::delete_record::OutputData {
            commit: write_result.commit,
        }
        .into(),
    ))
}

/// Get information about an account and repository
async fn describe_repo(
    State(config): State<AppConfig>,
    State(db): State<Db>,
    Query(input): Query<repo::describe_repo::ParametersData>,
) -> Result<Json<repo::describe_repo::Output>> {
    // Resolve the DID
    let (did, handle) = resolve_did(&db, &input.repo).await?;

    // Get collections from actor store
    let actor_store = ActorStore::new(Arc::new(config));
    let collections = actor_store
        .read(did.as_str(), |reader| reader.record().list_collections())
        .await?;

    Ok(Json(
        repo::describe_repo::OutputData {
            collections: collections
                .into_iter()
                .map(|c| Nsid::new(c).expect("valid NSID"))
                .collect(),
            did,
            did_doc: Unknown::Null, // TODO: fetch from PLC directory
            handle,
            handle_is_correct: true, // TODO: validate handle resolution
        }
        .into(),
    ))
}

/// Get a single record from a repository
async fn get_record(
    State(config): State<AppConfig>,
    State(db): State<Db>,
    Query(input): Query<repo::get_record::ParametersData>,
) -> Result<Json<repo::get_record::Output>> {
    if input.cid.is_some() {
        return Err(Error::unimplemented(anyhow!(
            "fetching specific CID versions not supported"
        )));
    }

    // Resolve the DID
    let (did, _) = resolve_did(&db, &input.repo).await?;

    // Construct record path and URI
    let key = format!("{}/{}", input.collection.as_str(), input.rkey.as_str());
    let uri = format!("at://{}/{}", did.as_str(), key);

    // Get record from actor store
    let actor_store = ActorStore::new(Arc::new(config));
    let record_opt = actor_store
        .read(did.as_str(), |reader| reader.record().get_record(&key))
        .await?;

    match record_opt {
        Some(record) => {
            // Parse record content
            let value: serde_json::Value = serde_json::from_slice(&record.content)?;
            let cid = Cid::from_str(&record.cid)?;

            Ok(Json(
                repo::get_record::OutputData {
                    cid: Some(atrium_api::types::string::Cid::new(cid)),
                    uri,
                    value: value.try_into_unknown()?,
                }
                .into(),
            ))
        }
        None => Err(Error::with_message(
            StatusCode::BAD_REQUEST,
            anyhow!("record not found: {}", uri),
            ErrorMessage::new("RecordNotFound", format!("Could not locate record: {uri}")),
        )),
    }
}

/// List records from a repository collection
async fn list_records(
    State(config): State<AppConfig>,
    State(db): State<Db>,
    Query(input): Query<Object<ListRecordsParameters>>,
) -> Result<Json<repo::list_records::Output>> {
    // Resolve the DID
    let (did, _) = resolve_did(&db, &input.repo).await?;

    // Parse limit parameter
    let limit = input
        .limit
        .as_deref()
        .and_then(|l| l.parse::<u32>().ok())
        .unwrap_or(50)
        .min(100);

    // Get records from actor store
    let actor_store = ActorStore::new(Arc::new(config));
    let records = actor_store
        .read(did.as_str(), |reader| {
            reader.record().list_collection_records(
                input.collection.as_str(),
                limit,
                input.cursor.as_deref(),
                input.reverse.unwrap_or(false),
            )
        })
        .await?;

    // Format records for response
    let mut result_records = Vec::new();
    let mut last_cursor = None;

    for record in &records {
        // Parse record
        let value: serde_json::Value = serde_json::from_slice(&record.content)?;
        let cid = Cid::from_str(&record.cid)?;
        last_cursor = Some(record.rkey.clone());

        result_records.push(
            repo::list_records::RecordData {
                cid: atrium_api::types::string::Cid::new(cid),
                uri: format!(
                    "at://{}/{}/{}",
                    did.as_str(),
                    input.collection.as_str(),
                    record.rkey
                ),
                value: value.try_into_unknown()?,
            }
            .into(),
        );
    }

    Ok(Json(
        repo::list_records::OutputData {
            cursor: last_cursor,
            records: result_records,
        }
        .into(),
    ))
}

/// Upload a blob
async fn upload_blob(
    user: AuthenticatedUser,
    State(config): State<AppConfig>,
    request: Request<Body>,
) -> Result<Json<repo::upload_blob::Output>> {
    // Get content-length and content-type
    let length = request
        .headers()
        .get(http::header::CONTENT_LENGTH)
        .context("no content length provided")?
        .to_str()?
        .parse::<u64>()?;

    let mime = request
        .headers()
        .get(http::header::CONTENT_TYPE)
        .context("no content-type provided")?
        .to_str()?
        .to_owned();

    // Check size limit
    if length > config.blob.limit {
        return Err(Error::with_status(
            StatusCode::PAYLOAD_TOO_LARGE,
            anyhow!("size {} above limit {}", length, config.blob.limit),
        ));
    }

    // Create temp file
    let filename = config
        .blob
        .path
        .join(format!("temp-{}.blob", chrono::Utc::now().timestamp()));
    let mut file = tokio::fs::File::create(&filename).await?;

    // Process upload
    let mut len = 0;
    let mut sha = Sha256::new();
    let mut stream = request.into_body().into_data_stream();

    while let Some(bytes) = stream.try_next().await? {
        len += bytes.len();

        if len as u64 > config.blob.limit {
            drop(file);
            tokio::fs::remove_file(&filename).await?;
            return Err(Error::with_status(
                StatusCode::PAYLOAD_TOO_LARGE,
                anyhow!("actual size exceeds limit"),
            ));
        }

        sha.update(&bytes);
        file.write_all(&bytes).await?;
    }

    // Finalize blob
    drop(file);
    let hash = sha.finalize();

    let cid = Cid::new_v1(
        0x55,                                                 // IPLD RAW
        atrium_repo::Multihash::wrap(0x12, hash.as_slice())?, // SHA2-256
    );

    let cid_str = cid.to_string();

    // Move to permanent location
    tokio::fs::rename(
        &filename,
        config.blob.path.join(format!("{}.blob", cid_str)),
    )
    .await?;

    // Register blob in actor store
    let actor_store = ActorStore::new(Arc::new(config));
    actor_store
        .transact(user.did(), |txn| {
            txn.blob()
                .register_blob(cid_str.clone(), mime.clone(), len as u64)
        })
        .await?;

    Ok(Json(
        repo::upload_blob::OutputData {
            blob: atrium_api::types::BlobRef::Typed(atrium_api::types::TypedBlobRef::Blob(
                atrium_api::types::Blob {
                    r#ref: atrium_api::types::CidLink(cid),
                    mime_type: mime,
                    size: len,
                },
            )),
        }
        .into(),
    ))
}

async fn todo() -> Result<()> {
    Err(Error::unimplemented(anyhow!("not implemented")))
}

/// Register repo endpoints
pub(super) fn routes() -> Router<AppState> {
    Router::new()
        .route(concat!("/", repo::apply_writes::NSID), post(apply_writes))
        .route(concat!("/", repo::create_record::NSID), post(create_record))
        .route(concat!("/", repo::put_record::NSID), post(put_record))
        .route(concat!("/", repo::delete_record::NSID), post(delete_record))
        .route(concat!("/", repo::upload_blob::NSID), post(upload_blob))
        .route(concat!("/", repo::describe_repo::NSID), get(describe_repo))
        .route(concat!("/", repo::get_record::NSID), get(get_record))
        .route(concat!("/", repo::import_repo::NSID), post(todo))
        .route(concat!("/", repo::list_missing_blobs::NSID), get(todo))
        .route(concat!("/", repo::list_records::NSID), get(list_records))
}
