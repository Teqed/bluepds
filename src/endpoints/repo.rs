//! PDS repository endpoints /xrpc/com.atproto.repo.*)
use std::{collections::HashSet, str::FromStr as _};

use anyhow::{Context as _, anyhow};
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
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncWriteExt as _;

use crate::{
    AppState, Db, Error, Result, SigningKey,
    auth::AuthenticatedUser,
    config::AppConfig,
    error::ErrorMessage,
    firehose::{self, FirehoseProducer, RepoOp},
    metrics::{REPO_COMMITS, REPO_OP_CREATE, REPO_OP_DELETE, REPO_OP_UPDATE},
    storage,
};

/// IPLD CID raw binary
const IPLD_RAW: u64 = 0x55;
/// SHA2-256 mulithash
const IPLD_MH_SHA2_256: u64 = 0x12;

/// Used in [`scan_blobs`] to identify a blob.
#[derive(Deserialize, Debug, Clone)]
struct BlobRef {
    /// `BlobRef` link. Include `$` when serializing to JSON, since `$` isn't allowed in struct names.
    #[serde(rename = "$link")]
    link: String,
}

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

async fn swap_commit(
    db: impl sqlx::Executor<'_, Database = sqlx::Sqlite>,
    cid: Cid,
    rev: Tid,
    old_cid: Option<Cid>,
    did_str: &str,
) -> anyhow::Result<bool> {
    let cid_str = cid.to_string();
    let rev_str = rev.to_string();

    if let Some(swap) = old_cid {
        let swap_str = swap.to_string();

        let result = sqlx::query!(
            r#"UPDATE accounts SET root = ?, rev = ? WHERE did = ? AND root = ?"#,
            cid_str,
            rev_str,
            did_str,
            swap_str,
        )
        .execute(db)
        .await
        .context("failed to update root")?;

        // If the swap failed, indicate as such.
        Ok(result.rows_affected() != 0)
    } else {
        _ = sqlx::query!(
            r#"UPDATE accounts SET root = ?, rev = ? WHERE did = ?"#,
            cid_str,
            rev_str,
            did_str,
        )
        .execute(db)
        .await
        .context("failed to update root")?;

        Ok(true)
    }
}

/// Resolves DID to DID document. Does not bi-directionally verify handle.
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

/// Used in [`apply_writes`] to scan for blobs in the JSON object and return their CIDs.
fn scan_blobs(unknown: &Unknown) -> anyhow::Result<Vec<Cid>> {
    // { "$type": "blob", "ref": { "$link": "bafyrei..." } }

    let mut cids = Vec::new();
    let mut stack = vec![
        serde_json::Value::try_from_unknown(unknown.clone())
            .context("failed to convert unknown into json")?,
    ];
    while let Some(value) = stack.pop() {
        match value {
            serde_json::Value::Bool(_)
            | serde_json::Value::Null
            | serde_json::Value::Number(_)
            | serde_json::Value::String(_) => (),
            serde_json::Value::Array(values) => stack.extend(values.into_iter()),
            serde_json::Value::Object(map) => {
                if let (Some(blob_type), Some(blob_ref)) = (map.get("$type"), map.get("ref")) {
                    if blob_type == &serde_json::Value::String("blob".to_owned()) {
                        if let Ok(rf) = serde_json::from_value::<BlobRef>(blob_ref.clone()) {
                            cids.push(Cid::from_str(&rf.link).context("failed to convert cid")?);
                        }
                    }
                }

                stack.extend(map.values().cloned());
            }
        }
    }

    Ok(cids)
}

#[test]
fn test_scan_blobs() {
    use std::str::FromStr as _;

    let json = serde_json::json!({
        "test": "a",
        "blob": {
            "$type": "blob",
            "ref": {
                "$link": "bafkreifzxf2wa6dyakzbdaxkz2wkvfrv3hiuafhxewbn5wahcw6eh3hzji"
            }
        }
    });

    let blob = scan_blobs(&json.try_into_unknown().expect("should be valid JSON"))
        .expect("should be able to scan blobs");
    assert_eq!(
        blob,
        vec![
            Cid::from_str("bafkreifzxf2wa6dyakzbdaxkz2wkvfrv3hiuafhxewbn5wahcw6eh3hzji")
                .expect("should be valid CID")
        ]
    );
}

#[expect(clippy::too_many_lines)]
/// Apply a batch transaction of repository creates, updates, and deletes. Requires auth, implemented by PDS.
/// - POST /xrpc/com.atproto.repo.applyWrites
/// ### Request Body
/// - `repo`: `at-identifier` // The handle or DID of the repo (aka, current account).
/// - `validate`: `boolean` // Can be set to 'false' to skip Lexicon schema validation of record data across all operations, 'true' to require it, or leave unset to validate only for known Lexicons.
/// - `writes`: `object[]` // One of:
/// - - com.atproto.repo.applyWrites.create
/// - - com.atproto.repo.applyWrites.update
/// - - com.atproto.repo.applyWrites.delete
/// - `swap_commit`: `cid` // If provided, the entire operation will fail if the current repo commit CID does not match this value. Used to prevent conflicting repo mutations.
async fn apply_writes(
    user: AuthenticatedUser,
    State(skey): State<SigningKey>,
    State(config): State<AppConfig>,
    State(db): State<Db>,
    State(fhp): State<FirehoseProducer>,
    Json(input): Json<repo::apply_writes::Input>,
) -> Result<Json<repo::apply_writes::Output>> {
    use atrium_api::com::atproto::repo::apply_writes::{self, InputWritesItem, OutputResultsItem};

    // TODO: `input.validate`

    let (target_did, _) = resolve_did(&db, &input.repo)
        .await
        .context("failed to resolve did")?;

    // Ensure that we are updating the correct repository.
    if target_did.as_str() != user.did() {
        return Err(Error::with_status(
            StatusCode::BAD_REQUEST,
            anyhow!("repo did not match the authenticated user"),
        ));
    }

    let mut repo = storage::open_repo_db(&config.repo, &db, user.did())
        .await
        .context("failed to open user repo")?;
    let orig_cid = repo.root();
    let orig_rev = repo.commit().rev();

    let mut blobs = vec![];
    let mut res = vec![];
    let mut ops = vec![];
    let mut keys = vec![];
    for write in &input.writes {
        let (builder, key) = match *write {
            InputWritesItem::Create(ref object) => {
                let now = Tid::now(LimitedU32::MIN);
                let key = format!(
                    "{}/{}",
                    &object.collection.as_str(),
                    &object.rkey.as_deref().unwrap_or_else(|| now.as_str())
                );
                let uri = format!("at://{}/{}", user.did(), key);

                let (builder, cid) = repo
                    .add_raw(&key, &object.value)
                    .await
                    .context("failed to add record")?;

                if let Ok(new_blobs) = scan_blobs(&object.value) {
                    blobs.extend(
                        new_blobs
                            .into_iter()
                            .map(|blob_cid| (key.clone(), blob_cid)),
                    );
                }

                ops.push(RepoOp::Create {
                    cid,
                    path: key.clone(),
                });

                res.push(OutputResultsItem::CreateResult(Box::new(
                    apply_writes::CreateResultData {
                        cid: atrium_api::types::string::Cid::new(cid),
                        uri,
                        validation_status: None,
                    }
                    .into(),
                )));

                (builder, key)
            }
            InputWritesItem::Update(ref object) => {
                let builder: atrium_repo::repo::CommitBuilder<_>;
                let key = format!("{}/{}", &object.collection.as_str(), &object.rkey.as_str());
                let uri = format!("at://{}/{}", user.did(), key);

                let prev = repo
                    .tree()
                    .get(&key)
                    .await
                    .context("failed to search MST")?;
                if prev.is_none() {
                    let (create_builder, cid) = repo
                        .add_raw(&key, &object.value)
                        .await
                        .context("failed to add record")?;
                    if let Ok(new_blobs) = scan_blobs(&object.value) {
                        blobs.extend(
                            new_blobs
                                .into_iter()
                                .map(|blod_cid| (key.clone(), blod_cid)),
                        );
                    }
                    ops.push(RepoOp::Create {
                        cid,
                        path: key.clone(),
                    });
                    res.push(OutputResultsItem::CreateResult(Box::new(
                        apply_writes::CreateResultData {
                            cid: atrium_api::types::string::Cid::new(cid),
                            uri,
                            validation_status: None,
                        }
                        .into(),
                    )));
                    builder = create_builder;
                } else {
                    let prev = prev.context("should be able to find previous record")?;
                    let (update_builder, cid) = repo
                        .update_raw(&key, &object.value)
                        .await
                        .context("failed to add record")?;
                    if let Ok(new_blobs) = scan_blobs(&object.value) {
                        blobs.extend(
                            new_blobs
                                .into_iter()
                                .map(|blod_cid| (key.clone(), blod_cid)),
                        );
                    }
                    ops.push(RepoOp::Update {
                        cid,
                        path: key.clone(),
                        prev,
                    });
                    res.push(OutputResultsItem::UpdateResult(Box::new(
                        apply_writes::UpdateResultData {
                            cid: atrium_api::types::string::Cid::new(cid),
                            uri,
                            validation_status: None,
                        }
                        .into(),
                    )));
                    builder = update_builder;
                }
                (builder, key)
            }
            InputWritesItem::Delete(ref object) => {
                let key = format!("{}/{}", &object.collection.as_str(), &object.rkey.as_str());

                let prev = repo
                    .tree()
                    .get(&key)
                    .await
                    .context("failed to search MST")?
                    .context("previous record does not exist")?;

                ops.push(RepoOp::Delete {
                    path: key.clone(),
                    prev,
                });

                res.push(OutputResultsItem::DeleteResult(Box::new(
                    apply_writes::DeleteResultData {}.into(),
                )));

                let builder = repo
                    .delete_raw(&key)
                    .await
                    .context("failed to add record")?;

                (builder, key)
            }
        };

        let sig = skey
            .sign(&builder.bytes())
            .context("failed to sign commit")?;

        _ = builder
            .finalize(sig)
            .await
            .context("failed to write signed commit")?;

        keys.push(key);
    }

    // Construct a firehose record.
    let mut mem = Vec::new();
    let mut store = CarStore::create_with_roots(std::io::Cursor::new(&mut mem), [repo.root()])
        .await
        .context("failed to create temp store")?;

    // Extract the records out of the user's repository.
    for key in keys {
        repo.extract_raw_into(&key, &mut store)
            .await
            .context("failed to extract key")?;
    }

    let mut tx = db.begin().await.context("failed to begin transaction")?;

    if !swap_commit(
        &mut *tx,
        repo.root(),
        repo.commit().rev(),
        input.swap_commit.as_ref().map(|cid| *cid.as_ref()),
        &user.did(),
    )
    .await
    .context("failed to swap commit")?
    {
        // This should always succeed.
        let old = input
            .swap_commit
            .clone()
            .context("swap_commit should always be Some")?;

        // The swap failed. Return the old commit and do not update the repository.
        return Ok(Json(
            apply_writes::OutputData {
                results: None,
                commit: Some(
                    CommitMetaData {
                        cid: old,
                        rev: orig_rev,
                    }
                    .into(),
                ),
            }
            .into(),
        ));
    }

    let did_str = user.did();

    // For updates and removals, unlink the old/deleted record from the blob_ref table.
    for op in &ops {
        match op {
            &RepoOp::Update { ref path, .. } | &RepoOp::Delete { ref path, .. } => {
                // FIXME: This may cause issues if a user deletes more than one record referencing the same blob.
                _ = &sqlx::query!(
                    r#"UPDATE blob_ref SET record = NULL WHERE did = ? AND record = ?"#,
                    did_str,
                    path
                )
                .execute(&mut *tx)
                .await
                .context("failed to remove blob_ref")?;
            }
            &RepoOp::Create { .. } => {}
        }
    }

    for &mut (ref key, cid) in &mut blobs {
        let cid_str = cid.to_string();

        // Handle the case where a new record references an existing blob.
        if sqlx::query!(
            r#"UPDATE blob_ref SET record = ? WHERE cid = ? AND did = ? AND record IS NULL"#,
            key,
            cid_str,
            did_str,
        )
        .execute(&mut *tx)
        .await
        .context("failed to update blob_ref")?
        .rows_affected()
            == 0
        {
            _ = sqlx::query!(
                r#"INSERT INTO blob_ref (record, cid, did) VALUES (?, ?, ?)"#,
                key,
                cid_str,
                did_str,
            )
            .execute(&mut *tx)
            .await
            .context("failed to update blob_ref")?;
        }
    }

    tx.commit()
        .await
        .context("failed to commit blob ref to database")?;

    // Update counters.
    counter!(REPO_COMMITS).increment(1);
    for op in &ops {
        match *op {
            RepoOp::Create { .. } => counter!(REPO_OP_CREATE).increment(1),
            RepoOp::Update { .. } => counter!(REPO_OP_UPDATE).increment(1),
            RepoOp::Delete { .. } => counter!(REPO_OP_DELETE).increment(1),
        }
    }

    // We've committed the transaction to the database, and the commit is now stored in the user's
    // canonical repository.
    // We can now broadcast this on the firehose.
    fhp.commit(firehose::Commit {
        car: mem,
        ops,
        cid: repo.root(),
        rev: repo.commit().rev().to_string(),
        did: atrium_api::types::string::Did::new(user.did()).expect("should be valid DID"),
        pcid: Some(orig_cid),
        blobs: blobs.into_iter().map(|(_, cid)| cid).collect::<Vec<_>>(),
    })
    .await;

    Ok(Json(
        apply_writes::OutputData {
            results: Some(res),
            commit: Some(
                CommitMetaData {
                    cid: atrium_api::types::string::Cid::new(repo.root()),
                    rev: repo.commit().rev(),
                }
                .into(),
            ),
        }
        .into(),
    ))
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
    State(skey): State<SigningKey>,
    State(config): State<AppConfig>,
    State(db): State<Db>,
    State(fhp): State<FirehoseProducer>,
    Json(input): Json<repo::create_record::Input>,
) -> Result<Json<repo::create_record::Output>> {
    let write_result = apply_writes(
        user,
        State(skey),
        State(config),
        State(db),
        State(fhp),
        Json(
            repo::apply_writes::InputData {
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
            .into(),
        ),
    )
    .await
    .context("failed to apply writes")?;

    let create_result = if let repo::apply_writes::OutputResultsItem::CreateResult(create_result) =
        write_result
            .results
            .clone()
            .and_then(|result| result.first().cloned())
            .context("unexpected output from apply_writes")?
    {
        Some(create_result)
    } else {
        None
    }
    .context("unexpected result from apply_writes")?;

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
    State(skey): State<SigningKey>,
    State(config): State<AppConfig>,
    State(db): State<Db>,
    State(fhp): State<FirehoseProducer>,
    Json(input): Json<repo::put_record::Input>,
) -> Result<Json<repo::put_record::Output>> {
    // TODO: `input.swap_record`
    // FIXME: "put" implies that we will create the record if it does not exist.
    // We currently only update existing records and/or throw an error if one doesn't exist.
    let input = (*input).clone();
    let input = repo::apply_writes::InputData {
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

    let write_result = apply_writes(
        user,
        State(skey),
        State(config),
        State(db),
        State(fhp),
        Json(input),
    )
    .await
    .context("failed to apply writes")?;

    let update_result = write_result
        .results
        .clone()
        .and_then(|result| result.first().cloned())
        .context("unexpected output from apply_writes")?;
    let (cid, uri) = match update_result {
        repo::apply_writes::OutputResultsItem::CreateResult(create_result) => (
            Some(create_result.cid.clone()),
            Some(create_result.uri.clone()),
        ),
        repo::apply_writes::OutputResultsItem::UpdateResult(update_result) => (
            Some(update_result.cid.clone()),
            Some(update_result.uri.clone()),
        ),
        repo::apply_writes::OutputResultsItem::DeleteResult(_) => (None, None),
    };
    Ok(Json(
        repo::put_record::OutputData {
            cid: cid.context("missing cid")?,
            commit: write_result.commit.clone(),
            uri: uri.context("missing uri")?,
            validation_status: Some("unknown".to_owned()),
        }
        .into(),
    ))
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
    State(skey): State<SigningKey>,
    State(config): State<AppConfig>,
    State(db): State<Db>,
    State(fhp): State<FirehoseProducer>,
    Json(input): Json<repo::delete_record::Input>,
) -> Result<Json<repo::delete_record::Output>> {
    // TODO: `input.swap_record`

    Ok(Json(
        repo::delete_record::OutputData {
            commit: apply_writes(
                user,
                State(skey),
                State(config),
                State(db),
                State(fhp),
                Json(
                    repo::apply_writes::InputData {
                        repo: input.repo.clone(),
                        swap_commit: input.swap_commit.clone(),
                        validate: None,
                        writes: vec![repo::apply_writes::InputWritesItem::Delete(Box::new(
                            repo::apply_writes::DeleteData {
                                collection: input.collection.clone(),
                                rkey: input.rkey.clone(),
                            }
                            .into(),
                        ))],
                    }
                    .into(),
                ),
            )
            .await
            .context("failed to apply writes")?
            .commit
            .clone(),
        }
        .into(),
    ))
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
    State(config): State<AppConfig>,
    State(db): State<Db>,
    Query(input): Query<repo::describe_repo::ParametersData>,
) -> Result<Json<repo::describe_repo::Output>> {
    // Lookup the DID by the provided handle.
    let (did, handle) = resolve_did(&db, &input.repo)
        .await
        .context("failed to resolve handle")?;

    let mut repo = storage::open_repo_db(&config.repo, &db, did.as_str())
        .await
        .context("failed to open user repo")?;

    let mut collections = HashSet::new();

    let mut tree = repo.tree();
    let mut it = Box::pin(tree.keys());
    while let Some(key) = it.try_next().await.context("failed to iterate repo keys")? {
        if let Some((collection, _rkey)) = key.split_once('/') {
            _ = collections.insert(collection.to_owned());
        }
    }

    Ok(Json(
        repo::describe_repo::OutputData {
            collections: collections
                .into_iter()
                .map(|nsid| Nsid::new(nsid).expect("should be valid NSID"))
                .collect::<Vec<_>>(),
            did: did.clone(),
            did_doc: Unknown::Null, // TODO: Fetch the DID document from the PLC directory
            handle: handle.clone(),
            handle_is_correct: true, // TODO
        }
        .into(),
    ))
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

    let mut repo = storage::open_repo_db(&config.repo, &db, did.as_str())
        .await
        .context("failed to open user repo")?;

    let key = format!("{}/{}", input.collection.as_str(), input.rkey.as_str());
    let uri = format!("at://{}/{}", did.as_str(), &key);

    let cid = repo
        .tree()
        .get(&key)
        .await
        .context("failed to find record")?;

    let record: Option<serde_json::Value> =
        repo.get_raw(&key).await.context("failed to read record")?;

    record.map_or_else(
        || {
            Err(Error::with_message(
                StatusCode::BAD_REQUEST,
                anyhow!("could not find the requested record at {}", uri),
                ErrorMessage::new("RecordNotFound", format!("Could not locate record: {uri}")),
            ))
        },
        |record_value| {
            Ok(Json(
                repo::get_record::OutputData {
                    cid: cid.map(atrium_api::types::string::Cid::new),
                    uri: uri.clone(),
                    value: record_value
                        .try_into_unknown()
                        .context("should be valid JSON")?,
                }
                .into(),
            ))
        },
    )
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
    State(config): State<AppConfig>,
    State(db): State<Db>,
    Query(input): Query<Object<repo::list_records::ParametersData>>,
) -> Result<Json<repo::list_records::Output>> {
    // TODO: `input.reverse`

    // Lookup the DID by the provided handle.
    let (did, _handle) = resolve_did(&db, &input.repo)
        .await
        .context("failed to resolve handle")?;

    let mut repo = storage::open_repo_db(&config.repo, &db, did.as_str())
        .await
        .context("failed to open user repo")?;

    let mut keys = Vec::new();
    let mut tree = repo.tree();

    let mut entry = input.collection.to_string();
    entry.push('/');
    let mut iterator = Box::pin(tree.entries_prefixed(entry.as_str()));
    while let Some((key, cid)) = iterator
        .try_next()
        .await
        .context("failed to iterate keys")?
    {
        keys.push((key, cid));
    }

    drop(iterator);

    // TODO: Calculate the view on `keys` using `cursor` and `limit`.

    let mut records = Vec::new();
    for &(ref key, cid) in &keys {
        let value: serde_json::Value = repo
            .get_raw(key)
            .await
            .context("failed to get record")?
            .context("record not found")?;

        records.push(
            repo::list_records::RecordData {
                cid: atrium_api::types::string::Cid::new(cid),
                uri: format!("at://{}/{}", did.as_str(), key),
                value: value.try_into_unknown().context("should be valid JSON")?,
            }
            .into(),
        );
    }

    #[expect(clippy::pattern_type_mismatch)]
    Ok(Json(
        repo::list_records::OutputData {
            cursor: keys.last().map(|(_, cid)| cid.to_string()),
            records,
        }
        .into(),
    ))
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

    // FIXME: Need to make this more robust. This will fail under load.
    let filename = config
        .blob
        .path
        .join(format!("temp-{}.blob", chrono::Utc::now().timestamp()));
    let mut file = tokio::fs::File::create(&filename)
        .await
        .context("failed to create temporary file")?;

    let mut len = 0_usize;
    let mut sha = Sha256::new();
    let mut stream = request.into_body().into_data_stream();
    while let Some(bytes) = stream.try_next().await.context("failed to receive file")? {
        len = len.checked_add(bytes.len()).context("size overflow")?;

        // Deal with any sneaky end-users trying to bypass size limitations.
        let len_u64: u64 = len.try_into().context("failed to convert `len`")?;
        if len_u64 > config.blob.limit {
            drop(file);
            tokio::fs::remove_file(&filename)
                .await
                .context("failed to remove temp file")?;

            return Err(Error::with_status(
                StatusCode::PAYLOAD_TOO_LARGE,
                anyhow!("size above limit and content-length header was wrong"),
            ));
        }

        sha.update(&bytes);

        file.write_all(&bytes)
            .await
            .context("failed to write blob")?;
    }

    drop(file);
    let hash = sha.finalize();

    let cid = Cid::new_v1(
        IPLD_RAW,
        atrium_repo::Multihash::wrap(IPLD_MH_SHA2_256, hash.as_slice())
            .context("should be valid hash")?,
    );

    let cid_str = cid.to_string();

    tokio::fs::rename(&filename, config.blob.path.join(format!("{cid_str}.blob")))
        .await
        .context("failed to finalize blob")?;

    let did_str = user.did();

    _ = sqlx::query!(
        r#"INSERT INTO blob_ref (cid, did, record) VALUES (?, ?, NULL)"#,
        cid_str,
        did_str
    )
    .execute(&db)
    .await
    .context("failed to insert blob into database")?;

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
