//! Apply a batch transaction of repository creates, updates, and deletes. Requires auth, implemented by PDS.
use std::{collections::HashSet, str::FromStr};

use anyhow::{Context as _, anyhow};
use atrium_api::com::atproto::repo::apply_writes::{self, InputWritesItem, OutputResultsItem};
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
    actor_store::{ActorStore, ActorStoreReader, ActorStoreTransactor, ActorStoreWriter},
    auth::AuthenticatedUser,
    config::AppConfig,
    error::ErrorMessage,
    firehose::{self, FirehoseProducer, RepoOp},
    metrics::{REPO_COMMITS, REPO_OP_CREATE, REPO_OP_DELETE, REPO_OP_UPDATE},
    repo::types::{PreparedWrite, WriteOpAction},
    storage,
};

use super::resolve_did;

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
pub(crate) async fn apply_writes(
    user: AuthenticatedUser,
    State(skey): State<SigningKey>,
    State(config): State<AppConfig>,
    State(db): State<Db>,
    State(fhp): State<FirehoseProducer>,
    Json(input): Json<repo::apply_writes::Input>,
) -> Result<Json<repo::apply_writes::Output>> {
    // TODO: `input.validate`

    // Resolve DID from identifier
    let (target_did, _) = resolve_did(&db, &input.repo)
        .await
        .context("failed to resolve did")?;

    // Ensure that we are updating the correct repository
    if target_did.as_str() != user.did() {
        return Err(Error::with_status(
            StatusCode::BAD_REQUEST,
            anyhow!("repo did not match the authenticated user"),
        ));
    }

    // Validate writes count
    if input.writes.len() > 200 {
        return Err(Error::with_status(
            StatusCode::BAD_REQUEST,
            anyhow!("Too many writes. Max: 200"),
        ));
    }

    // Convert input writes to prepared format
    let mut prepared_writes = Vec::with_capacity(input.writes.len());
    for write in input.writes.iter() {
        match write {
            InputWritesItem::Create(create) => {
                let uri = AtUri::make(
                    user.did(),
                    &create.collection.as_str(),
                    create
                        .rkey
                        .as_deref()
                        .unwrap_or(&Tid::now(LimitedU32::MIN).to_string()),
                );

                let cid = match cid_for_cbor(&create.value) {
                    Ok(cid) => cid,
                    Err(e) => {
                        return Err(Error::with_status(
                            StatusCode::BAD_REQUEST,
                            anyhow!("Failed to encode record: {}", e),
                        ));
                    }
                };

                let blobs = scan_blobs(&create.value)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|cid| {
                        // TODO: Create BlobRef from cid with proper metadata
                        BlobRef {
                            cid,
                            mime_type: "application/octet-stream".to_string(), // Default
                            size: 0, // Unknown at this point
                        }
                    })
                    .collect();

                prepared_writes.push(PreparedCreateOrUpdate {
                    action: WriteOpAction::Create,
                    uri: uri?.to_string(),
                    cid,
                    record: create.value.clone(),
                    blobs,
                    swap_cid: None,
                });
            }
            InputWritesItem::Update(update) => {
                let uri = AtUri::make(
                    user.did(),
                    Some(update.collection.to_string()),
                    Some(update.rkey.to_string()),
                );

                let cid = match cid_for_cbor(&update.value) {
                    Ok(cid) => cid,
                    Err(e) => {
                        return Err(Error::with_status(
                            StatusCode::BAD_REQUEST,
                            anyhow!("Failed to encode record: {}", e),
                        ));
                    }
                };

                let blobs = scan_blobs(&update.value)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|cid| {
                        // TODO: Create BlobRef from cid with proper metadata
                        BlobRef {
                            cid,
                            mime_type: "application/octet-stream".to_string(),
                            size: 0,
                        }
                    })
                    .collect();

                prepared_writes.push(PreparedCreateOrUpdate {
                    action: WriteOpAction::Update,
                    uri: uri?.to_string(),
                    cid,
                    record: update.value.clone(),
                    blobs,
                    swap_cid: None,
                });
            }
            InputWritesItem::Delete(delete) => {
                let uri = AtUri::make(user.did(), &delete.collection.as_str(), &delete.rkey);

                prepared_writes.push(PreparedCreateOrUpdate {
                    action: WriteOpAction::Delete,
                    uri: uri?.to_string(),
                    cid: Cid::default(), // Not needed for delete
                    record: serde_json::Value::Null,
                    blobs: vec![],
                    swap_cid: None,
                });
            }
        }
    }

    // Get swap commit CID if provided
    let swap_commit_cid = input.swap_commit.as_ref().map(|cid| *cid.as_ref());

    let did_str = user.did();
    let mut repo = storage::open_repo_db(&config.repo, &db, did_str)
        .await
        .context("failed to open user repo")?;
    let orig_cid = repo.root();
    let orig_rev = repo.commit().rev();

    let mut blobs = vec![];
    let mut res = vec![];
    let mut ops = vec![];

    for write in &prepared_writes {
        let (builder, key) = match write.action {
            WriteOpAction::Create => {
                let key = format!("{}/{}", write.uri.collection, write.uri.rkey);
                let uri = format!("at://{}/{}", user.did(), key);

                let (builder, cid) = repo
                    .add_raw(&key, &write.record)
                    .await
                    .context("failed to add record")?;

                // Extract and track blobs
                if let Ok(new_blobs) = scan_blobs(&write.record) {
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
            WriteOpAction::Update => {
                let key = format!("{}/{}", write.uri.collection, write.uri.rkey);
                let uri = format!("at://{}/{}", user.did(), key);

                let prev = repo
                    .tree()
                    .get(&key)
                    .await
                    .context("failed to search MST")?;

                if prev.is_none() {
                    // No existing record, treat as create
                    let (create_builder, cid) = repo
                        .add_raw(&key, &write.record)
                        .await
                        .context("failed to add record")?;

                    if let Ok(new_blobs) = scan_blobs(&write.record) {
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

                    (create_builder, key)
                } else {
                    // Update existing record
                    let prev = prev.context("should be able to find previous record")?;
                    let (update_builder, cid) = repo
                        .update_raw(&key, &write.record)
                        .await
                        .context("failed to add record")?;

                    if let Ok(new_blobs) = scan_blobs(&write.record) {
                        blobs.extend(
                            new_blobs
                                .into_iter()
                                .map(|blob_cid| (key.clone(), blob_cid)),
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

                    (update_builder, key)
                }
            }
            WriteOpAction::Delete => {
                let key = format!("{}/{}", write.uri.collection, write.uri.rkey);

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
    }

    // Construct a firehose record
    let mut mem = Vec::new();
    let mut store = CarStore::create_with_roots(std::io::Cursor::new(&mut mem), [repo.root()])
        .await
        .context("failed to create temp store")?;

    // Extract the records out of the user's repository
    for write in &prepared_writes {
        let key = format!("{}/{}", write.uri.collection, write.uri.rkey);
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

    // For updates and removals, unlink the old/deleted record from the blob_ref table
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

    // Process blobs
    for (key, cid) in &blobs {
        let cid_str = cid.to_string();

        // Handle the case where a new record references an existing blob
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

    // Update counters
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
