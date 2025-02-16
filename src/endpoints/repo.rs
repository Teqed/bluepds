use std::collections::HashSet;

use anyhow::{anyhow, bail, Context};
use atrium_api::{
    com::atproto::repo::{self, defs::CommitMetaData},
    types::{
        string::{AtIdentifier, Nsid, Tid},
        TryIntoUnknown, Unknown,
    },
};
use atrium_repo::{blockstore::CarStore, Cid};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use constcat::concat;
use futures::TryStreamExt;

use crate::{
    auth::AuthenticatedUser,
    config::AppConfig,
    firehose::{self, FirehoseProducer},
    storage, AppState, Db, Error, Result, SigningKey,
};

async fn swap_commit(
    db: &Db,
    cid: Cid,
    rev: Tid,
    old_cid: Option<Cid>,
    did_str: &str,
) -> anyhow::Result<bool> {
    let cid_str = cid.to_string();
    let rev_str = rev.to_string();

    if let Some(swap) = &old_cid {
        let swap_str = swap.to_string();

        let r = sqlx::query!(
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
        Ok(r.rows_affected() != 0)
    } else {
        sqlx::query!(
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

async fn resolve_did(
    db: &Db,
    ident: &AtIdentifier,
) -> anyhow::Result<(
    atrium_api::types::string::Did,
    atrium_api::types::string::Handle,
)> {
    let (handle, did) = match &ident {
        AtIdentifier::Handle(handle) => {
            let handle = handle.as_str();
            let did = sqlx::query_scalar!(r#"SELECT did FROM handles WHERE handle = ?"#, handle)
                .fetch_one(db)
                .await
                .context("failed to query did")?;

            (handle.to_string(), did)
        }
        AtIdentifier::Did(did) => {
            let did = did.as_str();
            let handle = sqlx::query_scalar!(r#"SELECT handle FROM handles WHERE did = ?"#, did)
                .fetch_one(db)
                .await
                .context("failed to query did")?;

            (handle, did.to_string())
        }
    };

    Ok((
        atrium_api::types::string::Did::new(did).unwrap(),
        atrium_api::types::string::Handle::new(handle).unwrap(),
    ))
}

async fn apply_writes(
    user: AuthenticatedUser,
    State(skey): State<SigningKey>,
    State(config): State<AppConfig>,
    State(db): State<Db>,
    State(fhp): State<FirehoseProducer>,
    Json(input): Json<repo::apply_writes::Input>,
) -> Result<Json<repo::apply_writes::Output>> {
    use atrium_api::com::atproto::repo::apply_writes::{self, InputWritesItem, OutputResultsItem};

    let mut repo = storage::open_repo_db(&config.repo, &db, user.did())
        .await
        .context("failed to open user repo")?;
    let orig_rev = repo.commit().rev();

    let mut res = vec![];
    let mut keys = vec![];
    for write in &input.writes {
        let (builder, key) = match write {
            InputWritesItem::Create(object) => {
                let key = format!(
                    "{}/{}",
                    object.collection.as_str(),
                    object.rkey.as_deref().unwrap_or(Tid::now(0).as_str())
                );
                let uri = format!("at://{}/{}", user.did(), key);

                let (b, c) = repo
                    .add_raw(&key, &object.value)
                    .await
                    .context("failed to add record")?;

                res.push(OutputResultsItem::CreateResult(Box::new(
                    apply_writes::CreateResultData {
                        cid: atrium_api::types::string::Cid::new(c),
                        uri,
                        validation_status: None,
                    }
                    .into(),
                )));

                (b, key)
            }
            InputWritesItem::Update(object) => {
                let key = format!("{}/{}", object.collection.as_str(), object.rkey);
                let uri = format!("at://{}/{}", user.did(), key);

                let (b, c) = repo
                    .update_raw(&key, &object.value)
                    .await
                    .context("failed to add record")?;

                res.push(OutputResultsItem::UpdateResult(Box::new(
                    apply_writes::UpdateResultData {
                        cid: atrium_api::types::string::Cid::new(c),
                        uri,
                        validation_status: None,
                    }
                    .into(),
                )));

                (b, key)
            }
            InputWritesItem::Delete(object) => {
                let key = format!("{}/{}", object.collection.as_str(), object.rkey);

                res.push(OutputResultsItem::DeleteResult(Box::new(
                    apply_writes::DeleteResultData {}.into(),
                )));

                let b = repo
                    .delete_raw(&key)
                    .await
                    .context("failed to add record")?;

                (b, key)
            }
        };

        let sig = skey
            .sign(&builder.hash())
            .context("failed to sign commit")?;

        builder
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

    // TODO: Generate a firehose record.

    if !swap_commit(
        &db,
        repo.root(),
        repo.commit().rev(),
        input.swap_commit.as_ref().map(|c| c.as_ref().clone()),
        &user.did(),
    )
    .await
    .context("failed to swap commit")?
    {
        // This should always succeed.
        let old = input.swap_commit.clone().unwrap();

        return Ok(Json(
            repo::apply_writes::OutputData {
                results: None,
                commit: Some(
                    CommitMetaData {
                        cid: old.clone(),
                        rev: orig_rev.to_string(),
                    }
                    .into(),
                ),
            }
            .into(),
        ));
    }

    Ok(Json(
        repo::apply_writes::OutputData {
            results: Some(res),
            commit: Some(
                CommitMetaData {
                    cid: atrium_api::types::string::Cid::new(repo.root()),
                    rev: repo.commit().rev().to_string(),
                }
                .into(),
            ),
        }
        .into(),
    ))
}

async fn create_record(
    user: AuthenticatedUser,
    State(skey): State<SigningKey>,
    State(config): State<AppConfig>,
    State(db): State<Db>,
    State(fhp): State<FirehoseProducer>,
    Json(input): Json<repo::create_record::Input>,
) -> Result<Json<repo::create_record::Output>> {
    let mut repo = storage::open_repo_db(&config.repo, &db, user.did())
        .await
        .context("failed to open user repo")?;
    let orig_rev = repo.commit().rev();

    let key = format!(
        "{}/{}",
        input.collection.as_str(),
        input.rkey.as_deref().unwrap_or(Tid::now(0).as_str())
    );
    let uri = format!("at://{}/{}", user.did(), &key);

    let (builder, rcid) = repo
        .add_raw(&key, &input.record)
        .await
        .context("failed to add record")?;

    let sig = skey
        .sign(&builder.hash())
        .context("failed to sign commit")?;

    let ccid = builder
        .finalize(sig)
        .await
        .context("failed to write signed commit")?;

    let mut contents = Vec::new();
    let mut ret_store = CarStore::create_with_roots(std::io::Cursor::new(&mut contents), [ccid])
        .await
        .context("failed to create car store")?;

    repo.extract_raw_into(&key, &mut ret_store)
        .await
        .context("failed to extract commits")?;

    fhp.commit(firehose::Commit {
        car: contents,
        ops: vec![firehose::RepoOp::Create {
            cid: rcid,
            path: key,
        }],
        cid: ccid,
        rev: repo.commit().rev().to_string(),
        did: atrium_api::types::string::Did::new(user.did()).unwrap(),
    })
    .await;

    if !swap_commit(
        &db,
        repo.root(),
        repo.commit().rev(),
        input.swap_commit.as_ref().map(|c| c.as_ref().clone()),
        &user.did(),
    )
    .await
    .context("failed to swap commit")?
    {
        // This should always succeed.
        let old = input.swap_commit.clone().unwrap();

        return Ok(Json(
            repo::create_record::OutputData {
                // FIXME: Not really sure what this should be.
                cid: atrium_api::types::string::Cid::new(ccid),
                commit: Some(
                    CommitMetaData {
                        cid: old.clone(),
                        rev: orig_rev.to_string(),
                    }
                    .into(),
                ),
                uri,
                validation_status: None,
            }
            .into(),
        ));
    }

    Ok(Json(
        repo::create_record::OutputData {
            cid: atrium_api::types::string::Cid::new(rcid),
            commit: Some(
                CommitMetaData {
                    cid: atrium_api::types::string::Cid::new(ccid),
                    rev: repo.commit().rev().to_string(),
                }
                .into(),
            ),
            uri,
            validation_status: Some("unknown".to_string()),
        }
        .into(),
    ))
}

async fn put_record(
    user: AuthenticatedUser,
    State(skey): State<SigningKey>,
    State(config): State<AppConfig>,
    State(db): State<Db>,
    State(fhp): State<FirehoseProducer>,
    Json(input): Json<repo::put_record::Input>,
) -> Result<Json<repo::put_record::Output>> {
    let mut repo = storage::open_repo_db(&config.repo, &db, user.did())
        .await
        .context("failed to open user repo")?;
    let orig_rev = repo.commit().rev();

    let key = format!("{}/{}", input.collection.as_str(), input.rkey);
    let uri = format!("at://{}/{}", user.did(), &key);

    let (builder, rcid) = if let Ok((builder, rcid)) = repo
        .update_raw(&key, &input.record)
        .await
        .context("failed to add record")
    {
        (builder, rcid)
    } else {
        repo.add_raw(&key, &input.record)
            .await
            .context("failed to add record")?
    };

    let sig = skey
        .sign(&builder.hash())
        .context("failed to sign commit")?;

    let ccid = builder
        .finalize(sig)
        .await
        .context("failed to write signed commit")?;

    let mut contents = Vec::new();
    let mut ret_store = CarStore::create_with_roots(std::io::Cursor::new(&mut contents), [ccid])
        .await
        .context("failed to create car store")?;

    repo.extract_raw_into(&key, &mut ret_store)
        .await
        .context("failed to extract commits")?;

    // FIXME: Probably need to broadcast a `Create` op if the record was created.
    fhp.commit(firehose::Commit {
        car: contents,
        ops: vec![firehose::RepoOp::Update {
            cid: rcid,
            path: key,
        }],
        cid: ccid,
        rev: repo.commit().rev().to_string(),
        did: atrium_api::types::string::Did::new(user.did()).unwrap(),
    })
    .await;

    if !swap_commit(
        &db,
        repo.root(),
        repo.commit().rev(),
        input.swap_commit.as_ref().map(|c| c.as_ref().clone()),
        &user.did(),
    )
    .await
    .context("failed to swap commit")?
    {
        // This should always succeed.
        let old = input.swap_commit.clone().unwrap();

        return Ok(Json(
            repo::put_record::OutputData {
                // FIXME: Not really sure what this should be.
                cid: atrium_api::types::string::Cid::new(ccid),
                commit: Some(
                    CommitMetaData {
                        cid: old.clone(),
                        rev: orig_rev.to_string(),
                    }
                    .into(),
                ),
                uri,
                validation_status: None,
            }
            .into(),
        ));
    }

    Ok(Json(
        repo::put_record::OutputData {
            cid: atrium_api::types::string::Cid::new(rcid),
            commit: Some(
                CommitMetaData {
                    cid: atrium_api::types::string::Cid::new(ccid),
                    rev: repo.commit().rev().to_string(),
                }
                .into(),
            ),
            uri,
            validation_status: Some("unknown".to_string()),
        }
        .into(),
    ))
}

async fn delete_record(
    user: AuthenticatedUser,
    State(skey): State<SigningKey>,
    State(config): State<AppConfig>,
    State(db): State<Db>,
    State(fhp): State<FirehoseProducer>,
    Json(input): Json<repo::delete_record::Input>,
) -> Result<Json<repo::delete_record::Output>> {
    let mut repo = storage::open_repo_db(&config.repo, &db, user.did())
        .await
        .context("failed to open user repo")?;
    let orig_rev = repo.commit().rev();

    let key = format!("{}/{}", input.collection.as_str(), input.rkey);

    let builder = repo
        .delete_raw(&key)
        .await
        .context("failed to delete record")?;

    let sig = skey
        .sign(&builder.hash())
        .context("failed to sign commit")?;

    let ccid = builder
        .finalize(sig)
        .await
        .context("failed to write signed commit")?;

    let mut contents = Vec::new();
    let mut ret_store = CarStore::create_with_roots(std::io::Cursor::new(&mut contents), [ccid])
        .await
        .context("failed to create car store")?;

    repo.extract_raw_into(&key, &mut ret_store)
        .await
        .context("failed to extract commits")?;

    fhp.commit(firehose::Commit {
        car: contents,
        ops: vec![firehose::RepoOp::Delete { path: key }],
        cid: ccid,
        rev: repo.commit().rev().to_string(),
        did: atrium_api::types::string::Did::new(user.did()).unwrap(),
    })
    .await;

    if !swap_commit(
        &db,
        repo.root(),
        repo.commit().rev(),
        input.swap_commit.as_ref().map(|c| c.as_ref().clone()),
        &user.did(),
    )
    .await
    .context("failed to swap commit")?
    {
        // This should always succeed.
        let old = input.swap_commit.clone().unwrap();

        return Ok(Json(
            repo::delete_record::OutputData {
                commit: Some(
                    CommitMetaData {
                        cid: old.clone(),
                        rev: orig_rev.to_string(),
                    }
                    .into(),
                ),
            }
            .into(),
        ));
    }

    Ok(Json(
        repo::delete_record::OutputData {
            commit: Some(
                CommitMetaData {
                    cid: atrium_api::types::string::Cid::new(ccid),
                    rev: repo.commit().rev().to_string(),
                }
                .into(),
            ),
        }
        .into(),
    ))
}

async fn describe_repo(
    State(config): State<AppConfig>,
    State(db): State<Db>,
    Query(input): Query<repo::describe_repo::Parameters>,
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
            collections.insert(collection.to_string());
        }
    }

    Ok(Json(
        repo::describe_repo::OutputData {
            collections: collections
                .into_iter()
                .map(|s| Nsid::new(s).unwrap())
                .collect::<Vec<_>>(),
            did: did.clone(),
            did_doc: Unknown::Null,
            handle: handle.clone(),
            handle_is_correct: true, // TODO
        }
        .into(),
    ))
}

async fn get_record(
    State(config): State<AppConfig>,
    State(db): State<Db>,
    Query(input): Query<repo::get_record::Parameters>,
) -> Result<Json<repo::get_record::Output>> {
    // Lookup the DID by the provided handle.
    let (did, _handle) = resolve_did(&db, &input.repo)
        .await
        .context("failed to resolve handle")?;

    let mut repo = storage::open_repo_db(&config.repo, &db, did.as_str())
        .await
        .context("failed to open user repo")?;

    let key = format!("{}/{}", input.collection.as_str(), input.rkey);
    let uri = format!("at://{}/{}", did.as_str(), &key);

    let record: Option<serde_json::Value> =
        repo.get_raw(&key).await.context("failed to read record")?;

    if let Some(record) = record {
        Ok(Json(
            repo::get_record::OutputData {
                cid: None, // TODO
                uri,
                value: record.try_into_unknown().unwrap(),
            }
            .into(),
        ))
    } else {
        return Err(Error::with_status(
            StatusCode::NOT_FOUND,
            anyhow!("could not find the requested record"),
        ));
    }
}

pub fn routes() -> Router<AppState> {
    // AP /xrpc/com.atproto.repo.applyWrites
    // AP /xrpc/com.atproto.repo.createRecord
    // AP /xrpc/com.atproto.repo.putRecord
    // AP /xrpc/com.atproto.repo.deleteRecord
    // UG /xrpc/com.atproto.repo.describeRepo
    // UG /xrpc/com.atproto.repo.getRecord
    // UG /xrpc/com.atproto.repo.listRecords
    // AP /xrpc/com.atproto.repo.uploadBlob
    Router::new()
        .route(concat!("/", repo::apply_writes::NSID), post(apply_writes))
        .route(concat!("/", repo::create_record::NSID), post(create_record))
        .route(concat!("/", repo::put_record::NSID), post(put_record))
        .route(concat!("/", repo::delete_record::NSID), post(delete_record))
        .route(concat!("/", repo::describe_repo::NSID), get(describe_repo))
        .route(concat!("/", repo::get_record::NSID), get(get_record))
}
