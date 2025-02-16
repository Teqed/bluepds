use anyhow::Context;
use atrium_api::{
    com::atproto::repo::{self, defs::CommitMetaData},
    types::string::Tid,
};
use atrium_repo::{blockstore::CarStore, Cid};
use axum::{extract::State, routing::post, Json, Router};
use constcat::concat;

use crate::{
    auth::AuthenticatedUser, config::AppConfig, storage, AppState, Db, Result, SigningKey,
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

async fn apply_writes(
    user: AuthenticatedUser,
    State(skey): State<SigningKey>,
    State(config): State<AppConfig>,
    State(db): State<Db>,
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

    // TODO: Broadcast `ret_store` on the firehose.

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

    // TODO: Broadcast `ret_store` on the firehose.

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

    // TODO: Broadcast `ret_store` on the firehose.

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
}
