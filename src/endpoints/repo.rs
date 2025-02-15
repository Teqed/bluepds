use anyhow::Context;
use atrium_api::{
    com::atproto::repo::{self, defs::CommitMetaData},
    types::string::Tid,
};
use atrium_repo::blockstore::CarStore;
use axum::{extract::State, routing::post, Json, Router};
use constcat::concat;

use crate::{
    auth::AuthenticatedUser, config::AppConfig, storage, AppState, Db, Result, SigningKey,
};

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

    let key = format!(
        "{}/{}",
        input.collection.as_str(),
        input.rkey.clone().unwrap_or(Tid::now(0).to_string())
    );
    let builder = repo
        .add_raw(&key, &input.record)
        .await
        .context("failed to add record")?;

    let sig = skey
        .sign(&builder.hash())
        .context("failed to sign commit")?;

    let rcid = builder
        .finalize(sig)
        .await
        .context("failed to write signed commit")?;

    let mut contents = Vec::new();
    let mut ret_store = CarStore::create_with_roots(std::io::Cursor::new(&mut contents), [rcid])
        .await
        .context("failed to create car store")?;

    repo.extract_raw_into(&key, &mut ret_store)
        .await
        .context("failed to extract commits")?;

    // TODO: Broadcast `ret_store` on the firehose.

    let uri = format!("at://{}/{}", user.did(), &key);

    let rev_str = repo.commit().rev().to_string();
    let cid_str = rcid.to_string();
    let did_str = user.did();

    sqlx::query!(
        r#"UPDATE accounts SET root = ?, rev = ? WHERE did = ?"#,
        cid_str,
        rev_str,
        did_str,
    )
    .execute(&db)
    .await
    .context("failed to update root")?;

    Ok(Json(
        repo::create_record::OutputData {
            cid: atrium_api::types::string::Cid::new(rcid),
            commit: Some(
                CommitMetaData {
                    cid: atrium_api::types::string::Cid::new(rcid),
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

pub fn routes() -> Router<AppState> {
    // AP /xrpc/com.atproto.repo.applyWrites
    // AP /xrpc/com.atproto.repo.createRecord
    // AP /xrpc/com.atproto.repo.putRecord
    // AP /xrpc/com.atproto.repo.deleteRecord
    // UG /xrpc/com.atproto.repo.describeRepo
    // UG /xrpc/com.atproto.repo.getRecord
    // UG /xrpc/com.atproto.repo.listRecords
    // AP /xrpc/com.atproto.repo.uploadBlob
    Router::new().route(concat!("/", repo::create_record::NSID), post(create_record))
}
