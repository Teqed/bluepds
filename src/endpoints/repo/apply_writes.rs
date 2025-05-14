//! Apply a batch transaction of repository creates, updates, and deletes. Requires auth, implemented by PDS.
use crate::{
    ActorPools, AppState, Db, Error, Result, SigningKey,
    actor_store::{ActorStore, sql_blob::BlobStoreSql},
    auth::AuthenticatedUser,
    config::AppConfig,
    error::ErrorMessage,
    firehose::{self, FirehoseProducer, RepoOp},
    metrics::{REPO_COMMITS, REPO_OP_CREATE, REPO_OP_DELETE, REPO_OP_UPDATE},
    storage,
};
use anyhow::bail;
use anyhow::{Context as _, anyhow};
use atrium_api::com::atproto::repo::apply_writes::{self, InputWritesItem, OutputResultsItem};
use atrium_api::{
    com::atproto::repo::{self, defs::CommitMetaData},
    types::{
        LimitedU32, Object, TryFromUnknown as _, TryIntoUnknown as _, Unknown,
        string::{AtIdentifier, Nsid, Tid},
    },
};
use atrium_repo::blockstore::CarStore;
use axum::{
    Json, Router,
    body::Body,
    extract::{Query, Request, State},
    http::{self, StatusCode},
    routing::{get, post},
};
use cidv10::Cid;
use constcat::concat;
use futures::TryStreamExt as _;
use futures::stream::{self, StreamExt};
use metrics::counter;
use rsky_lexicon::com::atproto::repo::{ApplyWritesInput, ApplyWritesInputRefWrite};
use rsky_pds::SharedSequencer;
use rsky_pds::account_manager::AccountManager;
use rsky_pds::account_manager::helpers::account::AvailabilityFlags;
use rsky_pds::apis::ApiError;
use rsky_pds::auth_verifier::AccessStandardIncludeChecks;
use rsky_pds::repo::prepare::{
    PrepareCreateOpts, PrepareDeleteOpts, PrepareUpdateOpts, prepare_create, prepare_delete,
    prepare_update,
};
use rsky_repo::types::PreparedWrite;
use rsky_syntax::aturi::AtUri;
use serde::Deserialize;
use std::{collections::HashSet, str::FromStr};
use tokio::io::AsyncWriteExt as _;

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
    State(db_actors): State<std::collections::HashMap<String, ActorPools>>,
    State(fhp): State<FirehoseProducer>,
    Json(input): Json<ApplyWritesInput>,
) -> Result<Json<repo::apply_writes::Output>> {
    let tx: ApplyWritesInput = input;
    let ApplyWritesInput {
        repo,
        validate,
        swap_commit,
        ..
    } = tx;
    let account = account_manager
        .get_account(
            &repo,
            Some(AvailabilityFlags {
                include_deactivated: Some(true),
                include_taken_down: None,
            }),
        )
        .await?;

    if let Some(account) = account {
        if account.deactivated_at.is_some() {
            return Err(Error::with_message(
                StatusCode::FORBIDDEN,
                anyhow!("Account is deactivated"),
                ErrorMessage::new("AccountDeactivated", "Account is deactivated"),
            ));
        }
        let did = account.did;
        if did != user.did() {
            return Err(Error::with_message(
                StatusCode::FORBIDDEN,
                anyhow!("AuthRequiredError"),
                ErrorMessage::new("AuthRequiredError", "Auth required"),
            ));
        }
        let did: &String = &did;
        if tx.writes.len() > 200 {
            return Err(Error::with_message(
                StatusCode::BAD_REQUEST,
                anyhow!("Too many writes. Max: 200"),
                ErrorMessage::new("TooManyWrites", "Too many writes. Max: 200"),
            ));
        }

        let writes: Vec<PreparedWrite> = stream::iter(tx.writes)
            .then(|write| async move {
                Ok::<PreparedWrite, anyhow::Error>(match write {
                    ApplyWritesInputRefWrite::Create(write) => PreparedWrite::Create(
                        prepare_create(PrepareCreateOpts {
                            did: did.clone(),
                            collection: write.collection,
                            rkey: write.rkey,
                            swap_cid: None,
                            record: serde_json::from_value(write.value)?,
                            validate,
                        })
                        .await?,
                    ),
                    ApplyWritesInputRefWrite::Update(write) => PreparedWrite::Update(
                        prepare_update(PrepareUpdateOpts {
                            did: did.clone(),
                            collection: write.collection,
                            rkey: write.rkey,
                            swap_cid: None,
                            record: serde_json::from_value(write.value)?,
                            validate,
                        })
                        .await?,
                    ),
                    ApplyWritesInputRefWrite::Delete(write) => {
                        PreparedWrite::Delete(prepare_delete(PrepareDeleteOpts {
                            did: did.clone(),
                            collection: write.collection,
                            rkey: write.rkey,
                            swap_cid: None,
                        })?)
                    }
                })
            })
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<PreparedWrite>, _>>()?;

        let swap_commit_cid = match swap_commit {
            Some(swap_commit) => Some(Cid::from_str(&swap_commit)?),
            None => None,
        };

        let actor_db = db_actors
            .get(did)
            .ok_or_else(|| anyhow!("Actor DB not found"))?;
        let mut actor_store = ActorStore::new(
            did.clone(),
            BlobStoreSql::new(did.clone(), actor_db.blob),
            actor_db.repo,
        );

        let commit = actor_store
            .process_writes(writes.clone(), swap_commit_cid)
            .await?;

        let mut lock = sequencer.sequencer.write().await;
        lock.sequence_commit(did.clone(), commit.clone()).await?;
        account_manager
            .update_repo_root(
                did.to_string(),
                commit.commit_data.cid,
                commit.commit_data.rev,
            )
            .await?;
        Ok(())
    } else {
        Err(Error::with_message(
            StatusCode::NOT_FOUND,
            anyhow!("Could not find repo: `{repo}`"),
            ErrorMessage::new("RepoNotFound", "Could not find repo"),
        ))
    }
}
