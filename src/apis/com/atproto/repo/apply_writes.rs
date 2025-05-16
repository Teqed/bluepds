//! Apply a batch transaction of repository creates, updates, and deletes. Requires auth, implemented by PDS.
use crate::SharedSequencer;
use crate::account_manager::helpers::account::AvailabilityFlags;
use crate::account_manager::{AccountManager, AccountManagerCreator, SharedAccountManager};
use crate::{
    ActorPools, AppState, SigningKey,
    actor_store::{ActorStore, sql_blob::BlobStoreSql},
    auth::AuthenticatedUser,
    config::AppConfig,
    error::{ApiError, ErrorMessage},
};
use anyhow::{Result, bail};
use axum::{
    Json, Router,
    body::Body,
    extract::{Query, Request, State},
    http::{self, StatusCode},
    routing::{get, post},
};
use cidv10::Cid;
use deadpool_diesel::sqlite::Pool;
use futures::stream::{self, StreamExt};
use rsky_lexicon::com::atproto::repo::{ApplyWritesInput, ApplyWritesInputRefWrite};
use rsky_pds::auth_verifier::AccessStandardIncludeChecks;
use rsky_pds::repo::prepare::{
    PrepareCreateOpts, PrepareDeleteOpts, PrepareUpdateOpts, prepare_create, prepare_delete,
    prepare_update,
};
use rsky_pds::sequencer::Sequencer;
use rsky_repo::types::PreparedWrite;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;

async fn inner_apply_writes(
    body: ApplyWritesInput,
    user: AuthenticatedUser,
    sequencer: &RwLock<Sequencer>,
    actor_pools: std::collections::HashMap<String, ActorPools>,
    account_manager: &RwLock<AccountManager>,
) -> Result<()> {
    let tx: ApplyWritesInput = body;
    let ApplyWritesInput {
        repo,
        validate,
        swap_commit,
        ..
    } = tx;
    let account = account_manager
        .read()
        .await
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
            bail!("Account is deactivated")
        }
        let did = account.did;
        if did != user.did() {
            bail!("AuthRequiredError")
        }
        let did: &String = &did;
        if tx.writes.len() > 200 {
            bail!("Too many writes. Max: 200")
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

        let mut actor_store = ActorStore::from_actor_pools(did, &actor_pools).await;

        let commit = actor_store
            .process_writes(writes.clone(), swap_commit_cid)
            .await?;

        _ = sequencer
            .write()
            .await
            .sequence_commit(did.clone(), commit.clone())
            .await?;
        account_manager
            .write()
            .await
            .update_repo_root(
                did.to_string(),
                commit.commit_data.cid,
                commit.commit_data.rev,
                &actor_pools,
            )
            .await?;
        Ok(())
    } else {
        bail!("Could not find repo: `{repo}`")
    }
}

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
#[axum::debug_handler(state = AppState)]
pub(crate) async fn apply_writes(
    user: AuthenticatedUser,
    State(state): State<AppState>,
    Json(body): Json<ApplyWritesInput>,
) -> Result<(), ApiError> {
    tracing::debug!("@LOG: debug apply_writes {body:#?}");
    let db_actors = state.db_actors;
    let sequencer = &state.sequencer.sequencer;
    let account_manager = &state.account_manager.account_manager;
    match inner_apply_writes(body, user, sequencer, db_actors, account_manager).await {
        Ok(()) => Ok(()),
        Err(error) => {
            tracing::error!("@LOG: ERROR: {error}");
            Err(ApiError::RuntimeError)
        }
    }
}
