/// Delete a repository record, or ensure it doesn't exist. Requires auth, implemented by PDS.
use crate::account_manager::AccountManager;
use crate::account_manager::helpers::account::AvailabilityFlags;
use crate::{
    actor_store::ActorStore,
    auth::AuthenticatedUser,
    error::ApiError,
    serve::{ActorStorage, AppState},
};
use anyhow::{Result, bail};
use axum::{Json, extract::State};
use cidv10::Cid;
use rsky_lexicon::com::atproto::repo::DeleteRecordInput;
use rsky_pds::repo::prepare::{PrepareDeleteOpts, prepare_delete};
use rsky_pds::sequencer::Sequencer;
use rsky_repo::types::PreparedWrite;
use rsky_syntax::aturi::AtUri;
use std::collections::HashMap;
use std::hash::RandomState;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;

async fn inner_delete_record(
    body: DeleteRecordInput,
    user: AuthenticatedUser,
    sequencer: Arc<RwLock<Sequencer>>,
    actor_pools: HashMap<String, ActorStorage>,
    account_manager: Arc<RwLock<AccountManager>>,
) -> Result<()> {
    let DeleteRecordInput {
        repo,
        collection,
        rkey,
        swap_record,
        swap_commit,
    } = body;
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
    match account {
        None => bail!("Could not find repo: `{repo}`"),
        Some(account) if account.deactivated_at.is_some() => bail!("Account is deactivated"),
        Some(account) => {
            let did = account.did;
            // if did != auth.access.credentials.unwrap().did.unwrap() {
            if did != user.did() {
                bail!("AuthRequiredError")
            }

            let swap_commit_cid = match swap_commit {
                Some(swap_commit) => Some(Cid::from_str(&swap_commit)?),
                None => None,
            };
            let swap_record_cid = match swap_record {
                Some(swap_record) => Some(Cid::from_str(&swap_record)?),
                None => None,
            };

            let write = prepare_delete(PrepareDeleteOpts {
                did: did.clone(),
                collection,
                rkey,
                swap_cid: swap_record_cid,
            })?;
            let mut actor_store = ActorStore::from_actor_pools(&did, &actor_pools).await;
            let write_at_uri: AtUri = write.uri.clone().try_into()?;
            let record = actor_store
                .record
                .get_record(&write_at_uri, None, Some(true))
                .await?;
            let commit = match record {
                None => return Ok(()), // No-op if record already doesn't exist
                Some(_) => {
                    actor_store
                        .process_writes(vec![PreparedWrite::Delete(write.clone())], swap_commit_cid)
                        .await?
                }
            };

            _ = sequencer
                .write()
                .await
                .sequence_commit(did.clone(), commit.clone())
                .await?;
            account_manager
                .write()
                .await
                .update_repo_root(
                    did,
                    commit.commit_data.cid,
                    commit.commit_data.rev,
                    &actor_pools,
                )
                .await?;

            Ok(())
        }
    }
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
#[axum::debug_handler(state = AppState)]
pub async fn delete_record(
    user: AuthenticatedUser,
    State(db_actors): State<HashMap<String, ActorStorage, RandomState>>,
    State(account_manager): State<Arc<RwLock<AccountManager>>>,
    State(sequencer): State<Arc<RwLock<Sequencer>>>,
    Json(body): Json<DeleteRecordInput>,
) -> Result<(), ApiError> {
    match inner_delete_record(body, user, sequencer, db_actors, account_manager).await {
        Ok(()) => Ok(()),
        Err(error) => {
            tracing::error!("@LOG: ERROR: {error}");
            Err(ApiError::RuntimeError)
        }
    }
}
