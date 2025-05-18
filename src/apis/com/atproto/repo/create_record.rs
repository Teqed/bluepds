//! Create a single new repository record. Requires auth, implemented by PDS.
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
use rsky_lexicon::com::atproto::repo::{CreateRecordInput, CreateRecordOutput};
use rsky_pds::SharedIdResolver;
use rsky_pds::repo::prepare::{
    PrepareCreateOpts, PrepareDeleteOpts, prepare_create, prepare_delete,
};
use rsky_pds::sequencer::Sequencer;
use rsky_repo::types::{PreparedDelete, PreparedWrite};
use rsky_syntax::aturi::AtUri;
use std::collections::HashMap;
use std::hash::RandomState;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;

async fn inner_create_record(
    body: CreateRecordInput,
    user: AuthenticatedUser,
    sequencer: Arc<RwLock<Sequencer>>,
    actor_pools: std::collections::HashMap<String, ActorStorage>,
    account_manager: Arc<RwLock<AccountManager>>,
) -> Result<CreateRecordOutput> {
    let CreateRecordInput {
        repo,
        collection,
        record,
        rkey,
        validate,
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
    if let Some(account) = account {
        if account.deactivated_at.is_some() {
            bail!("Account is deactivated")
        }
        let did = account.did;
        // if did != auth.access.credentials.unwrap().did.unwrap() {
        if did != user.did() {
            bail!("AuthRequiredError")
        }
        let swap_commit_cid = match swap_commit {
            Some(swap_commit) => Some(Cid::from_str(&swap_commit)?),
            None => None,
        };
        let write = prepare_create(PrepareCreateOpts {
            did: did.clone(),
            collection: collection.clone(),
            record: serde_json::from_value(record)?,
            rkey,
            validate,
            swap_cid: None,
        })
        .await?;

        let did: &String = &did;
        let mut actor_store = ActorStore::from_actor_pools(did, &actor_pools).await;
        let backlink_conflicts: Vec<AtUri> = match validate {
            Some(true) => {
                let write_at_uri: AtUri = write.uri.clone().try_into()?;
                actor_store
                    .record
                    .get_backlink_conflicts(&write_at_uri, &write.record)
                    .await?
            }
            _ => Vec::new(),
        };

        let backlink_deletions: Vec<PreparedDelete> = backlink_conflicts
            .iter()
            .map(|at_uri| {
                prepare_delete(PrepareDeleteOpts {
                    did: at_uri.get_hostname().to_string(),
                    collection: at_uri.get_collection(),
                    rkey: at_uri.get_rkey(),
                    swap_cid: None,
                })
            })
            .collect::<Result<Vec<PreparedDelete>>>()?;
        let mut writes: Vec<PreparedWrite> = vec![PreparedWrite::Create(write.clone())];
        for delete in backlink_deletions {
            writes.push(PreparedWrite::Delete(delete));
        }
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

        Ok(CreateRecordOutput {
            uri: write.uri.clone(),
            cid: write.cid.to_string(),
        })
    } else {
        bail!("Could not find repo: `{repo}`")
    }
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
#[axum::debug_handler(state = AppState)]
pub async fn create_record(
    user: AuthenticatedUser,
    State(db_actors): State<HashMap<String, ActorStorage, RandomState>>,
    State(account_manager): State<Arc<RwLock<AccountManager>>>,
    State(sequencer): State<Arc<RwLock<Sequencer>>>,
    Json(body): Json<CreateRecordInput>,
) -> Result<Json<CreateRecordOutput>, ApiError> {
    tracing::debug!("@LOG: debug create_record {body:#?}");
    match inner_create_record(body, user, sequencer, db_actors, account_manager).await {
        Ok(res) => Ok(Json(res)),
        Err(error) => {
            tracing::error!("@LOG: ERROR: {error}");
            Err(ApiError::RuntimeError)
        }
    }
}
