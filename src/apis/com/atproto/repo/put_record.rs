//! Write a repository record, creating or updating it as needed. Requires auth, implemented by PDS.
use anyhow::bail;
use rsky_lexicon::com::atproto::repo::{PutRecordInput, PutRecordOutput};
use rsky_repo::types::CommitDataWithOps;

use super::*;

#[tracing::instrument(skip_all)]
async fn inner_put_record(
    body: PutRecordInput,
    auth: AuthenticatedUser,
    sequencer: Arc<RwLock<Sequencer>>,
    actor_pools: HashMap<String, ActorStorage>,
    account_manager: Arc<RwLock<AccountManager>>,
) -> Result<PutRecordOutput> {
    let PutRecordInput {
        repo,
        collection,
        rkey,
        validate,
        record,
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
    if let Some(account) = account {
        if account.deactivated_at.is_some() {
            bail!("Account is deactivated")
        }
        let did = account.did;
        // if did != auth.access.credentials.unwrap().did.unwrap() {
        if did != auth.did() {
            bail!("AuthRequiredError")
        }
        let uri = AtUri::make(did.clone(), Some(collection.clone()), Some(rkey.clone()))?;
        let swap_commit_cid = match swap_commit {
            Some(swap_commit) => Some(Cid::from_str(&swap_commit)?),
            None => None,
        };
        let swap_record_cid = match swap_record {
            Some(swap_record) => Some(Cid::from_str(&swap_record)?),
            None => None,
        };
        let (commit, write): (Option<CommitDataWithOps>, PreparedWrite) = {
            let mut actor_store = ActorStore::from_actor_pools(&did, &actor_pools).await;

            let current = actor_store
                .record
                .get_record(&uri, None, Some(true))
                .await?;
            tracing::debug!("@LOG: debug inner_put_record, current: {current:?}");
            let write: PreparedWrite = if current.is_some() {
                PreparedWrite::Update(
                    prepare_update(PrepareUpdateOpts {
                        did: did.clone(),
                        collection,
                        rkey,
                        swap_cid: swap_record_cid,
                        record: serde_json::from_value(record)?,
                        validate,
                    })
                    .await?,
                )
            } else {
                PreparedWrite::Create(
                    prepare_create(PrepareCreateOpts {
                        did: did.clone(),
                        collection,
                        rkey: Some(rkey),
                        swap_cid: swap_record_cid,
                        record: serde_json::from_value(record)?,
                        validate,
                    })
                    .await?,
                )
            };

            match current {
                Some(current) if current.cid == write.cid().unwrap().to_string() => (None, write),
                _ => {
                    let commit = actor_store
                        .process_writes(vec![write.clone()], swap_commit_cid)
                        .await?;
                    (Some(commit), write)
                }
            }
        };

        if let Some(commit) = commit {
            sequencer
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
        }
        Ok(PutRecordOutput {
            uri: write.uri().to_string(),
            cid: write.cid().unwrap().to_string(),
        })
    } else {
        bail!("Could not find repo: `{repo}`")
    }
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
#[tracing::instrument(skip_all)]
pub async fn put_record(
    auth: AuthenticatedUser,
    State(sequencer): State<Arc<RwLock<Sequencer>>>,
    State(actor_pools): State<HashMap<String, ActorStorage, RandomState>>,
    State(account_manager): State<Arc<RwLock<AccountManager>>>,
    Json(body): Json<PutRecordInput>,
) -> Result<Json<PutRecordOutput>, ApiError> {
    tracing::debug!("@LOG: debug put_record {body:#?}");
    match inner_put_record(body, auth, sequencer, actor_pools, account_manager).await {
        Ok(res) => Ok(Json(res)),
        Err(error) => {
            tracing::error!("@LOG: ERROR: {error}");
            Err(ApiError::RuntimeError)
        }
    }
}
