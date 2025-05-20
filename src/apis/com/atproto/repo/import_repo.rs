use axum::{body::Bytes, http::HeaderMap};
use reqwest::header;
use rsky_common::env::env_int;
use rsky_repo::block_map::BlockMap;
use rsky_repo::car::{CarWithRoot, read_stream_car_with_root};
use rsky_repo::parse::get_and_parse_record;
use rsky_repo::repo::Repo;
use rsky_repo::sync::consumer::{VerifyRepoInput, verify_diff};
use rsky_repo::types::{RecordWriteDescript, VerifiedDiff};
use ubyte::ToByteUnit;

use super::*;

async fn from_data(bytes: Bytes) -> Result<CarWithRoot, ApiError> {
    let max_import_size = env_int("IMPORT_REPO_LIMIT").unwrap_or(100).megabytes();
    if bytes.len() > max_import_size {
        return Err(ApiError::InvalidRequest(format!(
            "Content-Length is greater than maximum of {max_import_size}"
        )));
    }

    let mut cursor = std::io::Cursor::new(bytes);
    match read_stream_car_with_root(&mut cursor).await {
        Ok(car_with_root) => Ok(car_with_root),
        Err(error) => {
            tracing::error!("Error reading stream car with root\n{error}");
            Err(ApiError::InvalidRequest("Invalid CAR file".to_owned()))
        }
    }
}

#[tracing::instrument(skip_all)]
#[axum::debug_handler(state = AppState)]
/// Import a repo in the form of a CAR file. Requires Content-Length HTTP header to be set.
/// Request
/// mime     application/vnd.ipld.car
/// Body - required
pub async fn import_repo(
    // auth: AccessFullImport,
    auth: AuthenticatedUser,
    headers: HeaderMap,
    State(actor_pools): State<HashMap<String, ActorStorage, RandomState>>,
    body: Bytes,
) -> Result<(), ApiError> {
    // let requester = auth.access.credentials.unwrap().did.unwrap();
    let requester = auth.did();
    let mut actor_store = ActorStore::from_actor_pools(&requester, &actor_pools).await;

    // Check headers
    let content_length = headers
        .get(header::CONTENT_LENGTH)
        .expect("no content length provided")
        .to_str()
        .map_err(anyhow::Error::from)
        .and_then(|content_length| content_length.parse::<u64>().map_err(anyhow::Error::from))
        .expect("invalid content-length header");
    if content_length > env_int("IMPORT_REPO_LIMIT").unwrap_or(100).megabytes() {
        return Err(ApiError::InvalidRequest(format!(
            "Content-Length is greater than maximum of {}",
            env_int("IMPORT_REPO_LIMIT").unwrap_or(100).megabytes()
        )));
    };

    // Get current repo if it exists
    let curr_root: Option<Cid> = actor_store.get_repo_root().await;
    let curr_repo: Option<Repo> = match curr_root {
        None => None,
        Some(_root) => Some(Repo::load(actor_store.storage.clone(), curr_root).await?),
    };

    // Process imported car
    // let car_with_root = import_repo_input.car_with_root;
    let car_with_root: CarWithRoot = match from_data(body).await {
        Ok(car) => car,
        Err(error) => {
            tracing::error!("Error importing repo\n{error:?}");
            return Err(ApiError::InvalidRequest("Invalid CAR file".to_owned()));
        }
    };

    // Get verified difference from current repo and imported repo
    let mut imported_blocks: BlockMap = car_with_root.blocks;
    let imported_root: Cid = car_with_root.root;
    let opts = VerifyRepoInput {
        ensure_leaves: Some(false),
    };

    let diff: VerifiedDiff = match verify_diff(
        curr_repo,
        &mut imported_blocks,
        imported_root,
        None,
        None,
        Some(opts),
    )
    .await
    {
        Ok(res) => res,
        Err(error) => {
            tracing::error!("{:?}", error);
            return Err(ApiError::RuntimeError);
        }
    };

    let commit_data = diff.commit;
    let prepared_writes: Vec<PreparedWrite> =
        prepare_import_repo_writes(requester, diff.writes, &imported_blocks).await?;
    match actor_store
        .process_import_repo(commit_data, prepared_writes)
        .await
    {
        Ok(_res) => {}
        Err(error) => {
            tracing::error!("Error importing repo\n{error}");
            return Err(ApiError::RuntimeError);
        }
    }

    Ok(())
}

/// Converts list of RecordWriteDescripts into a list of PreparedWrites
async fn prepare_import_repo_writes(
    did: String,
    writes: Vec<RecordWriteDescript>,
    blocks: &BlockMap,
) -> Result<Vec<PreparedWrite>, ApiError> {
    match stream::iter(writes)
        .then(|write| {
            let did = did.clone();
            async move {
                Ok::<PreparedWrite, anyhow::Error>(match write {
                    RecordWriteDescript::Create(write) => {
                        let parsed_record = get_and_parse_record(blocks, write.cid)?;
                        PreparedWrite::Create(
                            prepare_create(PrepareCreateOpts {
                                did: did.clone(),
                                collection: write.collection,
                                rkey: Some(write.rkey),
                                swap_cid: None,
                                record: parsed_record.record,
                                validate: Some(true),
                            })
                            .await?,
                        )
                    }
                    RecordWriteDescript::Update(write) => {
                        let parsed_record = get_and_parse_record(blocks, write.cid)?;
                        PreparedWrite::Update(
                            prepare_update(PrepareUpdateOpts {
                                did: did.clone(),
                                collection: write.collection,
                                rkey: write.rkey,
                                swap_cid: None,
                                record: parsed_record.record,
                                validate: Some(true),
                            })
                            .await?,
                        )
                    }
                    RecordWriteDescript::Delete(write) => {
                        PreparedWrite::Delete(prepare_delete(PrepareDeleteOpts {
                            did: did.clone(),
                            collection: write.collection,
                            rkey: write.rkey,
                            swap_cid: None,
                        })?)
                    }
                })
            }
        })
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<PreparedWrite>, _>>()
    {
        Ok(res) => Ok(res),
        Err(error) => {
            tracing::error!("Error preparing import repo writes\n{error}");
            Err(ApiError::RuntimeError)
        }
    }
}
