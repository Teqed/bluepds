//! Get information about an account and repository, including the list of collections. Does not require auth.
use super::*;

async fn inner_describe_repo(
    repo: String,
    id_resolver: Arc<RwLock<IdResolver>>,
    actor_pools: HashMap<String, ActorStorage>,
    account_manager: Arc<RwLock<AccountManager>>,
) -> Result<DescribeRepoOutput> {
    let account = account_manager
        .read()
        .await
        .get_account(&repo, None)
        .await?;
    match account {
        None => bail!("Cound not find user: `{repo}`"),
        Some(account) => {
            let did_doc: DidDocument = match id_resolver
                .write()
                .await
                .did
                .ensure_resolve(&account.did, None)
                .await
            {
                Err(err) => bail!("Could not resolve DID: `{err}`"),
                Ok(res) => res,
            };
            let handle = rsky_common::get_handle(&did_doc);
            let handle_is_correct = handle == account.handle;

            let actor_store =
                ActorStore::from_actor_pools(&account.did.clone(), &actor_pools).await;
            let collections = actor_store.record.list_collections().await?;

            Ok(DescribeRepoOutput {
                handle: account.handle.unwrap_or_else(|| INVALID_HANDLE.to_owned()),
                did: account.did,
                did_doc: serde_json::to_value(did_doc)?,
                collections,
                handle_is_correct,
            })
        }
    }
}

/// Get information about an account and repository, including the list of collections. Does not require auth.
/// - GET /xrpc/com.atproto.repo.describeRepo
/// ### Query Parameters
/// - `repo`: `at-identifier` // The handle or DID of the repo.
/// ### Responses
/// - 200 OK: {"handle": "string","did": "string","didDoc": {},"collections": [string],"handleIsCorrect": true} \
///   handeIsCorrect - boolean - Indicates if handle is currently valid (resolves bi-directionally)
/// - 400 Bad Request: {error:[`InvalidRequest`, `ExpiredToken`, `InvalidToken`]}
/// - 401 Unauthorized
#[tracing::instrument(skip_all)]
#[axum::debug_handler(state = AppState)]
pub async fn describe_repo(
    Query(input): Query<atrium_repo::describe_repo::ParametersData>,
    State(db_actors): State<HashMap<String, ActorStorage, RandomState>>,
    State(account_manager): State<Arc<RwLock<AccountManager>>>,
    State(id_resolver): State<Arc<RwLock<IdResolver>>>,
) -> Result<Json<DescribeRepoOutput>, ApiError> {
    match inner_describe_repo(input.repo.into(), id_resolver, db_actors, account_manager).await {
        Ok(res) => Ok(Json(res)),
        Err(error) => {
            tracing::error!("{error:?}");
            Err(ApiError::RuntimeError)
        }
    }
}
