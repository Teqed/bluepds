//! Get a single record from a repository. Does not require auth.
use super::*;

async fn inner_get_record(
    repo: String,
    collection: String,
    rkey: String,
    cid: Option<String>,
    // req: ProxyRequest<'_>,
    actor_pools: HashMap<String, ActorStorage>,
    account_manager: Arc<RwLock<AccountManager>>,
) -> Result<GetRecordOutput> {
    let did = account_manager
        .read()
        .await
        .get_did_for_actor(&repo, None)
        .await?;

    // fetch from pds if available, if not then fetch from appview
    if let Some(did) = did {
        let uri = AtUri::make(did.clone(), Some(collection), Some(rkey))?;

        let mut actor_store = ActorStore::from_actor_pools(&did, &actor_pools).await;

        match actor_store.record.get_record(&uri, cid, None).await {
            Ok(Some(record)) if record.takedown_ref.is_none() => Ok(GetRecordOutput {
                uri: uri.to_string(),
                cid: Some(record.cid),
                value: serde_json::to_value(record.value)?,
            }),
            _ => bail!("Could not locate record: `{uri}`"),
        }
    } else {
        // match req.cfg.bsky_app_view {
        //     None => bail!("Could not locate record"),
        //     Some(_) => match pipethrough(
        //         &req,
        //         None,
        //         OverrideOpts {
        //             aud: None,
        //             lxm: None,
        //         },
        //     )
        //     .await
        //     {
        //         Err(error) => {
        //             tracing::error!("@LOG: ERROR: {error}");
        bail!("Could not locate record")
        //         }
        //         Ok(res) => {
        //             let output: GetRecordOutput = serde_json::from_slice(res.buffer.as_slice())?;
        //             Ok(output)
        //         }
        //     },
        // }
    }
}

/// Get a single record from a repository. Does not require auth.
/// - GET /xrpc/com.atproto.repo.getRecord
/// ### Query Parameters
/// - `repo`: `at-identifier` // The handle or DID of the repo.
/// - `collection`: `nsid` // The NSID of the record collection.
/// - `rkey`: `string` // The record key. <= 512 characters.
/// - `cid`: `cid` // The CID of the version of the record. If not specified, then return the most recent version.
/// ### Responses
/// - 200 OK: {"uri": "string","cid": "string","value": {}}
/// - 400 Bad Request: {error:[`InvalidRequest`, `ExpiredToken`, `InvalidToken`, `RecordNotFound`]}
/// - 401 Unauthorized
#[tracing::instrument(skip_all)]
#[axum::debug_handler(state = AppState)]
pub async fn get_record(
    Query(input): Query<ParametersData>,
    State(db_actors): State<HashMap<String, ActorStorage, RandomState>>,
    State(account_manager): State<Arc<RwLock<AccountManager>>>,
) -> Result<Json<GetRecordOutput>, ApiError> {
    let repo = input.repo;
    let collection = input.collection;
    let rkey = input.rkey;
    let cid = input.cid;
    // let req: ProxyRequest = todo!(); // TODO: Implement service proxy
    match inner_get_record(
        repo,
        collection,
        rkey,
        cid,
        // req,
        db_actors,
        account_manager,
    )
    .await
    {
        Ok(res) => Ok(Json(res)),
        Err(error) => {
            tracing::error!("@LOG: ERROR: {error}");
            Err(ApiError::RecordNotFound)
        }
    }
}

#[derive(serde::Deserialize, Debug)]
pub struct ParametersData {
    pub cid: Option<String>,
    pub collection: String,
    pub repo: String,
    pub rkey: String,
}
