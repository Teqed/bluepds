//! List a range of records in a repository, matching a specific collection. Does not require auth.
use super::*;

// #[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
// #[serde(rename_all = "camelCase")]
// /// Parameters for [`list_records`].
// pub(super) struct ListRecordsParameters {
//     ///The NSID of the record type.
//     pub collection: Nsid,
//     /// The cursor to start from.
//     #[serde(skip_serializing_if = "core::option::Option::is_none")]
//     pub cursor: Option<String>,
//     ///The number of records to return.
//     #[serde(skip_serializing_if = "core::option::Option::is_none")]
//     pub limit: Option<String>,
//     ///The handle or DID of the repo.
//     pub repo: AtIdentifier,
//     ///Flag to reverse the order of the returned records.
//     #[serde(skip_serializing_if = "core::option::Option::is_none")]
//     pub reverse: Option<bool>,
//     ///DEPRECATED: The highest sort-ordered rkey to stop at (exclusive)
//     #[serde(skip_serializing_if = "core::option::Option::is_none")]
//     pub rkey_end: Option<String>,
//     ///DEPRECATED: The lowest sort-ordered rkey to start from (exclusive)
//     #[serde(skip_serializing_if = "core::option::Option::is_none")]
//     pub rkey_start: Option<String>,
// }

#[expect(non_snake_case, clippy::too_many_arguments)]
async fn inner_list_records(
    // The handle or DID of the repo.
    repo: String,
    // The NSID of the record type.
    collection: String,
    // The number of records to return.
    limit: u16,
    cursor: Option<String>,
    // DEPRECATED: The lowest sort-ordered rkey to start from (exclusive)
    rkeyStart: Option<String>,
    // DEPRECATED: The highest sort-ordered rkey to stop at (exclusive)
    rkeyEnd: Option<String>,
    // Flag to reverse the order of the returned records.
    reverse: bool,
    // The actor pools
    actor_pools: HashMap<String, ActorStorage>,
    account_manager: Arc<RwLock<AccountManager>>,
) -> Result<ListRecordsOutput> {
    if limit > 100 {
        bail!("Error: limit can not be greater than 100")
    }
    let did = account_manager
        .read()
        .await
        .get_did_for_actor(&repo, None)
        .await?;
    if let Some(did) = did {
        let mut actor_store = ActorStore::from_actor_pools(&did, &actor_pools).await;

        let records: Vec<Record> = actor_store
            .record
            .list_records_for_collection(
                collection,
                limit as i64,
                reverse,
                cursor,
                rkeyStart,
                rkeyEnd,
                None,
            )
            .await?
            .into_iter()
            .map(|record| {
                Ok(Record {
                    uri: record.uri.clone(),
                    cid: record.cid.clone(),
                    value: serde_json::to_value(record)?,
                })
            })
            .collect::<Result<Vec<Record>>>()?;

        let last_record = records.last();
        let cursor: Option<String>;
        if let Some(last_record) = last_record {
            let last_at_uri: AtUri = last_record.uri.clone().try_into()?;
            cursor = Some(last_at_uri.get_rkey());
        } else {
            cursor = None;
        }
        Ok(ListRecordsOutput { records, cursor })
    } else {
        bail!("Could not find repo: {repo}")
    }
}

/// List a range of records in a repository, matching a specific collection. Does not require auth.
/// - GET /xrpc/com.atproto.repo.listRecords
/// ### Query Parameters
/// - `repo`: `at-identifier` // The handle or DID of the repo.
/// - `collection`: `nsid` // The NSID of the record type.
/// - `limit`: `integer` // The maximum number of records to return. Default 50, >=1 and <=100.
/// - `cursor`: `string`
/// - `reverse`: `boolean` // Flag to reverse the order of the returned records.
/// ### Responses
/// - 200 OK: {"cursor": "string","records": [{"uri": "string","cid": "string","value": {}}]}
/// - 400 Bad Request: {error:[`InvalidRequest`, `ExpiredToken`, `InvalidToken`]}
/// - 401 Unauthorized
#[tracing::instrument(skip_all)]
#[allow(non_snake_case)]
#[axum::debug_handler(state = AppState)]
pub async fn list_records(
    Query(input): Query<atrium_repo::list_records::ParametersData>,
    State(actor_pools): State<HashMap<String, ActorStorage, RandomState>>,
    State(account_manager): State<Arc<RwLock<AccountManager>>>,
) -> Result<Json<ListRecordsOutput>, ApiError> {
    let repo = input.repo;
    let collection = input.collection;
    let limit: Option<u8> = input.limit.map(u8::from);
    let limit: Option<u16> = limit.map(|x| x.into());
    let cursor = input.cursor;
    let reverse = input.reverse;
    let rkeyStart = None;
    let rkeyEnd = None;

    let limit = limit.unwrap_or(50);
    let reverse = reverse.unwrap_or(false);

    match inner_list_records(
        repo.into(),
        collection.into(),
        limit,
        cursor,
        rkeyStart,
        rkeyEnd,
        reverse,
        actor_pools,
        account_manager,
    )
    .await
    {
        Ok(res) => Ok(Json(res)),
        Err(error) => {
            tracing::error!("@LOG: ERROR: {error}");
            Err(ApiError::RuntimeError)
        }
    }
}
