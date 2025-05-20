//! Returns a list of missing blobs for the requesting account. Intended to be used in the account migration flow.
use rsky_lexicon::com::atproto::repo::ListMissingBlobsOutput;
use rsky_pds::actor_store::blob::ListMissingBlobsOpts;

use super::*;

/// Returns a list of missing blobs for the requesting account. Intended to be used in the account migration flow.
/// Request
/// Query Parameters
///     limit    integer
///         Possible values: >= 1 and <= 1000
///         Default value: 500
///     cursor   string
/// Responses
/// cursor      string
/// blobs       object[]
#[tracing::instrument(skip_all)]
#[axum::debug_handler(state = AppState)]
pub async fn list_missing_blobs(
    user: AuthenticatedUser,
    Query(input): Query<atrium_repo::list_missing_blobs::ParametersData>,
    State(actor_pools): State<HashMap<String, ActorStorage, RandomState>>,
) -> Result<Json<ListMissingBlobsOutput>, ApiError> {
    let cursor = input.cursor;
    let limit = input.limit;
    let default_limit: atrium_api::types::LimitedNonZeroU16<1000> =
        atrium_api::types::LimitedNonZeroU16::try_from(500).expect("default limit");
    let limit: u16 = limit.unwrap_or(default_limit).into();
    // let did = auth.access.credentials.unwrap().did.unwrap();
    let did = user.did();

    let actor_store = ActorStore::from_actor_pools(&did, &actor_pools).await;

    match actor_store
        .blob
        .list_missing_blobs(ListMissingBlobsOpts { cursor, limit })
        .await
    {
        Ok(blobs) => {
            let cursor = blobs.last().map(|last_blob| last_blob.cid.clone());
            Ok(Json(ListMissingBlobsOutput { cursor, blobs }))
        }
        Err(error) => {
            tracing::error!("{error:?}");
            Err(ApiError::RuntimeError)
        }
    }
}
