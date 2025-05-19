//!
use rsky_lexicon::com::atproto::repo::ListMissingBlobsOutput;
use rsky_pds::actor_store::blob::ListMissingBlobsOpts;

use super::*;

///
#[tracing::instrument(skip_all)]
#[axum::debug_handler(state = AppState)]
pub async fn list_missing_blobs(
    user: AuthenticatedUser,
    Query(input): Query<atrium_repo::list_missing_blobs::ParametersData>,
    State(actor_pools): State<HashMap<String, ActorStorage, RandomState>>,
) -> Result<Json<ListMissingBlobsOutput>, ApiError> {
    let cursor = input.cursor;
    let limit = input.limit;
    let limit: Option<u16> = Some(limit.unwrap().into());
    // let did = auth.access.credentials.unwrap().did.unwrap();
    let did = user.did();
    let limit: u16 = limit.unwrap_or(500);

    let actor_store = ActorStore::from_actor_pools(&did, &actor_pools).await;

    match actor_store
        .blob
        .list_missing_blobs(ListMissingBlobsOpts { cursor, limit })
        .await
    {
        Ok(blobs) => {
            let cursor = match blobs.last() {
                Some(last_blob) => Some(last_blob.cid.clone()),
                None => None,
            };
            Ok(Json(ListMissingBlobsOutput { cursor, blobs }))
        }
        Err(error) => {
            tracing::error!("{error:?}");
            Err(ApiError::RuntimeError)
        }
    }
}
