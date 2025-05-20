//! Upload a new blob, to be referenced from a repository record.
use crate::config::AppConfig;
use anyhow::Context as _;
use axum::{
    body::Bytes,
    http::{self, HeaderMap},
};
use rsky_lexicon::com::atproto::repo::{Blob, BlobOutput};
use rsky_repo::types::{BlobConstraint, PreparedBlobRef};
// use rsky_common::BadContentTypeError;

use super::*;

async fn inner_upload_blob(
    auth: AuthenticatedUser,
    blob: Bytes,
    content_type: String,
    actor_pools: HashMap<String, ActorStorage>,
) -> Result<BlobOutput> {
    // let requester = auth.access.credentials.unwrap().did.unwrap();
    let requester = auth.did();

    let actor_store = ActorStore::from_actor_pools(&requester, &actor_pools).await;

    let metadata = actor_store
        .blob
        .upload_blob_and_get_metadata(content_type, blob)
        .await?;
    let blobref = actor_store.blob.track_untethered_blob(metadata).await?;

    // make the blob permanent if an associated record is already indexed
    let records_for_blob = actor_store
        .blob
        .get_records_for_blob(blobref.get_cid()?)
        .await?;

    if !records_for_blob.is_empty() {
        actor_store
            .blob
            .verify_blob_and_make_permanent(PreparedBlobRef {
                cid: blobref.get_cid()?,
                mime_type: blobref.get_mime_type().to_string(),
                constraints: BlobConstraint {
                    max_size: None,
                    accept: None,
                },
            })
            .await?;
    }

    Ok(BlobOutput {
        blob: Blob {
            r#type: Some("blob".to_owned()),
            r#ref: Some(blobref.get_cid()?),
            cid: None,
            mime_type: blobref.get_mime_type().to_string(),
            size: blobref.get_size(),
            original: None,
        },
    })
}

/// Upload a new blob, to be referenced from a repository record. \
/// The blob will be deleted if it is not referenced within a time window (eg, minutes). \
/// Blob restrictions (mimetype, size, etc) are enforced when the reference is created. \
/// Requires auth, implemented by PDS.
/// - POST /xrpc/com.atproto.repo.uploadBlob
/// ### Request Body
/// ### Responses
/// - 200 OK: {"blob": "binary"}
/// - 400 Bad Request: {error:[`InvalidRequest`, `ExpiredToken`, `InvalidToken`]}
/// - 401 Unauthorized
#[tracing::instrument(skip_all)]
#[axum::debug_handler(state = AppState)]
pub async fn upload_blob(
    auth: AuthenticatedUser,
    headers: HeaderMap,
    State(config): State<AppConfig>,
    State(actor_pools): State<HashMap<String, ActorStorage, RandomState>>,
    blob: Bytes,
) -> Result<Json<BlobOutput>, ApiError> {
    let content_length = headers
        .get(http::header::CONTENT_LENGTH)
        .context("no content length provided")?
        .to_str()
        .map_err(anyhow::Error::from)
        .and_then(|content_length| content_length.parse::<u64>().map_err(anyhow::Error::from))
        .context("invalid content-length header")?;
    let content_type = headers
        .get(http::header::CONTENT_TYPE)
        .context("no content-type provided")?
        .to_str()
        // .map_err(BadContentTypeError::MissingType)
        .context("invalid content-type provided")?
        .to_owned();

    if content_length > config.blob.limit {
        return Err(ApiError::InvalidRequest(format!(
            "Content-Length is greater than maximum of {}",
            config.blob.limit
        )));
    };
    if blob.len() as u64 > config.blob.limit {
        return Err(ApiError::InvalidRequest(format!(
            "Blob size is greater than maximum of {} despite content-length header",
            config.blob.limit
        )));
    };

    match inner_upload_blob(auth, blob, content_type, actor_pools).await {
        Ok(res) => Ok(Json(res)),
        Err(error) => {
            tracing::error!("{error:?}");
            Err(ApiError::RuntimeError)
        }
    }
}
