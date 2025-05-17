use atrium_api::com::atproto::repo;
use axum::{Router, routing::post};
use constcat::concat;

use crate::serve::AppState;

pub mod apply_writes;
// pub mod create_record;
// pub mod delete_record;
// pub mod describe_repo;
// pub mod get_record;
// pub mod import_repo;
// pub mod list_missing_blobs;
// pub mod list_records;
// pub mod put_record;
// pub mod upload_blob;

/// These endpoints are part of the atproto PDS repository management APIs. \
/// Requests usually require authentication (unlike the com.atproto.sync.* endpoints), and are made directly to the user's own PDS instance.
/// ### Routes
/// - AP /xrpc/com.atproto.repo.applyWrites     -> [`apply_writes`]
/// - AP /xrpc/com.atproto.repo.createRecord    -> [`create_record`]
/// - AP /xrpc/com.atproto.repo.putRecord       -> [`put_record`]
/// - AP /xrpc/com.atproto.repo.deleteRecord    -> [`delete_record`]
/// - AP /xrpc/com.atproto.repo.uploadBlob      -> [`upload_blob`]
/// - UG /xrpc/com.atproto.repo.describeRepo    -> [`describe_repo`]
/// - UG /xrpc/com.atproto.repo.getRecord       -> [`get_record`]
/// - UG /xrpc/com.atproto.repo.listRecords     -> [`list_records`]
///     - [ ] xx /xrpc/com.atproto.repo.importRepo
// - [ ] xx /xrpc/com.atproto.repo.listMissingBlobs
pub(crate) fn routes() -> Router<AppState> {
    Router::new().route(
        concat!("/", repo::apply_writes::NSID),
        post(apply_writes::apply_writes),
    )
    // .route(concat!("/", repo::create_record::NSID), post(create_record))
    // .route(concat!("/", repo::put_record::NSID), post(put_record))
    // .route(concat!("/", repo::delete_record::NSID), post(delete_record))
    // .route(concat!("/", repo::upload_blob::NSID), post(upload_blob))
    // .route(concat!("/", repo::describe_repo::NSID), get(describe_repo))
    // .route(concat!("/", repo::get_record::NSID), get(get_record))
    // .route(concat!("/", repo::import_repo::NSID), post(todo))
    // .route(concat!("/", repo::list_missing_blobs::NSID), get(todo))
    // .route(concat!("/", repo::list_records::NSID), get(list_records))
}
