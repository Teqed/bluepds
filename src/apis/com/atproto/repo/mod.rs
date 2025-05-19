use atrium_api::com::atproto::repo as atrium_repo;
use axum::{
    Router,
    routing::{get, post},
};
use constcat::concat;

pub mod apply_writes;
pub mod create_record;
pub mod delete_record;
pub mod describe_repo;
pub mod get_record;
pub mod import_repo;
pub mod list_missing_blobs;
pub mod list_records;
pub mod put_record;
pub mod upload_blob;

use crate::account_manager::AccountManager;
use crate::account_manager::helpers::account::AvailabilityFlags;
use crate::{
    actor_store::ActorStore,
    auth::AuthenticatedUser,
    error::ApiError,
    serve::{ActorStorage, AppState},
};
use anyhow::{Result, bail};
use axum::extract::Query;
use axum::{Json, extract::State};
use cidv10::Cid;
use futures::stream::{self, StreamExt};
use rsky_identity::IdResolver;
use rsky_identity::types::DidDocument;
use rsky_lexicon::com::atproto::repo::DeleteRecordInput;
use rsky_lexicon::com::atproto::repo::DescribeRepoOutput;
use rsky_lexicon::com::atproto::repo::GetRecordOutput;
use rsky_lexicon::com::atproto::repo::{ApplyWritesInput, ApplyWritesInputRefWrite};
use rsky_lexicon::com::atproto::repo::{CreateRecordInput, CreateRecordOutput};
use rsky_lexicon::com::atproto::repo::{ListRecordsOutput, Record};
// use rsky_pds::pipethrough::{OverrideOpts, ProxyRequest, pipethrough};
use rsky_pds::repo::prepare::{
    PrepareCreateOpts, PrepareDeleteOpts, PrepareUpdateOpts, prepare_create, prepare_delete,
    prepare_update,
};
use rsky_pds::sequencer::Sequencer;
use rsky_repo::types::PreparedDelete;
use rsky_repo::types::PreparedWrite;
use rsky_syntax::aturi::AtUri;
use rsky_syntax::handle::INVALID_HANDLE;
use std::collections::HashMap;
use std::hash::RandomState;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;

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
    Router::new()
        .route(
            concat!("/", atrium_repo::apply_writes::NSID),
            post(apply_writes::apply_writes),
        )
        .route(
            concat!("/", atrium_repo::create_record::NSID),
            post(create_record::create_record),
        )
        .route(
            concat!("/", atrium_repo::put_record::NSID),
            post(put_record::put_record),
        )
        .route(
            concat!("/", atrium_repo::delete_record::NSID),
            post(delete_record::delete_record),
        )
        .route(
            concat!("/", atrium_repo::upload_blob::NSID),
            post(upload_blob::upload_blob),
        )
        .route(
            concat!("/", atrium_repo::describe_repo::NSID),
            get(describe_repo::describe_repo),
        )
        .route(
            concat!("/", atrium_repo::get_record::NSID),
            get(get_record::get_record),
        )
        .route(
            concat!("/", atrium_repo::import_repo::NSID),
            post(import_repo::import_repo),
        )
        .route(
            concat!("/", atrium_repo::list_missing_blobs::NSID),
            get(list_missing_blobs::list_missing_blobs),
        )
        .route(
            concat!("/", atrium_repo::list_records::NSID),
            get(list_records::list_records),
        )
}
