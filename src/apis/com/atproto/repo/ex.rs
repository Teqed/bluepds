//!
use crate::account_manager::AccountManager;
use crate::serve::ActorStorage;
use crate::{actor_store::ActorStore, error::ApiError, serve::AppState};
use anyhow::{Result, bail};
use axum::extract::Query;
use axum::{Json, extract::State};
use rsky_identity::IdResolver;
use rsky_pds::sequencer::Sequencer;
use std::collections::HashMap;
use std::hash::RandomState;
use std::sync::Arc;
use tokio::sync::RwLock;

async fn fun(
    actor_pools: HashMap<String, ActorStorage>,
    account_manager: Arc<RwLock<AccountManager>>,
    id_resolver: Arc<RwLock<IdResolver>>,
    sequencer: Arc<RwLock<Sequencer>>,
) -> Result<_> {
    todo!();
}

///
#[tracing::instrument(skip_all)]
#[axum::debug_handler(state = AppState)]
pub async fn fun(
    auth: AuthenticatedUser,
    Query(input): Query<atrium_api::com::atproto::repo::describe_repo::ParametersData>,
    State(actor_pools): State<HashMap<String, ActorStorage, RandomState>>,
    State(account_manager): State<Arc<RwLock<AccountManager>>>,
    State(id_resolver): State<Arc<RwLock<IdResolver>>>,
    State(sequencer): State<Arc<RwLock<Sequencer>>>,
    Json(body): Json<ApplyWritesInput>,
) -> Result<Json<_>, ApiError> {
    todo!();
}
