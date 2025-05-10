use std::sync::Arc;

use sqlx::SqlitePool;

use super::resources::ActorStoreResources;
use crate::SigningKey;

pub(crate) struct ActorStoreWriter {
    pub(crate) did: String,
    pub(crate) db: SqlitePool,
    pub(crate) keypair: Arc<SigningKey>,
    pub(crate) resources: ActorStoreResources,
}
