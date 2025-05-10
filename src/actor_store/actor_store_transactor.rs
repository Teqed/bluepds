use std::sync::Arc;

use sqlx::SqlitePool;

use super::resources::ActorStoreResources;
use crate::SigningKey;

pub(crate) struct ActorStoreTransactor {
    pub(crate) did: String,
    pub(crate) db: SqlitePool,
    pub(crate) keypair: Arc<SigningKey>,
    pub(crate) resources: ActorStoreResources,
}

impl ActorStoreTransactor {
    pub(crate) fn new(
        did: String,
        db: SqlitePool,
        keypair: Arc<SigningKey>,
        resources: ActorStoreResources,
    ) -> Self {
        Self {
            did,
            db,
            keypair,
            resources,
        }
    }
}
