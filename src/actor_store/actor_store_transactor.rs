use std::sync::Arc;

use super::{ActorDb, resources::ActorStoreResources};
use crate::SigningKey;

pub(crate) struct ActorStoreTransactor {
    pub(crate) did: String,
    pub(crate) db: ActorDb,
    pub(crate) keypair: Arc<SigningKey>,
    pub(crate) resources: ActorStoreResources,
}

impl ActorStoreTransactor {
    pub(crate) fn new(
        did: String,
        db: ActorDb,
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
