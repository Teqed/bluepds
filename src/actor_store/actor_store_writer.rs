use std::sync::Arc;

use super::{ActorDb, ActorStoreResources};
use crate::SigningKey;

pub(crate) struct ActorStoreWriter {
    pub(crate) did: String,
    pub(crate) db: ActorDb,
    pub(crate) keypair: Arc<SigningKey>,
    pub(crate) resources: ActorStoreResources,
}
