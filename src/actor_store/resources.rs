
use std::sync::Arc;

use crate::config::{BlobConfig, RepoConfig};

pub(crate) struct ActorStoreResources {
    pub(crate) config: RepoConfig,
    pub(crate) blob_config: BlobConfig,
    pub(crate) background_queue: Arc<()>, // Placeholder
}

impl Clone for ActorStoreResources {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            blob_config: self.blob_config.clone(),
            background_queue: self.background_queue.clone(),
        }
    }
}