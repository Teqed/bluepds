use std::path::PathBuf;
use std::sync::Arc;

use super::blob::{BackgroundQueue, BlobStorePlaceholder};

pub(crate) struct ActorStoreResources {
    // Factory function to create blobstore instances
    blobstore_factory: Arc<dyn Fn(String) -> BlobStorePlaceholder + Send + Sync>,
    // Shared background queue
    background_queue: Arc<BackgroundQueue>,
    // Optional directory for reserved keys
    reserved_key_dir: Option<PathBuf>,
}

impl ActorStoreResources {
    // Simple constructor with minimal parameters
    pub(crate) fn new(
        blobstore_factory: impl Fn(String) -> BlobStorePlaceholder + Send + Sync + 'static,
        concurrency: usize,
    ) -> Self {
        Self {
            blobstore_factory: Arc::new(blobstore_factory),
            background_queue: Arc::new(BackgroundQueue::new(concurrency)),
            reserved_key_dir: None,
        }
    }

    // Set reserved key directory
    pub(crate) fn with_reserved_key_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.reserved_key_dir = Some(dir.into());
        self
    }

    // Get a blobstore for a DID
    pub(crate) fn blobstore(&self, did: String) -> BlobStorePlaceholder {
        (self.blobstore_factory)(did)
    }

    // Get the background queue
    pub(crate) fn background_queue(&self) -> Arc<BackgroundQueue> {
        self.background_queue.clone()
    }

    // Get the reserved key directory
    pub(crate) fn reserved_key_dir(&self) -> Option<&PathBuf> {
        self.reserved_key_dir.as_ref()
    }
}
