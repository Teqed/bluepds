use crate::repo::types::BlobStore;

use super::blob::BackgroundQueue;
pub(crate) struct ActorStoreResources {
    pub(crate) blobstore: fn(did: String) -> BlobStore,
    pub(crate) background_queue: BackgroundQueue,
    pub(crate) reserved_key_dir: Option<String>,
}
impl ActorStoreResources {
    pub(crate) fn new(
        blobstore: fn(did: String) -> BlobStore,
        background_queue: BackgroundQueue,
        reserved_key_dir: Option<String>,
    ) -> Self {
        Self {
            blobstore,
            background_queue,
            reserved_key_dir,
        }
    }

    pub(crate) fn blobstore(&self, did: String) -> BlobStore {
        (self.blobstore)(did)
    }
}
