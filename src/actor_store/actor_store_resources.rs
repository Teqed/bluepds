use super::blob::{BackgroundQueue, BlobStorePlaceholder};
pub(crate) struct ActorStoreResources {
    pub(crate) blobstore: fn(did: String) -> BlobStorePlaceholder,
    pub(crate) background_queue: BackgroundQueue,
    pub(crate) reserved_key_dir: Option<String>,
}
impl ActorStoreResources {
    pub(crate) fn new(
        blobstore: fn(did: String) -> BlobStorePlaceholder,
        background_queue: BackgroundQueue,
        reserved_key_dir: Option<String>,
    ) -> Self {
        Self {
            blobstore,
            background_queue,
            reserved_key_dir,
        }
    }

    pub(crate) fn blobstore(&self, did: String) -> BlobStorePlaceholder {
        (self.blobstore)(did)
    }
}
