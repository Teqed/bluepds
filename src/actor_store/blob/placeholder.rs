use anyhow::Result;
use atrium_repo::Cid;

use super::{BlobStore, BlobStream};

/// Placeholder implementation for blob store
#[derive(Clone)]
pub struct BlobStorePlaceholder;

impl BlobStore for BlobStorePlaceholder {
    async fn put_temp(&self, _bytes: &[u8]) -> Result<String> {
        todo!("BlobStorePlaceholder::put_temp not implemented");
    }

    async fn make_permanent(&self, _key: &str, _cid: Cid) -> Result<()> {
        todo!("BlobStorePlaceholder::make_permanent not implemented");
    }

    async fn put_permanent(&self, _cid: Cid, _bytes: &[u8]) -> Result<()> {
        todo!("BlobStorePlaceholder::put_permanent not implemented");
    }

    async fn quarantine(&self, _cid: Cid) -> Result<()> {
        todo!("BlobStorePlaceholder::quarantine not implemented");
    }

    async fn unquarantine(&self, _cid: Cid) -> Result<()> {
        todo!("BlobStorePlaceholder::unquarantine not implemented");
    }

    async fn get_bytes(&self, _cid: Cid) -> Result<Vec<u8>> {
        todo!("BlobStorePlaceholder::get_bytes not implemented");
    }

    async fn get_stream(&self, _cid: Cid) -> Result<BlobStream> {
        todo!("BlobStorePlaceholder::get_stream not implemented");
    }

    async fn has_temp(&self, _key: &str) -> Result<bool> {
        todo!("BlobStorePlaceholder::has_temp not implemented");
    }

    async fn has_stored(&self, _cid: Cid) -> Result<bool> {
        todo!("BlobStorePlaceholder::has_stored not implemented");
    }

    async fn delete(&self, _cid: Cid) -> Result<()> {
        todo!("BlobStorePlaceholder::delete not implemented");
    }

    async fn delete_many(&self, _cids: Vec<Cid>) -> Result<()> {
        todo!("BlobStorePlaceholder::delete_many not implemented");
    }
}
