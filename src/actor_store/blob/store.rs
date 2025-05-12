use anyhow::Result;
use atrium_repo::Cid;
use std::fmt::Debug;

/// BlobStream
/// A stream of blob data.
pub(crate) struct BlobStream(Box<dyn std::io::Read + Send>);
impl Debug for BlobStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobStream").finish()
    }
}

pub(crate) struct BlobStorePlaceholder;
pub(crate) trait BlobStore {
    async fn put_temp(&self, bytes: &[u8]) -> Result<String>; // bytes: Uint8Array | stream.Readable
    async fn make_permanent(&self, key: &str, cid: Cid) -> Result<()>;
    async fn put_permanent(&self, cid: Cid, bytes: &[u8]) -> Result<()>;
    async fn quarantine(&self, cid: Cid) -> Result<()>;
    async fn unquarantine(&self, cid: Cid) -> Result<()>;
    async fn get_bytes(&self, cid: Cid) -> Result<Vec<u8>>;
    async fn get_stream(&self, cid: Cid) -> Result<BlobStream>; // Promise<stream.Readable>
    async fn has_temp(&self, key: &str) -> Result<bool>;
    async fn has_stored(&self, cid: Cid) -> Result<bool>;
    async fn delete(&self, cid: Cid) -> Result<()>;
    async fn delete_many(&self, cids: Vec<Cid>) -> Result<()>;
}
impl BlobStore for BlobStorePlaceholder {
    async fn put_temp(&self, bytes: &[u8]) -> Result<String> {
        // Implementation here
        todo!();
        Ok("temp_key".to_string())
    }

    async fn make_permanent(&self, key: &str, cid: Cid) -> Result<()> {
        // Implementation here
        todo!();
        Ok(())
    }

    async fn put_permanent(&self, cid: Cid, bytes: &[u8]) -> Result<()> {
        // Implementation here
        todo!();
        Ok(())
    }

    async fn quarantine(&self, cid: Cid) -> Result<()> {
        // Implementation here
        todo!();
        Ok(())
    }

    async fn unquarantine(&self, cid: Cid) -> Result<()> {
        // Implementation here
        todo!();
        Ok(())
    }

    async fn get_bytes(&self, cid: Cid) -> Result<Vec<u8>> {
        // Implementation here
        todo!();
        Ok(vec![])
    }

    async fn get_stream(&self, cid: Cid) -> Result<BlobStream> {
        // Implementation here
        todo!();
        Ok(BlobStream(Box::new(std::io::empty())))
    }

    async fn has_temp(&self, key: &str) -> Result<bool> {
        // Implementation here
        todo!();
        Ok(true)
    }

    async fn has_stored(&self, cid: Cid) -> Result<bool> {
        // Implementation here
        todo!();
        Ok(true)
    }
    async fn delete(&self, cid: Cid) -> Result<()> {
        // Implementation here
        todo!();
        Ok(())
    }
    async fn delete_many(&self, cids: Vec<Cid>) -> Result<()> {
        // Implementation here
        todo!();
        Ok(())
    }
}
