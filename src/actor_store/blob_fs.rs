//! File system implementation of blob storage
//! Based on the S3 implementation but using local file system instead
use anyhow::Result;
use std::str::FromStr;
use cidv10::Cid;
use rsky_common::get_random_str;
use rsky_repo::error::BlobError;
use std::path::{Path, PathBuf};
use tokio::fs as async_fs;
use tokio::io::AsyncWriteExt;
use tracing::{debug, error, warn};

/// ByteStream implementation for blob data
pub struct ByteStream {
    pub bytes: Vec<u8>,
}

impl ByteStream {
    /// Create a new ByteStream with the given bytes
    pub const fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    /// Collect the bytes from the stream
    pub async fn collect(self) -> Result<Vec<u8>> {
        Ok(self.bytes)
    }
}

/// Path information for moving a blob
struct MoveObject {
    from: PathBuf,
    to: PathBuf,
}

/// File system implementation of blob storage
pub struct BlobStoreFs {
    /// Base directory for storing blobs
    pub base_dir: PathBuf,
    /// DID of the actor
    pub did: String,
}

impl BlobStoreFs {
    /// Create a new file system blob store for the given DID and base directory
    pub fn new(did: String, base_dir: PathBuf) -> Self {
        Self { base_dir, did }
    }

    /// Create a factory function for blob stores
    pub fn creator(base_dir: PathBuf) -> Box<dyn Fn(String) -> BlobStoreFs> {
        let base_dir_clone = base_dir.clone();
        Box::new(move |did: String| BlobStoreFs::new(did, base_dir_clone.clone()))
    }

    /// Generate a random key for temporary storage
    fn gen_key(&self) -> String {
        get_random_str()
    }

    /// Get path to the temporary blob storage
    fn get_tmp_path(&self, key: &str) -> PathBuf {
        self.base_dir.join("tmp").join(&self.did).join(key)
    }

    /// Get path to the stored blob with appropriate sharding
    fn get_stored_path(&self, cid: Cid) -> PathBuf {
        let cid_str = cid.to_string();
        
        // Create two-level sharded structure based on CID
        // First 10 chars for level 1, next 10 chars for level 2
        let first_level = if cid_str.len() >= 10 {
            &cid_str[0..10]
        } else {
            &cid_str
        };
        
        let second_level = if cid_str.len() >= 20 {
            &cid_str[10..20]
        } else {
            "default"
        };
        
        self.base_dir
            .join("blocks")
            .join(&self.did)
            .join(first_level)
            .join(second_level)
            .join(&cid_str)
    }

    /// Get path to the quarantined blob
    fn get_quarantined_path(&self, cid: Cid) -> PathBuf {
        let cid_str = cid.to_string();
        self.base_dir.join("quarantine").join(&self.did).join(&cid_str)
    }

    /// Store a blob temporarily
    pub async fn put_temp(&self, bytes: Vec<u8>) -> Result<String> {
        let key = self.gen_key();
        let temp_path = self.get_tmp_path(&key);
        
        // Ensure the directory exists
        if let Some(parent) = temp_path.parent() {
            async_fs::create_dir_all(parent).await?;
        }
        
        // Write the temporary blob
        let mut file = async_fs::File::create(&temp_path).await?;
        file.write_all(&bytes).await?;
        file.flush().await?;
        
        debug!("Stored temp blob at: {:?}", temp_path);
        Ok(key)
    }

    /// Make a temporary blob permanent by moving it to the blob store
    pub async fn make_permanent(&self, key: String, cid: Cid) -> Result<()> {
        let already_has = self.has_stored(cid).await?;
        
        if !already_has {
            // Move the temporary blob to permanent storage
            self.move_object(MoveObject {
                from: self.get_tmp_path(&key),
                to: self.get_stored_path(cid),
            }).await?;
            debug!("Moved temp blob to permanent: {} -> {}", key, cid);
        } else {
            // Already saved, so just delete the temp
            let temp_path = self.get_tmp_path(&key);
            if temp_path.exists() {
                async_fs::remove_file(temp_path).await?;
                debug!("Deleted temp blob as permanent already exists: {}", key);
            }
        }
        
        Ok(())
    }

    /// Store a blob directly as permanent
    pub async fn put_permanent(&self, cid: Cid, bytes: Vec<u8>) -> Result<()> {
        let target_path = self.get_stored_path(cid);
        
        // Ensure the directory exists
        if let Some(parent) = target_path.parent() {
            async_fs::create_dir_all(parent).await?;
        }
        
        // Write the blob
        let mut file = async_fs::File::create(&target_path).await?;
        file.write_all(&bytes).await?;
        file.flush().await?;
        
        debug!("Stored permanent blob: {}", cid);
        Ok(())
    }

    /// Quarantine a blob by moving it to the quarantine area
    pub async fn quarantine(&self, cid: Cid) -> Result<()> {
        self.move_object(MoveObject {
            from: self.get_stored_path(cid),
            to: self.get_quarantined_path(cid),
        }).await?;
        
        debug!("Quarantined blob: {}", cid);
        Ok(())
    }

    /// Unquarantine a blob by moving it back to regular storage
    pub async fn unquarantine(&self, cid: Cid) -> Result<()> {
        self.move_object(MoveObject {
            from: self.get_quarantined_path(cid),
            to: self.get_stored_path(cid),
        }).await?;
        
        debug!("Unquarantined blob: {}", cid);
        Ok(())
    }

    /// Get a blob as a stream
    async fn get_object(&self, cid: Cid) -> Result<ByteStream> {
        let blob_path = self.get_stored_path(cid);
        
        match async_fs::read(&blob_path).await {
            Ok(bytes) => Ok(ByteStream::new(bytes)),
            Err(e) => {
                error!("Failed to read blob at path {:?}: {}", blob_path, e);
                Err(anyhow::Error::new(BlobError::BlobNotFoundError))
            }
        }
    }

    /// Get blob bytes
    pub async fn get_bytes(&self, cid: Cid) -> Result<Vec<u8>> {
        let stream = self.get_object(cid).await?;
        stream.collect().await
    }

    /// Get a blob as a stream
    pub async fn get_stream(&self, cid: Cid) -> Result<ByteStream> {
        self.get_object(cid).await
    }

    /// Delete a blob by CID string
    pub async fn delete(&self, cid_str: String) -> Result<()> {
        match Cid::from_str(&cid_str) {
            Ok(cid) => self.delete_path(self.get_stored_path(cid)).await,
            Err(e) => {
                warn!("Invalid CID: {} - {}", cid_str, e);
                Err(anyhow::anyhow!("Invalid CID: {}", e))
            }
        }
    }

    /// Delete multiple blobs by CID
    pub async fn delete_many(&self, cids: Vec<Cid>) -> Result<()> {
        let mut futures = Vec::with_capacity(cids.len());
        
        for cid in cids {
            futures.push(self.delete_path(self.get_stored_path(cid)));
        }
        
        // Execute all delete operations concurrently
        let results = futures::future::join_all(futures).await;
        
        // Count errors but don't fail the operation
        let error_count = results.iter().filter(|r| r.is_err()).count();
        if error_count > 0 {
            warn!("{} errors occurred while deleting {} blobs", error_count, results.len());
        }
        
        Ok(())
    }

    /// Check if a blob is stored in the regular storage
    pub async fn has_stored(&self, cid: Cid) -> Result<bool> {
        let blob_path = self.get_stored_path(cid);
        Ok(blob_path.exists())
    }

    /// Check if a temporary blob exists
    pub async fn has_temp(&self, key: String) -> Result<bool> {
        let temp_path = self.get_tmp_path(&key);
        Ok(temp_path.exists())
    }

    /// Helper function to delete a file at the given path
    async fn delete_path(&self, path: PathBuf) -> Result<()> {
        if path.exists() {
            async_fs::remove_file(&path).await?;
            debug!("Deleted file at: {:?}", path);
            Ok(())
        } else {
            Err(anyhow::Error::new(BlobError::BlobNotFoundError))
        }
    }

    /// Move a blob from one path to another
    async fn move_object(&self, mov: MoveObject) -> Result<()> {
        // Ensure the source exists
        if !mov.from.exists() {
            return Err(anyhow::Error::new(BlobError::BlobNotFoundError));
        }
        
        // Ensure the target directory exists
        if let Some(parent) = mov.to.parent() {
            async_fs::create_dir_all(parent).await?;
        }
        
        // Copy first, then delete source after success
        async_fs::copy(&mov.from, &mov.to).await?;
        async_fs::remove_file(&mov.from).await?;
        
        debug!("Moved blob: {:?} -> {:?}", mov.from, mov.to);
        Ok(())
    }
}