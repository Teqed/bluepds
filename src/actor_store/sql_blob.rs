#![expect(
    clippy::pub_use,
    clippy::single_char_lifetime_names,
    unused_qualifications
)]
use std::{path::PathBuf, str::FromStr as _, sync::Arc};

use anyhow::{Context, Result};
use cidv10::Cid;
use diesel::*;
use rsky_common::get_random_str;
use tokio::fs;

use crate::db::DbConn;

/// Type for stream of blob data
pub type BlobStream = Box<dyn std::io::Read + Send>;

/// ByteStream implementation for blob data
pub struct ByteStream {
    pub bytes: Vec<u8>,
}

impl ByteStream {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    pub async fn collect(self) -> Result<Vec<u8>> {
        Ok(self.bytes)
    }
}

/// SQL-based implementation of blob storage
#[derive(Clone)]
pub struct BlobStoreSql {
    /// Database connection for metadata
    pub db: Arc<DbConn>,
    /// DID of the actor
    pub did: String,
    /// Path for blob storage
    pub path: PathBuf,
}

/// Configuration for the blob store
pub struct BlobConfig {
    /// Base path for blob storage
    pub path: PathBuf,
}

/// Represents a move operation for blobs
struct MoveObject {
    from: String,
    to: String,
}

/// Blob table structure for SQL operations
#[derive(Queryable, Insertable, Debug)]
#[diesel(table_name = blobs)]
struct BlobEntry {
    cid: String,
    did: String,
    path: String,
    size: i32,
    mime_type: String,
    quarantined: bool,
    temp: bool,
}

// Table definition for blobs
table! {
    blobs (cid, did) {
        cid -> Text,
        did -> Text,
        path -> Text,
        size -> Integer,
        mime_type -> Text,
        quarantined -> Bool,
        temp -> Bool,
    }
}

impl BlobStoreSql {
    /// Create a new SQL-based blob store for the given DID
    pub fn new(did: String, cfg: &BlobConfig, db: Arc<DbConn>) -> Self {
        let actor_path = cfg.path.join(&did);

        // Create actor directory if it doesn't exist
        if !actor_path.exists() {
            // Use blocking to avoid complicating this constructor
            std::fs::create_dir_all(&actor_path).unwrap_or_else(|_| {
                panic!("Failed to create blob directory: {}", actor_path.display())
            });
        }

        BlobStoreSql {
            db,
            did,
            path: actor_path,
        }
    }

    /// Create a factory function for blob stores
    pub fn creator(cfg: &BlobConfig, db: Arc<DbConn>) -> Box<dyn Fn(String) -> BlobStoreSql + '_> {
        let db_clone = db.clone();
        Box::new(move |did: String| BlobStoreSql::new(did, cfg, db_clone.clone()))
    }

    /// Generate a random key for temporary blobs
    fn gen_key(&self) -> String {
        get_random_str()
    }

    /// Get the path for a temporary blob
    fn get_tmp_path(&self, key: &str) -> PathBuf {
        self.path.join("tmp").join(key)
    }

    /// Get the filesystem path for a stored blob
    fn get_stored_path(&self, cid: &Cid) -> PathBuf {
        self.path.join("blocks").join(cid.to_string())
    }

    /// Get the filesystem path for a quarantined blob
    fn get_quarantined_path(&self, cid: &Cid) -> PathBuf {
        self.path.join("quarantine").join(cid.to_string())
    }

    /// Store a blob temporarily
    pub async fn put_temp(&self, bytes: Vec<u8>) -> Result<String> {
        let key = self.gen_key();
        let tmp_path = self.get_tmp_path(&key);

        // Ensure the directory exists
        if let Some(parent) = tmp_path.parent() {
            fs::create_dir_all(parent)
                .await
                .context("Failed to create temp directory")?;
        }

        // Write the blob data to the file
        fs::write(&tmp_path, &bytes)
            .await
            .context("Failed to write temporary blob")?;

        // Clone values to be used in the closure
        let did_clone = self.did.clone();
        let tmp_path_str = tmp_path.to_string_lossy().to_string();
        let bytes_len = bytes.len() as i32;

        // Store metadata in the database (will be updated when made permanent)
        self.db
            .run(move |conn| {
                let entry = BlobEntry {
                    cid: "temp".to_string(), // Will be updated when made permanent
                    did: did_clone,
                    path: tmp_path_str,
                    size: bytes_len,
                    mime_type: "application/octet-stream".to_string(), // Will be updated when made permanent
                    quarantined: false,
                    temp: true,
                };

                diesel::insert_into(blobs::table)
                    .values(&entry)
                    .execute(conn)
                    .context("Failed to insert temporary blob metadata")
            })
            .await?;

        Ok(key)
    }

    /// Make a temporary blob permanent
    pub async fn make_permanent(&self, key: String, cid: Cid) -> Result<()> {
        let already_has = self.has_stored(cid).await?;
        if !already_has {
            let tmp_path = self.get_tmp_path(&key);
            let stored_path = self.get_stored_path(&cid);

            // Ensure parent directory exists
            if let Some(parent) = stored_path.parent() {
                fs::create_dir_all(parent)
                    .await
                    .context("Failed to create blocks directory")?;
            }

            // Read the bytes
            let bytes = fs::read(&tmp_path)
                .await
                .context("Failed to read temporary blob")?;

            // Write to permanent location
            fs::write(&stored_path, &bytes)
                .await
                .context("Failed to write permanent blob")?;

            // Update database metadata
            let tmp_path_clone = tmp_path.clone();
            self.db
                .run(move |conn| {
                    // Update the entry with the correct CID and path
                    diesel::update(blobs::table)
                        .filter(blobs::path.eq(tmp_path_clone.to_string_lossy().to_string()))
                        .set((
                            blobs::cid.eq(cid.to_string()),
                            blobs::path.eq(stored_path.to_string_lossy().to_string()),
                            blobs::temp.eq(false),
                        ))
                        .execute(conn)
                        .context("Failed to update blob metadata")
                })
                .await?;

            // Remove the temporary file
            fs::remove_file(tmp_path)
                .await
                .context("Failed to remove temporary blob")?;

            Ok(())
        } else {
            // Already saved, so delete the temp file
            let tmp_path = self.get_tmp_path(&key);
            if tmp_path.exists() {
                fs::remove_file(tmp_path)
                    .await
                    .context("Failed to remove existing temporary blob")?;
            }
            Ok(())
        }
    }

    /// Store a blob directly as permanent
    pub async fn put_permanent(&self, cid: Cid, bytes: Vec<u8>) -> Result<()> {
        let stored_path = self.get_stored_path(&cid);

        // Ensure parent directory exists
        if let Some(parent) = stored_path.parent() {
            fs::create_dir_all(parent)
                .await
                .context("Failed to create blocks directory")?;
        }

        // Write to permanent location
        fs::write(&stored_path, &bytes)
            .await
            .context("Failed to write permanent blob")?;

        let stored_path_str = stored_path.to_string_lossy().to_string();
        let cid_str = cid.to_string();
        let did_clone = self.did.clone();
        let bytes_len = bytes.len() as i32;

        // Update database metadata
        self.db
            .run(move |conn| {
                let entry = BlobEntry {
                    cid: cid_str,
                    did: did_clone,
                    path: stored_path_str.clone(),
                    size: bytes_len,
                    mime_type: "application/octet-stream".to_string(), // Could be improved with MIME detection
                    quarantined: false,
                    temp: false,
                };

                diesel::insert_into(blobs::table)
                    .values(&entry)
                    .on_conflict((blobs::cid, blobs::did))
                    .do_update()
                    .set(blobs::path.eq(stored_path_str))
                    .execute(conn)
                    .context("Failed to insert permanent blob metadata")
            })
            .await?;

        Ok(())
    }

    /// Quarantine a blob
    pub async fn quarantine(&self, cid: Cid) -> Result<()> {
        let stored_path = self.get_stored_path(&cid);
        let quarantined_path = self.get_quarantined_path(&cid);

        // Ensure parent directory exists
        if let Some(parent) = quarantined_path.parent() {
            fs::create_dir_all(parent)
                .await
                .context("Failed to create quarantine directory")?;
        }

        // Move the blob if it exists
        if stored_path.exists() {
            // Read the bytes
            let bytes = fs::read(&stored_path)
                .await
                .context("Failed to read stored blob")?;

            // Write to quarantine location
            fs::write(&quarantined_path, &bytes)
                .await
                .context("Failed to write quarantined blob")?;

            // Update database metadata
            let cid_str = cid.to_string();
            let did_clone = self.did.clone();
            let quarantined_path_str = quarantined_path.to_string_lossy().to_string();

            self.db
                .run(move |conn| {
                    diesel::update(blobs::table)
                        .filter(blobs::cid.eq(cid_str))
                        .filter(blobs::did.eq(did_clone))
                        .set((
                            blobs::path.eq(quarantined_path_str),
                            blobs::quarantined.eq(true),
                        ))
                        .execute(conn)
                        .context("Failed to update blob metadata for quarantine")
                })
                .await?;

            // Remove the original file
            fs::remove_file(stored_path)
                .await
                .context("Failed to remove quarantined blob")?;
        }

        Ok(())
    }

    /// Unquarantine a blob
    pub async fn unquarantine(&self, cid: Cid) -> Result<()> {
        let quarantined_path = self.get_quarantined_path(&cid);
        let stored_path = self.get_stored_path(&cid);

        // Ensure parent directory exists
        if let Some(parent) = stored_path.parent() {
            fs::create_dir_all(parent)
                .await
                .context("Failed to create blocks directory")?;
        }

        // Move the blob if it exists
        if quarantined_path.exists() {
            // Read the bytes
            let bytes = fs::read(&quarantined_path)
                .await
                .context("Failed to read quarantined blob")?;

            // Write to normal location
            fs::write(&stored_path, &bytes)
                .await
                .context("Failed to write unquarantined blob")?;

            // Update database metadata
            let stored_path_str = stored_path.to_string_lossy().to_string();
            let cid_str = cid.to_string();
            let did_clone = self.did.clone();

            self.db
                .run(move |conn| {
                    diesel::update(blobs::table)
                        .filter(blobs::cid.eq(cid_str))
                        .filter(blobs::did.eq(did_clone))
                        .set((
                            blobs::path.eq(stored_path_str),
                            blobs::quarantined.eq(false),
                        ))
                        .execute(conn)
                        .context("Failed to update blob metadata for unquarantine")
                })
                .await?;

            // Remove the quarantined file
            fs::remove_file(quarantined_path)
                .await
                .context("Failed to remove from quarantine")?;
        }

        Ok(())
    }

    /// Get a blob as a stream
    pub async fn get_object(&self, cid_param: Cid) -> Result<ByteStream> {
        use self::blobs::dsl::*;

        // Get the blob path from the database
        let cid_string = cid_param.to_string();
        let did_clone = self.did.clone();

        let blob_record = self
            .db
            .run(move |conn| {
                blobs
                    .filter(cid.eq(&cid_string))
                    .filter(did.eq(&did_clone))
                    .filter(quarantined.eq(false))
                    .select(path)
                    .first::<String>(conn)
                    .optional()
                    .context("Failed to query blob metadata")
            })
            .await?;

        if let Some(blob_path) = blob_record {
            // Read the blob data
            let bytes = fs::read(blob_path)
                .await
                .context("Failed to read blob data")?;
            Ok(ByteStream::new(bytes))
        } else {
            anyhow::bail!("Blob not found: {:?}", cid)
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
    pub async fn delete(&self, cid: String) -> Result<()> {
        self.delete_key(self.get_stored_path(&Cid::from_str(&cid)?))
            .await
    }

    /// Delete multiple blobs by CID
    pub async fn delete_many(&self, cids: Vec<Cid>) -> Result<()> {
        for cid in cids {
            self.delete_key(self.get_stored_path(&cid)).await?;
        }
        Ok(())
    }

    /// Check if a blob is stored
    pub async fn has_stored(&self, cid_param: Cid) -> Result<bool> {
        use self::blobs::dsl::*;

        let cid_string = cid_param.to_string();
        let did_clone = self.did.clone();

        let exists = self
            .db
            .run(move |conn| {
                diesel::select(diesel::dsl::exists(
                    blobs
                        .filter(cid.eq(&cid_string))
                        .filter(did.eq(&did_clone))
                        .filter(temp.eq(false)),
                ))
                .get_result::<bool>(conn)
                .context("Failed to check if blob exists")
            })
            .await?;

        Ok(exists)
    }

    /// Check if a temporary blob exists
    pub async fn has_temp(&self, key: String) -> Result<bool> {
        let tmp_path = self.get_tmp_path(&key);
        Ok(tmp_path.exists())
    }

    /// Check if a blob exists by key
    async fn has_key(&self, key_path: PathBuf) -> bool {
        key_path.exists()
    }

    /// Delete a blob by its key path
    async fn delete_key(&self, key_path: PathBuf) -> Result<()> {
        use self::blobs::dsl::*;

        // Delete from database first
        let key_path_clone = key_path.clone();
        self.db
            .run(move |conn| {
                diesel::delete(blobs)
                    .filter(path.eq(key_path_clone.to_string_lossy().to_string()))
                    .execute(conn)
                    .context("Failed to delete blob metadata")
            })
            .await?;

        // Then delete the file if it exists
        if key_path.exists() {
            fs::remove_file(key_path)
                .await
                .context("Failed to delete blob file")?;
        }

        Ok(())
    }

    /// Delete multiple blobs by key path
    async fn delete_many_keys(&self, keys: Vec<String>) -> Result<()> {
        for key in keys {
            self.delete_key(PathBuf::from(key)).await?;
        }
        Ok(())
    }

    /// Move a blob from one location to another
    async fn move_object(&self, keys: MoveObject) -> Result<()> {
        let from_path = PathBuf::from(&keys.from);
        let to_path = PathBuf::from(&keys.to);

        // Ensure parent directory exists
        if let Some(parent) = to_path.parent() {
            fs::create_dir_all(parent)
                .await
                .context("Failed to create directory")?;
        }

        // Only move if the source exists
        if from_path.exists() {
            // Read the data
            let data = fs::read(&from_path)
                .await
                .context("Failed to read source blob")?;

            // Write to the destination
            fs::write(&to_path, data)
                .await
                .context("Failed to write destination blob")?;

            // Update the database record
            let from_path_clone = from_path.clone();
            self.db
                .run(move |conn| {
                    diesel::update(blobs::table)
                        .filter(blobs::path.eq(from_path_clone.to_string_lossy().to_string()))
                        .set(blobs::path.eq(to_path.to_string_lossy().to_string()))
                        .execute(conn)
                        .context("Failed to update blob path")
                })
                .await?;

            // Delete the source file
            fs::remove_file(from_path)
                .await
                .context("Failed to remove source blob")?;
        }

        Ok(())
    }
}
