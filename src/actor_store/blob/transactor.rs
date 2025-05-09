//! Blob transaction functionality.

use anyhow::{Context as _, Result};
use atrium_api::types::{Blob, CidLink};
use atrium_repo::Cid;
use sha2::Digest;
use sqlx::{Row, SqlitePool};
use std::path::PathBuf;
use tokio::fs;
use uuid::Uuid;

use super::reader::BlobReader;
use crate::config::BlobConfig;

/// Blob metadata for a newly uploaded blob.
#[derive(Debug, Clone)]
pub struct BlobMetadata {
    /// Temporary key for the blob during upload.
    pub temp_key: String,
    /// Size of the blob in bytes.
    pub size: u64,
    /// CID of the blob.
    pub cid: Cid,
    /// MIME type of the blob.
    pub mime_type: String,
    /// Optional width if the blob is an image.
    pub width: Option<i32>,
    /// Optional height if the blob is an image.
    pub height: Option<i32>,
}

/// Transactor for blob operations.
pub struct BlobTransactor {
    /// The blob reader.
    pub reader: BlobReader,
    /// The blob storage directory.
    pub blobs_dir: PathBuf,
    /// Temporary directory for blob uploads.
    pub temp_dir: PathBuf,
}

impl BlobTransactor {
    /// Create a new blob transactor.
    pub fn new(db: SqlitePool, config: BlobConfig, did: String) -> Self {
        Self {
            reader: BlobReader::new(db, config.clone(), did),
            blobs_dir: config.path.clone(),
            temp_dir: config.path.join("temp"),
        }
    }

    /// Register blob associations with records.
    pub async fn insert_blobs(&self, record_uri: &str, blobs: &[Blob]) -> Result<()> {
        if blobs.is_empty() {
            return Ok(());
        }

        let mut query =
            sqlx::QueryBuilder::new("INSERT INTO record_blob (recordUri, blobCid) VALUES ");

        for (i, blob) in blobs.iter().enumerate() {
            if i > 0 {
                query.push(", ");
            }

            let cid_str = blob.r#ref.0.to_string();
            query
                .push("(")
                .push_bind(record_uri)
                .push(", ")
                .push_bind(cid_str)
                .push(")");
        }

        query.push(" ON CONFLICT DO NOTHING");

        query
            .build()
            .execute(&self.reader.db)
            .await
            .context("failed to insert blob references")?;

        Ok(())
    }

    /// Upload a blob and get its metadata.
    pub async fn upload_blob_and_get_metadata(
        &self,
        mime_type: &str,
        data: &[u8],
    ) -> Result<BlobMetadata> {
        // Ensure temp directory exists
        fs::create_dir_all(&self.temp_dir)
            .await
            .context("failed to create temp directory")?;

        // Generate a temporary key
        let temp_key = format!("temp-{}", Uuid::new_v4());
        let temp_path = self.temp_dir.join(&temp_key);

        // Write data to temp file
        fs::write(&temp_path, data)
            .await
            .context("failed to write blob to temp file")?;

        // Calculate SHA-256 hash for CID
        let digest = sha2::Sha256::digest(data);

        // Create CID from hash (using raw codec 0x55)
        let multihash = atrium_repo::Multihash::wrap(atrium_repo::blockstore::SHA2_256, &digest)
            .context("failed to create multihash")?;
        let cid = Cid::new_v1(0x55, multihash);

        // For now, we're not detecting image dimensions
        let width = None;
        let height = None;

        Ok(BlobMetadata {
            temp_key,
            size: data.len() as u64,
            cid,
            mime_type: mime_type.to_string(),
            width,
            height,
        })
    }

    /// Track a new blob that's not yet associated with a record.
    pub async fn track_untethered_blob(&self, metadata: &BlobMetadata) -> Result<Blob> {
        let cid_str = metadata.cid.to_string();

        // Check if blob exists and is taken down
        let existing = sqlx::query!(r#"SELECT takedownRef FROM blob WHERE cid = ?"#, cid_str)
            .fetch_optional(&self.reader.db)
            .await
            .context("failed to check blob existence")?;

        if let Some(row) = existing {
            if row.takedownRef.is_some() {
                return Err(anyhow::anyhow!(
                    "Blob has been taken down, cannot re-upload"
                ));
            }
        }

        // Insert or update blob record
        let size = metadata.size as i64;
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query!(
            r#"
            INSERT INTO blob
            (cid, mimeType, size, tempKey, width, height, createdAt)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(cid) DO UPDATE SET
            tempKey = CASE
                WHEN blob.tempKey IS NULL THEN EXCLUDED.tempKey
                ELSE blob.tempKey
            END
            "#,
            cid_str,
            metadata.mime_type,
            size,
            metadata.temp_key,
            metadata.width,
            metadata.height,
            now,
        )
        .execute(&self.reader.db)
        .await
        .context("failed to track blob in database")?;

        // Create and return a blob reference
        Ok(Blob {
            r#ref: CidLink(metadata.cid.clone()),
            mime_type: metadata.mime_type.clone(),
            size: metadata.size as usize,
        })
    }

    /// Process blobs for a repository write operation.
    pub async fn process_write_blobs(
        &self,
        _rev: &str,
        blobs: &[Blob],
        uris: &[String],
    ) -> Result<()> {
        // Handle deleted/updated records
        self.delete_dereferenced_blobs(uris).await?;

        // Process each blob
        for blob in blobs {
            // Verify and make permanent
            self.verify_blob_and_make_permanent(blob).await?;
        }

        Ok(())
    }

    /// Delete blobs that are no longer referenced by any record.
    pub async fn delete_dereferenced_blobs(&self, updated_uris: &[String]) -> Result<()> {
        if updated_uris.is_empty() {
            return Ok(());
        }

        // Find blobs that were referenced by the updated URIs
        let placeholders = (0..updated_uris.len())
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(",");

        let query = format!(
            "DELETE FROM record_blob WHERE recordUri IN ({}) RETURNING blobCid",
            placeholders
        );

        let mut query_builder = sqlx::query(&query);
        for uri in updated_uris {
            query_builder = query_builder.bind(uri);
        }

        let deleted_blobs = query_builder
            .map(|row: sqlx::sqlite::SqliteRow| row.get::<String, _>(0))
            .fetch_all(&self.reader.db)
            .await
            .context("failed to delete dereferenced blobs")?;

        if deleted_blobs.is_empty() {
            return Ok(());
        }

        // Find blobs that are still referenced elsewhere
        let still_referenced = sqlx::query!(
            r#"SELECT DISTINCT blobCid FROM record_blob WHERE blobCid IN (SELECT blobCid FROM record_blob)"#
        )
        .fetch_all(&self.reader.db)
        .await
        .context("failed to find referenced blobs")?;

        let referenced_set: std::collections::HashSet<String> = still_referenced
            .into_iter()
            .map(|row| row.blobCid)
            .collect();

        // Delete blobs that are no longer referenced anywhere
        let blobs_to_delete: Vec<String> = deleted_blobs
            .into_iter()
            .filter(|cid| !referenced_set.contains(cid))
            .collect();

        if !blobs_to_delete.is_empty() {
            // Delete from database
            let placeholders = (0..blobs_to_delete.len())
                .map(|_| "?")
                .collect::<Vec<_>>()
                .join(",");

            let query = format!("DELETE FROM blob WHERE cid IN ({})", placeholders);

            let mut query_builder = sqlx::query(&query);
            for cid in &blobs_to_delete {
                query_builder = query_builder.bind(cid);
            }

            query_builder
                .execute(&self.reader.db)
                .await
                .context("failed to delete blob records")?;

            // Delete files from disk
            for cid in blobs_to_delete {
                let path = self.blobs_dir.join(format!("{}.blob", cid));
                if let Err(e) = fs::remove_file(&path).await {
                    tracing::warn!("Failed to delete blob file: {:?}", e);
                }
            }
        }

        Ok(())
    }

    /// Verify a blob's integrity and move it from temporary to permanent storage.
    pub async fn verify_blob_and_make_permanent(&self, blob: &Blob) -> Result<()> {
        let cid_str = blob.r#ref.0.to_string();

        // Get blob from database
        let row = sqlx::query!(
            r#"SELECT * FROM blob WHERE cid = ? AND takedownRef IS NULL"#,
            cid_str
        )
        .fetch_optional(&self.reader.db)
        .await
        .context("failed to find blob")?;

        let row = match row {
            Some(r) => r,
            None => return Err(anyhow::anyhow!("Could not find blob: {}", cid_str)),
        };

        // Verify size constraint
        if (row.size as u64) > self.reader.config.limit {
            return Err(anyhow::anyhow!(
                "Blob is too large. Size is {} bytes but maximum allowed is {} bytes",
                row.size,
                self.reader.config.limit
            ));
        }

        // Check MIME type
        if row.mimeType != blob.mime_type {
            return Err(anyhow::anyhow!(
                "MIME type does not match. Expected: {}, got: {}",
                row.mimeType,
                blob.mime_type
            ));
        }

        // If temp key exists, move to permanent storage
        if let Some(temp_key) = &row.tempKey {
            let temp_path = self.temp_dir.join(temp_key);
            let blob_path = self.blobs_dir.join(format!("{}.blob", cid_str));

            // Only move if temp file exists and permanent file doesn't
            if fs::try_exists(&temp_path).await.unwrap_or(false)
                && !fs::try_exists(&blob_path).await.unwrap_or(false)
            {
                // Create parent directories if needed
                if let Some(parent) = blob_path.parent() {
                    fs::create_dir_all(parent)
                        .await
                        .context("failed to create blob directory")?;
                }

                // Move file from temp to permanent location
                fs::copy(&temp_path, &blob_path)
                    .await
                    .context("failed to copy blob to permanent storage")?;
                fs::remove_file(&temp_path)
                    .await
                    .context("failed to remove temporary blob file")?;
            }

            // Update database
            sqlx::query!(r#"UPDATE blob SET tempKey = NULL WHERE cid = ?"#, cid_str)
                .execute(&self.reader.db)
                .await
                .context("failed to update blob record after making permanent")?;
        }

        Ok(())
    }

    /// Register a blob in the database
    pub async fn register_blob(&self, cid: String, mime_type: String, size: u64) -> Result<()> {
        self.reader.register_blob(cid, mime_type, size as i64).await
    }

    /// Associate a blob with a record
    pub async fn associate_blob(&self, cid: &str, record_uri: &str) -> Result<()> {
        sqlx::query!(
            r#"
            INSERT INTO record_blob (blobCid, recordUri)
            VALUES (?, ?)
            ON CONFLICT DO NOTHING
            "#,
            cid,
            record_uri
        )
        .execute(&self.reader.db)
        .await
        .context("failed to associate blob with record")?;

        Ok(())
    }
}

/// Check if a mime type is accepted based on the accept list.
fn accepts_mime(mime: &str, accepted: &[String]) -> bool {
    // Accept all types
    if accepted.contains(&"*/*".to_string()) {
        return true;
    }

    // Check for explicit match
    if accepted.contains(&mime.to_string()) {
        return true;
    }

    // Check for type/* matches
    let type_prefix = mime.split('/').next().unwrap_or("");
    let wildcard = format!("{}/*", type_prefix);

    accepted.contains(&wildcard)
}
