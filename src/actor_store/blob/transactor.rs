// src/actor_store/blob/transactor.rs
//! Blob transaction functionality.

use anyhow::{Context as _, Result};
use atrium_api::types::Blob;
use atrium_repo::Cid;
use sqlx::{Row, SqlitePool};
use std::path::PathBuf;
use tokio::fs;

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

            let cid_str = format!("{:?}", blob.r#ref);
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
        todo!()
    }

    /// Track a new blob that's not yet associated with a record.
    pub async fn track_untethered_blob(&self, metadata: &BlobMetadata) -> Result<Blob> {
        todo!()
    }

    /// Process blobs for a repository write operation.
    pub async fn process_write_blobs(
        &self,
        rev: &str,
        blobs: &[Blob],
        uris: &[String],
    ) -> Result<()> {
        todo!()
    }

    /// Update the takedown status of a blob.
    pub async fn update_blob_takedown_status(
        &self,
        cid: &Cid,
        takedown_ref: Option<String>,
    ) -> Result<()> {
        let cid_str = cid.to_string();

        sqlx::query!(
            r#"UPDATE blob SET takedownRef = ? WHERE cid = ?"#,
            takedown_ref,
            cid_str
        )
        .execute(&self.reader.db)
        .await
        .context("failed to update blob takedown status")?;

        // Handle quarantine/unquarantine
        let blob_path = self.blobs_dir.join(format!("{}.blob", cid_str));
        let quarantine_path = self
            .blobs_dir
            .join("quarantine")
            .join(format!("{}.blob", cid_str));

        if takedown_ref.is_some() {
            // Quarantine the blob
            fs::create_dir_all(self.blobs_dir.join("quarantine"))
                .await
                .context("failed to create quarantine directory")?;

            if fs::try_exists(&blob_path).await? {
                fs::rename(&blob_path, &quarantine_path)
                    .await
                    .context("failed to quarantine blob")?;
            }
        } else {
            // Unquarantine the blob
            if fs::try_exists(&quarantine_path).await? {
                fs::rename(&quarantine_path, &blob_path)
                    .await
                    .context("failed to unquarantine blob")?;
            }
        }

        Ok(())
    }

    /// Delete blobs that are no longer referenced by any record.
    async fn delete_dereferenced_blobs(&self, updated_uris: &[String]) -> Result<()> {
        if updated_uris.is_empty() {
            return Ok(());
        }

        // Find blobs that were referenced by the updated URIs
        let placeholders = (0..updated_uris.len())
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(",");

        let mut query = format!(
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

            let mut query = format!("DELETE FROM blob WHERE cid IN ({})", placeholders);

            let mut query_builder = sqlx::query(&query);
            for cid in &blobs_to_delete {
                query_builder = query_builder.bind(cid);
            }

            query_builder
                .execute(&self.reader.db)
                .await
                .context("failed to delete blob records")?;

            // Delete files from disk (background task)
            // tokio::spawn(async move {
            //     for cid in blobs_to_delete {
            //         let path = self.blobs_dir.join(format!("{}.blob", cid));
            //         if let Err(e) = fs::remove_file(&path).await {
            //             eprintln!("Failed to delete blob file: {:?}", e);
            //         }
            //     }
            // });
            // For now, do in foreground:
            for cid in blobs_to_delete {
                let path = self.blobs_dir.join(format!("{}.blob", cid));
                if let Err(e) = fs::remove_file(&path).await {
                    eprintln!("Failed to delete blob file: {:?}", e);
                }
            }
        }

        Ok(())
    }

    /// Verify a blob's integrity and move it from temporary to permanent storage.
    async fn verify_blob_and_make_permanent(&self, blob: &Blob) -> Result<()> {
        todo!()
    }

    /// Verify blob constraints (size, mime type).
    async fn verify_blob_constraints(
        &self,
        blob: &Blob,
        record: &sqlx::sqlite::SqliteRow,
    ) -> Result<()> {
        todo!()
    }

    /// Associate a blob with a record.
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
