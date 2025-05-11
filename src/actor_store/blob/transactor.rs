//! Blob transaction functionality.

use anyhow::{Context as _, Result};
use atrium_api::{
    com::atproto::admin::defs::StatusAttr,
    types::{Blob, CidLink},
};
use atrium_repo::Cid;
use futures::FutureExt;
use futures::future::BoxFuture;
use uuid::Uuid;

use super::{BackgroundQueue, BlobReader};
use crate::{
    actor_store::ActorDb,
    repo::{
        block_map::sha256_raw_to_cid,
        types::{BlobStore, BlobStoreTrait as _, PreparedBlobRef, PreparedWrite, WriteOpAction},
    },
};

/// Blob metadata for a newly uploaded blob.
#[derive(Debug, Clone)]
pub(crate) struct BlobMetadata {
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
pub(crate) struct BlobTransactor {
    /// The blob reader.
    pub reader: BlobReader,
    pub background_queue: BackgroundQueue,
}

impl BlobTransactor {
    /// Create a new blob transactor.
    pub(crate) fn new(
        db: ActorDb,
        blob_store: BlobStore,
        background_queue: BackgroundQueue,
    ) -> Self {
        Self {
            reader: BlobReader::new(db, blob_store),
            background_queue,
        }
    }

    /// Register blob associations with records.
    pub(crate) async fn insert_blobs(&self, record_uri: &str, blobs: &[Blob]) -> Result<()> {
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
            .execute(&self.reader.db.pool)
            .await
            .context("failed to insert blob references")?;

        Ok(())
    }

    /// Upload a blob and get its metadata.
    pub(crate) async fn upload_blob_and_get_metadata(
        &self,
        user_suggested_mime: &str,
        blob_stream: &[u8],
    ) -> Result<BlobMetadata> {
        let temp_key = self.reader.blobstore.put_temp(blob_stream).await?;
        todo!();
        let size = stream_size(blob_stream).await?;
        let sha256 = sha256_stream(blob_stream).await?;
        let img_info = img::maybe_get_info(blob_stream).await?;
        let sniffed_mime = mime_type_from_stream(blob_stream).await?;
        let cid = sha256_raw_to_cid(sha256);
        let mime_type = sniffed_mime.unwrap_or_else(|| user_suggested_mime.to_string());
        Ok(BlobMetadata {
            temp_key,
            size,
            cid,
            mime_type,
            width: img_info.map(|info| info.width),
            height: img_info.map(|info| info.height),
        })
    }

    /// Track a new blob that's not yet associated with a record.
    pub(crate) async fn track_untethered_blob(&self, metadata: &BlobMetadata) -> Result<Blob> {
        let cid_str = metadata.cid.to_string();

        // Check if blob exists and is taken down
        let existing = sqlx::query!(r#"SELECT takedownRef FROM blob WHERE cid = ?"#, cid_str)
            .fetch_optional(&self.reader.db.pool)
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
        .execute(&self.reader.db.pool)
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
    pub(crate) async fn process_write_blobs(&self, writes: Vec<PreparedWrite>) -> Result<()> {
        self.delete_dereferenced_blobs(writes.clone(), false)
            .await
            .context("failed to delete dereferenced blobs")?;

        // Process blobs for creates and updates
        let mut futures: Vec<BoxFuture<'_, Result<()>>> = Vec::new();
        for write in writes.iter() {
            if write.action() == &WriteOpAction::Create || write.action() == &WriteOpAction::Update
            {
                for blob in write.blobs().unwrap().iter() {
                    futures.push(Box::pin(self.verify_blob_and_make_permanent(blob)));
                    futures.push(Box::pin(self.associate_blob(blob, write.uri())));
                }
            }
        }

        // Wait for all blob operations to complete
        futures::future::join_all(futures)
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        Ok(())
    }

    /// Update the takedown status of a blob.
    pub(crate) async fn update_blob_takedown_status(
        &self,
        blob: &Blob,
        takedown: &StatusAttr,
    ) -> Result<()> {
        let takedown_ref = if takedown.applied {
            Some(
                takedown
                    .r#ref
                    .clone()
                    .unwrap_or_else(|| Uuid::new_v4().to_string()),
            )
        } else {
            None
        };

        let cid_str = blob.r#ref.0.to_string();
        sqlx::query!(
            r#"UPDATE blob SET takedownRef = ? WHERE cid = ?"#,
            takedown_ref,
            cid_str
        )
        .execute(&self.reader.db.pool)
        .await
        .context("failed to update blob takedown status")?;

        Ok(())
    }

    /// Delete blobs that are no longer referenced by any record.
    pub(crate) async fn delete_dereferenced_blobs(
        &self,
        writes: Vec<PreparedWrite>,
        skip_blob_store: bool,
    ) -> Result<()> {
        let deletes = writes
            .iter()
            .filter(|w| w.action() == &WriteOpAction::Delete)
            .collect::<Vec<_>>();
        let updates = writes
            .iter()
            .filter(|w| w.action() == &WriteOpAction::Update)
            .collect::<Vec<_>>();
        let uris: Vec<String> = deletes
            .iter()
            .chain(updates.iter())
            .map(|w| w.uri().to_string())
            .collect();

        if uris.is_empty() {
            return Ok(());
        }

        // Delete blobs from record_blob table
        let uris = uris.join(",");
        let deleted_repo_blobs = sqlx::query!(
            r#"DELETE FROM record_blob WHERE recordUri IN (?1) RETURNING *"#,
            uris
        )
        .fetch_all(&self.reader.db.pool)
        .await
        .context("failed to delete dereferenced blobs")?;

        if deleted_repo_blobs.is_empty() {
            return Ok(());
        }

        // Get the CIDs of the deleted blobs
        let deleted_repo_blob_cids: Vec<String> = deleted_repo_blobs
            .iter()
            .map(|row| row.blobCid.clone())
            .collect();

        // Check for duplicates in the record_blob table
        let duplicate_cids = deleted_repo_blob_cids.join(",");
        let duplicate_cids = sqlx::query!(
            r#"SELECT blobCid FROM record_blob WHERE blobCid IN (?1)"#,
            duplicate_cids
        )
        .fetch_all(&self.reader.db.pool)
        .await
        .context("failed to fetch duplicate CIDs")?;

        // Get new blob CIDs from the writes
        let new_blob_cids: Vec<String> = writes
            .iter()
            .filter_map(|w| {
                if w.action() == &WriteOpAction::Create || w.action() == &WriteOpAction::Update {
                    Some(
                        w.blobs()
                            .unwrap()
                            .iter()
                            .map(|b| b.cid.to_string())
                            .collect::<Vec<String>>(),
                    )
                } else {
                    None
                }
            })
            .flatten()
            .collect();

        // Determine which CIDs to keep and which to delete
        let cids_to_keep: Vec<String> = new_blob_cids
            .into_iter()
            .chain(duplicate_cids.into_iter().map(|row| row.blobCid))
            .collect();
        let cids_to_delete: Vec<String> = deleted_repo_blob_cids
            .into_iter()
            .filter(|cid| !cids_to_keep.contains(cid))
            .collect();
        if cids_to_delete.is_empty() {
            return Ok(());
        }
        // Delete blobs from the blob table
        let cids_to_delete = cids_to_delete.join(",");
        sqlx::query!(r#"DELETE FROM blob WHERE cid IN (?1)"#, cids_to_delete)
            .execute(&self.reader.db.pool)
            .await
            .context("failed to delete dereferenced blobs from blob table")?;
        // Optionally delete blobs from the blob store
        if !skip_blob_store {
            todo!();
        }
        Ok(())
    }

    /// Verify a blob's integrity and move it from temporary to permanent storage.
    pub(crate) async fn verify_blob_and_make_permanent(
        &self,
        blob: &PreparedBlobRef,
    ) -> Result<()> {
        let cid_str = blob.cid.to_string();
        let found = sqlx::query!(r#"SELECT * FROM blob WHERE cid = ?"#, cid_str)
            .fetch_optional(&self.reader.db.pool)
            .await
            .context("failed to fetch blob")?;
        if found.is_none() {
            return Err(anyhow::anyhow!("Blob not found"));
        }
        let found = found.unwrap();
        if found.takedownRef.is_some() {
            return Err(anyhow::anyhow!("Blob has been taken down"));
        }
        if found.tempKey.is_some() {
            todo!("verify_blob");
            verify_blob(blob, found);
            self.reader
                .blobstore
                .make_permanent(&found.tempKey.unwrap(), blob.cid)
                .await?;
            sqlx::query!(
                r#"UPDATE blob SET tempKey = NULL WHERE tempKey = ?"#,
                found.tempKey
            )
            .execute(&self.reader.db.pool)
            .await
            .context("failed to update blob temp key")?;
        }
        Ok(())
    }

    /// Associate a blob with a record
    pub(crate) async fn associate_blob(
        &self,
        blob: &PreparedBlobRef,
        record_uri: &str,
    ) -> Result<()> {
        let cid = blob.cid.to_string();
        sqlx::query!(
            r#"
            INSERT INTO record_blob (blobCid, recordUri)
            VALUES (?, ?)
            ON CONFLICT DO NOTHING
            "#,
            cid,
            record_uri
        )
        .execute(&self.reader.db.pool)
        .await
        .context("failed to associate blob with record")?;

        Ok(())
    }
}

/// Check if a mime type is accepted based on the accept list.
fn accepted_mime(mime: &str, accepted: &[String]) -> bool {
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
