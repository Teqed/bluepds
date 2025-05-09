//! Blob reading functionality.

use anyhow::{Context as _, Result};
use atrium_repo::Cid;
use sqlx::{Row, SqlitePool};

use crate::config::BlobConfig;

/// Reader for blob data in the actor store.
pub struct BlobReader {
    /// Database connection.
    pub db: SqlitePool,
    /// Configuration for blob storage.
    pub config: BlobConfig,
    /// DID of the repository owner.
    pub did: String,
}

impl BlobReader {
    /// Create a new blob reader.
    pub fn new(db: SqlitePool, config: BlobConfig, did: String) -> Self {
        Self { db, config, did }
    }

    /// Get metadata for a blob.
    pub async fn get_blob_metadata(&self, cid: &Cid) -> Result<Option<BlobMetadata>> {
        let cid_str = cid.to_string();
        let result = sqlx::query!(
            r#"SELECT size, mimeType, takedownRef FROM blob WHERE cid = ?"#,
            cid_str
        )
        .fetch_optional(&self.db)
        .await
        .context("failed to fetch blob metadata")?;

        match result {
            Some(row) => Ok(Some(BlobMetadata {
                cid: cid.clone(),
                size: row.size as u64,
                mime_type: row.mimeType,
                takedown_ref: row.takedownRef,
            })),
            None => Ok(None),
        }
    }

    /// Get a blob's full data and metadata.
    pub async fn get_blob(&self, cid: &Cid) -> Result<Option<BlobData>> {
        // First check the metadata
        let metadata = match self.get_blob_metadata(cid).await? {
            Some(meta) => meta,
            None => return Ok(None),
        };

        // If there's a takedown, return metadata only with no content
        if metadata.takedown_ref.is_some() {
            return Ok(Some(BlobData {
                metadata,
                content: None,
            }));
        }

        // Get the blob file path
        let blob_path = self.config.path.join(format!("{}.blob", cid));

        // Check if file exists
        if !blob_path.exists() {
            return Ok(None);
        }

        // Read the file
        let content = tokio::fs::read(&blob_path)
            .await
            .context("failed to read blob file")?;

        Ok(Some(BlobData {
            metadata,
            content: Some(content),
        }))
    }

    /// List blobs for a repository.
    pub async fn list_blobs(&self, opts: ListBlobsOptions) -> Result<Vec<String>> {
        todo!("Implement blob listing");
    }

    /// Get takedown status for a blob.
    pub async fn get_blob_takedown_status(&self, cid: &Cid) -> Result<Option<String>> {
        let cid_str = cid.to_string();
        let result = sqlx::query!(r#"SELECT takedownRef FROM blob WHERE cid = ?"#, cid_str)
            .fetch_optional(&self.db)
            .await
            .context("failed to fetch blob takedown status")?;

        Ok(result.and_then(|row| row.takedownRef))
    }

    /// Get records that reference a blob.
    pub async fn get_records_for_blob(&self, cid: &Cid) -> Result<Vec<String>> {
        let cid_str = cid.to_string();
        let records = sqlx::query!(
            r#"SELECT recordUri FROM record_blob WHERE blobCid = ?"#,
            cid_str
        )
        .fetch_all(&self.db)
        .await
        .context("failed to fetch records for blob")?;

        Ok(records.into_iter().map(|r| r.recordUri).collect())
    }

    /// Get blobs referenced by a record.
    pub async fn get_blobs_for_record(&self, record_uri: &str) -> Result<Vec<String>> {
        let blobs = sqlx::query!(
            r#"SELECT blobCid FROM record_blob WHERE recordUri = ?"#,
            record_uri
        )
        .fetch_all(&self.db)
        .await
        .context("failed to fetch blobs for record")?;

        Ok(blobs.into_iter().map(|r| r.blobCid).collect())
    }

    /// Count total blobs.
    pub async fn blob_count(&self) -> Result<i64> {
        let result = sqlx::query!(r#"SELECT COUNT(*) as count FROM blob"#)
            .fetch_one(&self.db)
            .await
            .context("failed to count blobs")?;

        Ok(result.count)
    }

    /// Count distinct blobs referenced by records.
    pub async fn record_blob_count(&self) -> Result<i64> {
        let result = sqlx::query!(r#"SELECT COUNT(DISTINCT blobCid) as count FROM record_blob"#)
            .fetch_one(&self.db)
            .await
            .context("failed to count record blobs")?;

        Ok(result.count)
    }

    /// List blobs that are referenced but missing from storage.
    pub async fn list_missing_blobs(
        &self,
        opts: ListMissingBlobsOptions,
    ) -> Result<Vec<MissingBlob>> {
        let mut query = sqlx::QueryBuilder::new(
            "SELECT rb.blobCid, rb.recordUri FROM record_blob rb
             WHERE NOT EXISTS (
                SELECT 1 FROM blob b WHERE b.cid = rb.blobCid
             )",
        );

        if let Some(cursor) = &opts.cursor {
            query.push(" AND rb.blobCid > ").push_bind(cursor);
        }

        query
            .push(" ORDER BY rb.blobCid ASC")
            .push(" LIMIT ")
            .push_bind(opts.limit);

        let missing = query
            .build()
            .map(|row: sqlx::sqlite::SqliteRow| MissingBlob {
                cid: row.get::<String, _>(0),
                record_uri: row.get::<String, _>(1),
            })
            .fetch_all(&self.db)
            .await
            .context("failed to fetch missing blobs")?;

        Ok(missing)
    }
}

/// Metadata about a blob.
#[derive(Debug, Clone)]
pub struct BlobMetadata {
    /// The CID of the blob.
    pub cid: Cid,
    /// The size of the blob in bytes.
    pub size: u64,
    /// The MIME type of the blob.
    pub mime_type: String,
    /// Reference for takedown, if any.
    pub takedown_ref: Option<String>,
}

/// Complete blob data with content.
#[derive(Debug)]
pub struct BlobData {
    /// Metadata about the blob.
    pub metadata: BlobMetadata,
    /// The actual content of the blob, if available.
    pub content: Option<Vec<u8>>,
}

/// Options for listing blobs.
#[derive(Debug, Clone)]
pub struct ListBlobsOptions {
    /// Optional revision to list blobs since.
    pub since: Option<String>,
    /// Optional cursor for pagination.
    pub cursor: Option<String>,
    /// Maximum number of blobs to return.
    pub limit: i64,
}

/// Options for listing missing blobs.
#[derive(Debug, Clone)]
pub struct ListMissingBlobsOptions {
    /// Optional cursor for pagination.
    pub cursor: Option<String>,
    /// Maximum number of missing blobs to return.
    pub limit: i64,
}

/// Information about a missing blob.
#[derive(Debug, Clone)]
pub struct MissingBlob {
    /// CID of the missing blob.
    pub cid: String,
    /// URI of the record referencing the missing blob.
    pub record_uri: String,
}
