//! Blob reading functionality.

use std::str::FromStr;

use anyhow::{Context as _, Result};
use atrium_api::com::atproto::admin::defs::StatusAttrData;
use atrium_repo::Cid;
use sqlx::{Row, SqlitePool};

/// Reader for blob data in the actor store.
pub(crate) struct BlobReader {
    /// Database connection.
    pub db: SqlitePool,
    /// BlobStore.
    pub blobstore: BlobStore,
}

impl BlobReader {
    /// Create a new blob reader.
    pub(crate) fn new(db: SqlitePool, blobstore: BlobStore) -> Self {
        Self { db, blobstore }
    }

    /// Get metadata for a blob.
    pub(crate) async fn get_blob_metadata(&self, cid: &Cid) -> Result<Option<BlobMetadata>> {
        let cid_str = cid.to_string();
        let found = sqlx::query!(
            r#"
            SELECT mimeType, size, takedownRef
            FROM blob
            WHERE cid = ? AND takedownRef IS NULL
            "#,
            cid_str
        )
        .fetch_optional(&self.db)
        .await
        .context("failed to fetch blob metadata")?;
        if found.is_none() {
            return Err(anyhow::anyhow!("Blob not found")); // InvalidRequestError('Blob not found')
        }
        let found = found.unwrap();
        let size = found.size as u64;
        let mime_type = found.mimeType;
        return Ok(Some(BlobMetadata { size, mime_type }));
    }

    /// Get a blob's full data and metadata.
    pub(crate) async fn get_blob(&self, cid: &Cid) -> Result<Option<BlobData>> {
        let metadata = self.get_blob_metadata(cid).await?;
        let blob_stream = self.blobstore.get_stream(cid).await?;
        if blob_stream.is_none() {
            return Err(anyhow::anyhow!("Blob not found")); // InvalidRequestError('Blob not found')
        }
        let metadata = metadata.unwrap();
        Ok(Some(BlobData {
            size: metadata.size,
            mime_type: Some(metadata.mime_type),
            stream: blob_stream.unwrap(),
        }))
    }

    /// List blobs for a repository.
    pub(crate) async fn list_blobs(&self, opts: ListBlobsOptions) -> Result<Vec<String>> {
        let mut query = sqlx::QueryBuilder::new(
            "SELECT rb.blobCid FROM record_blob rb
             INNER JOIN record r ON r.uri = rb.recordUri",
        );
        if let Some(since) = &opts.since {
            query.push(" WHERE r.repoRev > ").push_bind(since);
        }
        if let Some(cursor) = &opts.cursor {
            query.push(" AND rb.blobCid > ").push_bind(cursor);
        }
        query
            .push(" ORDER BY rb.blobCid ASC")
            .push(" LIMIT ")
            .push_bind(opts.limit);
        let blobs = query
            .build()
            .map(|row: sqlx::sqlite::SqliteRow| row.get::<String, _>(0))
            .fetch_all(&self.db)
            .await
            .context("failed to fetch blobs")?;
        Ok(blobs)
    }

    /// Get takedown status for a blob.
    pub(crate) async fn get_blob_takedown_status(
        &self,
        cid: &Cid,
    ) -> Result<Option<StatusAttrData>> {
        // const res = await this.db.db
        //   .selectFrom('blob')
        //   .select('takedownRef')
        //   .where('cid', '=', cid.toString())
        //   .executeTakeFirst()
        // if (!res) return null
        // return res.takedownRef
        //   ? { applied: true, ref: res.takedownRef }
        //   : { applied: false }
        let cid_str = cid.to_string();
        let result = sqlx::query!(r#"SELECT takedownRef FROM blob WHERE cid = ?"#, cid_str)
            .fetch_optional(&self.db)
            .await
            .context("failed to fetch blob takedown status")?;

        Ok({
            if result.is_none() {
                None
            } else {
                let takedown_ref = result.unwrap().takedownRef.unwrap();
                if takedown_ref == "false" {
                    Some(StatusAttrData {
                        applied: false,
                        r#ref: None,
                    })
                } else {
                    Some(StatusAttrData {
                        applied: true,
                        r#ref: Some(takedown_ref),
                    })
                }
            }
        })
    }

    /// Get records that reference a blob.
    pub(crate) async fn get_records_for_blob(&self, cid: &Cid) -> Result<Vec<String>> {
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
    pub(crate) async fn get_blobs_for_record(&self, record_uri: &str) -> Result<Vec<String>> {
        // const res = await this.db.db
        //   .selectFrom('blob')
        //   .innerJoin('record_blob', 'record_blob.blobCid', 'blob.cid')
        //   .where('recordUri', '=', recordUri)
        //   .select('blob.cid')
        //   .execute()
        // return res.map((row) => row.cid)
        let blobs = sqlx::query!(
            r#"SELECT blob.cid FROM blob INNER JOIN record_blob ON record_blob.blobCid = blob.cid WHERE recordUri = ?"#,
            record_uri
        )
        .fetch_all(&self.db)
        .await
        .context("failed to fetch blobs for record")?;

        Ok(blobs.into_iter().map(|blob| blob.cid).collect())
    }

    /// Count total blobs.
    pub(crate) async fn blob_count(&self) -> Result<i64> {
        let result = sqlx::query!(r#"SELECT COUNT(*) as count FROM blob"#)
            .fetch_one(&self.db)
            .await
            .context("failed to count blobs")?;

        Ok(result.count)
    }

    /// Count distinct blobs referenced by records.
    pub(crate) async fn record_blob_count(&self) -> Result<i64> {
        let result = sqlx::query!(r#"SELECT COUNT(DISTINCT blobCid) as count FROM record_blob"#)
            .fetch_one(&self.db)
            .await
            .context("failed to count record blobs")?;

        Ok(result.count)
    }

    /// List blobs that are referenced but missing from storage.
    pub(crate) async fn list_missing_blobs(
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

    pub(crate) async fn get_blod_cids(&self) -> Result<Vec<Cid>> {
        let rows = sqlx::query!("SELECT cid FROM blob")
            .fetch_all(&self.db)
            .await
            .context("failed to fetch blob CIDs")?;
        Ok(rows
            .into_iter()
            .map(|row| Cid::from_str(&row.cid).unwrap())
            .collect())
    }
}

/// Metadata about a blob.
#[derive(Debug, Clone)]
pub(crate) struct BlobMetadata {
    /// The size of the blob in bytes.
    pub size: u64,
    /// The MIME type of the blob.
    pub mime_type: String,
}

/// Complete blob data with content.
#[derive(Debug)]
pub(crate) struct BlobData {
    /// The size of the blob.
    pub size: u64,
    /// The MIME type of the blob.
    pub mime_type: Option<String>,
    pub stream: BlobStream,
}

/// Options for listing blobs.
#[derive(Debug, Clone)]
pub(crate) struct ListBlobsOptions {
    /// Optional revision to list blobs since.
    pub since: Option<String>,
    /// Optional cursor for pagination.
    pub cursor: Option<String>,
    /// Maximum number of blobs to return.
    pub limit: i64,
}

/// Options for listing missing blobs.
#[derive(Debug, Clone)]
pub(crate) struct ListMissingBlobsOptions {
    /// Optional cursor for pagination.
    pub cursor: Option<String>,
    /// Maximum number of missing blobs to return.
    pub limit: i64,
}

/// Information about a missing blob.
#[derive(Debug, Clone)]
pub(crate) struct MissingBlob {
    /// CID of the missing blob.
    pub cid: String,
    /// URI of the record referencing the missing blob.
    pub record_uri: String,
}
