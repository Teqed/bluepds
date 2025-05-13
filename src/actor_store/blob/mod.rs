// bluepds/src/actor_store/blob/mod.rs

//! Blob storage and retrieval for the actor store.

use std::str::FromStr;

use anyhow::{Context as _, Result, bail};
use atrium_api::com::atproto::admin::defs::StatusAttr;
use atrium_repo::Cid;
use diesel::associations::HasTable as _;
use diesel::prelude::*;
use futures::{StreamExt, future::try_join_all};
use rsky_common::ipld::sha256_raw_to_cid;
use rsky_pds::actor_store::blob::sha256_stream;
use rsky_pds::image::{maybe_get_info, mime_type_from_bytes};
use rsky_pds::schema::pds::*;
use rsky_repo::types::{PreparedBlobRef, PreparedWrite, WriteOpAction};
use sha2::Digest;
use uuid::Uuid;

use crate::actor_store::PreparedWrite as BluePreparedWrite;
use crate::actor_store::db::ActorDb;

/// Background task queue for blob operations
pub mod background;
// Re-export BackgroundQueue
pub use background::BackgroundQueue;

pub mod placeholder;
pub(crate) use placeholder::BlobStorePlaceholder;

/// Type for stream of blob data
pub type BlobStream = Box<dyn std::io::Read + Send>;

/// Blob store interface
pub trait BlobStore: Send + Sync {
    async fn put_temp(&self, bytes: &[u8]) -> Result<String>;
    async fn make_permanent(&self, key: &str, cid: Cid) -> Result<()>;
    async fn put_permanent(&self, cid: Cid, bytes: &[u8]) -> Result<()>;
    async fn quarantine(&self, cid: Cid) -> Result<()>;
    async fn unquarantine(&self, cid: Cid) -> Result<()>;
    async fn get_bytes(&self, cid: Cid) -> Result<Vec<u8>>;
    async fn get_stream(&self, cid: Cid) -> Result<BlobStream>;
    async fn has_temp(&self, key: &str) -> Result<bool>;
    async fn has_stored(&self, cid: Cid) -> Result<bool>;
    async fn delete(&self, cid: Cid) -> Result<()>;
    async fn delete_many(&self, cids: Vec<Cid>) -> Result<()>;
}

/// Blob metadata for upload
pub struct BlobMetadata {
    pub temp_key: String,
    pub size: i64,
    pub cid: Cid,
    pub mime_type: String,
    pub width: Option<i32>,
    pub height: Option<i32>,
}

/// Blob data with content stream
pub struct BlobData {
    pub size: u64,
    pub mime_type: Option<String>,
    pub stream: BlobStream,
}

/// Options for listing blobs
pub struct ListBlobsOptions {
    pub since: Option<String>,
    pub cursor: Option<String>,
    pub limit: i64,
}

/// Options for listing missing blobs
pub struct ListMissingBlobsOptions {
    pub cursor: Option<String>,
    pub limit: i64,
}

/// Information about a missing blob
pub struct MissingBlob {
    pub cid: String,
    pub record_uri: String,
}

/// Unified handler for blob operations
pub struct BlobHandler {
    /// Database connection
    pub db: ActorDb,
    /// DID of the actor
    pub did: String,
    /// Blob store implementation
    pub blobstore: Box<dyn BlobStore>,
    /// Background queue for async operations
    pub background_queue: Option<background::BackgroundQueue>,
}

impl BlobHandler {
    /// Create a new blob handler with background queue for write operations
    pub fn new(
        db: ActorDb,
        blobstore: impl BlobStore + 'static,
        background_queue: background::BackgroundQueue,
        did: String,
    ) -> Self {
        Self {
            db,
            did,
            blobstore: Box::new(blobstore),
            background_queue: Some(background_queue),
        }
    }

    /// Get metadata for a blob
    pub async fn get_blob_metadata(&self, cid: &Cid) -> Result<BlobMetadata> {
        let cid_str = cid.to_string();
        let did = self.did.clone();

        let found = self
            .db
            .run(move |conn| {
                blob::table
                    .filter(blob::cid.eq(&cid_str))
                    .filter(blob::did.eq(&did))
                    .filter(blob::takedownRef.is_null())
                    .first::<BlobModel>(conn)
                    .optional()
            })
            .await?;

        match found {
            Some(found) => Ok(BlobMetadata {
                temp_key: found.temp_key.unwrap_or_default(),
                size: found.size as i64,
                cid: Cid::from_str(&found.cid)?,
                mime_type: found.mime_type,
                width: found.width,
                height: found.height,
            }),
            None => bail!("Blob not found"),
        }
    }

    /// Get a blob's complete data
    pub async fn get_blob(&self, cid: &Cid) -> Result<BlobData> {
        let metadata = self.get_blob_metadata(cid).await?;
        let blob_stream = self.blobstore.get_stream(*cid).await?;

        Ok(BlobData {
            size: metadata.size as u64,
            mime_type: Some(metadata.mime_type),
            stream: blob_stream,
        })
    }

    /// List blobs for a repository
    pub async fn list_blobs(&self, opts: ListBlobsOptions) -> Result<Vec<String>> {
        let did = self.did.clone();
        let since = opts.since;
        let cursor = opts.cursor;
        let limit = opts.limit;

        self.db
            .run(move |conn| {
                let mut query = record_blob::table
                    .inner_join(
                        crate::schema::record::table
                            .on(crate::schema::record::uri.eq(record_blob::record_uri)),
                    )
                    .filter(record_blob::did.eq(&did))
                    .select(record_blob::blob_cid)
                    .distinct()
                    .order(record_blob::blob_cid.asc())
                    .limit(limit)
                    .into_boxed();

                if let Some(since_val) = since {
                    query = query.filter(crate::schema::record::repo_rev.gt(since_val));
                }

                if let Some(cursor_val) = cursor {
                    query = query.filter(record_blob::blob_cid.gt(cursor_val));
                }

                query.load::<String>(conn)
            })
            .await
    }

    /// Get records that reference a blob
    pub async fn get_records_for_blob(&self, cid: &Cid) -> Result<Vec<String>> {
        let cid_str = cid.to_string();
        let did = self.did.clone();

        self.db
            .run(move |conn| {
                record_blob::table
                    .filter(record_blob::blob_cid.eq(&cid_str))
                    .filter(record_blob::did.eq(&did))
                    .select(record_blob::record_uri)
                    .load::<String>(conn)
            })
            .await
    }

    /// Get blobs referenced by a record
    pub async fn get_blobs_for_record(&self, record_uri: &str) -> Result<Vec<String>> {
        let record_uri_str = record_uri.to_string();
        let did = self.did.clone();

        self.db
            .run(move |conn| {
                blob::table
                    .inner_join(record_blob::table.on(record_blob::blob_cid.eq(blob::cid)))
                    .filter(record_blob::record_uri.eq(&record_uri_str))
                    .filter(blob::did.eq(&did))
                    .select(blob::cid)
                    .load::<String>(conn)
            })
            .await
    }

    /// Upload a blob and get its metadata
    pub async fn upload_blob_and_get_metadata(
        &self,
        user_suggested_mime: &str,
        blob_bytes: &[u8],
    ) -> Result<BlobMetadata> {
        let temp_key = self.blobstore.put_temp(blob_bytes).await?;
        let size = blob_bytes.len() as i64;
        let sha256 = sha256_stream(blob_bytes).await?;
        let img_info = maybe_get_info(blob_bytes).await?;
        let sniffed_mime = mime_type_from_bytes(blob_bytes).await?;
        let cid = sha256_raw_to_cid(sha256);
        let mime_type = sniffed_mime.unwrap_or_else(|| user_suggested_mime.to_string());

        Ok(BlobMetadata {
            temp_key,
            size,
            cid,
            mime_type,
            width: img_info.as_ref().map(|info| info.width as i32),
            height: img_info.as_ref().map(|info| info.height as i32),
        })
    }

    /// Count total blobs
    pub async fn blob_count(&self) -> Result<i64> {
        let did = self.did.clone();

        self.db
            .run(move |conn| {
                blob::table
                    .filter(blob::did.eq(&did))
                    .count()
                    .get_result(conn)
            })
            .await
    }

    /// Count distinct blobs referenced by records
    pub async fn record_blob_count(&self) -> Result<i64> {
        let did = self.did.clone();

        self.db
            .run(move |conn| {
                record_blob::table
                    .filter(record_blob::did.eq(&did))
                    .select(diesel::dsl::count_distinct(record_blob::blob_cid))
                    .first::<i64>(conn)
            })
            .await
    }

    /// List blobs that are referenced but missing from storage
    pub async fn list_missing_blobs(
        &self,
        opts: ListMissingBlobsOptions,
    ) -> Result<Vec<MissingBlob>> {
        let did = self.did.clone();
        let limit = opts.limit;
        let cursor = opts.cursor;

        self.db
            .run(move |conn| {
                let mut query = record_blob::table
                    .left_join(
                        blob::table.on(blob::cid.eq(record_blob::blob_cid).and(blob::did.eq(&did))),
                    )
                    .filter(record_blob::did.eq(&did))
                    .filter(blob::cid.is_null())
                    .select((record_blob::blob_cid, record_blob::record_uri))
                    .order(record_blob::blob_cid.asc())
                    .limit(limit)
                    .into_boxed();

                if let Some(cursor_val) = cursor {
                    query = query.filter(record_blob::blob_cid.gt(cursor_val));
                }

                let results = query.load::<(String, String)>(conn)?;

                Ok(results
                    .into_iter()
                    .map(|(cid, record_uri)| MissingBlob { cid, record_uri })
                    .collect())
            })
            .await
    }

    /// Get takedown status for a blob
    pub async fn get_blob_takedown_status(&self, cid: &Cid) -> Result<Option<StatusAttr>> {
        let cid_str = cid.to_string();
        let did = self.did.clone();

        self.db
            .run(move |conn| {
                let result = blob::table
                    .filter(blob::cid.eq(&cid_str))
                    .filter(blob::did.eq(&did))
                    .select(blob::takedownRef)
                    .first::<Option<String>>(conn)
                    .optional()?;

                match result {
                    Some(takedown) => match takedown {
                        Some(takedownRef) => Ok(Some(StatusAttr {
                            applied: true,
                            r#ref: Some(takedownRef),
                        })),
                        None => Ok(Some(StatusAttr {
                            applied: false,
                            r#ref: None,
                        })),
                    },
                    None => Ok(None),
                }
            })
            .await
    }

    /// Get all blob CIDs in the repository
    pub async fn get_blob_cids(&self) -> Result<Vec<Cid>> {
        let did = self.did.clone();

        let rows = self
            .db
            .run(move |conn| {
                blob::table
                    .filter(blob::did.eq(&did))
                    .select(blob::cid)
                    .load::<String>(conn)
            })
            .await?;

        rows.into_iter()
            .map(|cid_str| Cid::from_str(&cid_str).context("Invalid CID format"))
            .collect()
    }

    /// Track a blob that's not yet associated with a record
    pub async fn track_untethered_blob(&self, metadata: &BlobMetadata) -> Result<()> {
        let cid_str = metadata.cid.to_string();
        let did = self.did.clone();

        // Check if blob exists and is taken down
        let existing = self
            .db
            .run({
                let cid_str_clone = cid_str.clone();
                let did_clone = did.clone();

                move |conn| {
                    blob::table
                        .filter(blob::did.eq(&did_clone))
                        .filter(blob::cid.eq(&cid_str_clone))
                        .select(blob::takedownRef)
                        .first::<Option<String>>(conn)
                        .optional()
                }
            })
            .await?;

        if let Some(row) = existing {
            if row.is_some() {
                return Err(anyhow::anyhow!(
                    "Blob has been taken down, cannot re-upload"
                ));
            }
        }

        let size = metadata.size as i32;
        let now = chrono::Utc::now().to_rfc3339();
        let mime_type = metadata.mime_type.clone();
        let temp_key = metadata.temp_key.clone();
        let width = metadata.width;
        let height = metadata.height;

        self.db.run(move |conn| {
            diesel::insert_into(blob::table)
                .values((
                    blob::cid.eq(&cid_str),
                    blob::did.eq(&did),
                    blob::mime_type.eq(&mime_type),
                    blob::size.eq(size),
                    blob::temp_key.eq(&temp_key),
                    blob::width.eq(width),
                    blob::height.eq(height),
                    blob::created_at.eq(&now),
                ))
                .on_conflict((blob::cid, blob::did))
                .do_update()
                .set(
                    blob::temp_key.eq(
                        diesel::dsl::sql::<diesel::sql_types::Text>(
                            "CASE WHEN blob.temp_key IS NULL THEN excluded.temp_key ELSE blob.temp_key END"
                        )
                    )
                )
                .execute(conn)
                .context("Failed to track untethered blob")
        }).await?;

        Ok(())
    }

    /// Process blobs for repository writes
    pub async fn process_write_blobs(&self, rev: &str, writes: Vec<PreparedWrite>) -> Result<()> {
        self.delete_dereferenced_blobs(writes.clone()).await?;

        let futures = writes.iter().filter_map(|write| match write {
            PreparedWrite::Create(w) | PreparedWrite::Update(w) => {
                let blobs = &w.blobs;
                let uri = w.uri.clone();
                let handler = self;

                Some(async move {
                    for blob in blobs {
                        handler.verify_blob_and_make_permanent(blob).await?;
                        handler.associate_blob(blob, &uri).await?;
                    }
                    Ok(())
                })
            }
            _ => None,
        });

        try_join_all(futures).await?;

        Ok(())
    }

    /// Delete blobs that are no longer referenced
    pub async fn delete_dereferenced_blobs(&self, writes: Vec<PreparedWrite>) -> Result<()> {
        let uris: Vec<String> = writes
            .iter()
            .filter_map(|w| match w {
                PreparedWrite::Delete(w) => Some(w.uri.clone()),
                PreparedWrite::Update(w) => Some(w.uri.clone()),
                _ => None,
            })
            .collect();

        if uris.is_empty() {
            return Ok(());
        }

        let did = self.did.clone();

        // Delete record-blob associations
        let deleted_repo_blobs = self
            .db
            .run({
                let uris_clone = uris.clone();
                let did_clone = did.clone();

                move |conn| {
                    let query = diesel::delete(record_blob::table)
                        .filter(record_blob::did.eq(&did_clone))
                        .filter(record_blob::record_uri.eq_any(&uris_clone))
                        .returning(RecordBlob::as_returning());

                    query.load(conn)
                }
            })
            .await?;

        if deleted_repo_blobs.is_empty() {
            return Ok(());
        }

        // Collect deleted blob CIDs
        let deleted_repo_blob_cids: Vec<String> = deleted_repo_blobs
            .iter()
            .map(|rb| rb.blob_cid.clone())
            .collect();

        // Find duplicates in record_blob table
        let duplicate_cids = self
            .db
            .run({
                let blob_cids = deleted_repo_blob_cids.clone();
                let did_clone = did.clone();

                move |conn| {
                    record_blob::table
                        .filter(record_blob::did.eq(&did_clone))
                        .filter(record_blob::blob_cid.eq_any(&blob_cids))
                        .select(record_blob::blob_cid)
                        .load::<String>(conn)
                }
            })
            .await?;

        // Get new blob CIDs from writes
        let new_blob_cids: Vec<String> = writes
            .iter()
            .filter_map(|w| match w {
                PreparedWrite::Create(w) | PreparedWrite::Update(w) => Some(
                    w.blobs
                        .iter()
                        .map(|b| b.cid.to_string())
                        .collect::<Vec<String>>(),
                ),
                _ => None,
            })
            .flatten()
            .collect();

        // Determine which CIDs to keep and which to delete
        let cids_to_keep: std::collections::HashSet<String> = new_blob_cids
            .into_iter()
            .chain(duplicate_cids.into_iter())
            .collect();

        let cids_to_delete: Vec<String> = deleted_repo_blob_cids
            .into_iter()
            .filter(|cid| !cids_to_keep.contains(cid))
            .collect();

        if cids_to_delete.is_empty() {
            return Ok(());
        }

        // Delete blobs from the database
        self.db
            .run({
                let cids = cids_to_delete.clone();
                let did_clone = did.clone();

                move |conn| {
                    diesel::delete(blob::table)
                        .filter(blob::did.eq(&did_clone))
                        .filter(blob::cid.eq_any(&cids))
                        .execute(conn)
                }
            })
            .await?;

        // Delete blobs from storage
        let cids_to_delete_objects: Vec<Cid> = cids_to_delete
            .iter()
            .filter_map(|cid_str| Cid::from_str(cid_str).ok())
            .collect();

        // Use background queue if available
        if let Some(queue) = &self.background_queue {
            let blobstore = self.blobstore.clone();
            queue
                .add(async move {
                    let _ = blobstore.delete_many(cids_to_delete_objects).await;
                })
                .await;
        } else {
            // Otherwise delete directly
            if !cids_to_delete_objects.is_empty() {
                self.blobstore.delete_many(cids_to_delete_objects).await?;
            }
        }

        Ok(())
    }

    /// Verify blob integrity and move from temporary to permanent storage
    pub async fn verify_blob_and_make_permanent(&self, blob: &PreparedBlobRef) -> Result<()> {
        let cid_str = blob.cid.to_string();
        let did = self.did.clone();

        let found = self
            .db
            .run(move |conn| {
                blob::table
                    .filter(blob::did.eq(&did))
                    .filter(blob::cid.eq(&cid_str))
                    .filter(blob::takedownRef.is_null())
                    .first::<BlobModel>(conn)
                    .optional()
            })
            .await?;

        let found = match found {
            Some(b) => b,
            None => bail!("Blob not found: {}", cid_str),
        };

        // Verify blob constraints
        if let Some(max_size) = blob.constraints.max_size {
            if found.size as usize > max_size {
                bail!(
                    "BlobTooLarge: This file is too large. It is {} but the maximum size is {}",
                    found.size,
                    max_size
                );
            }
        }

        if blob.mime_type != found.mime_type {
            bail!(
                "InvalidMimeType: Referenced MIME type does not match stored blob. Expected: {}, Got: {}",
                found.mime_type,
                blob.mime_type
            );
        }

        if let Some(ref accept) = blob.constraints.accept {
            if !accepted_mime(&blob.mime_type, accept).await {
                bail!(
                    "Wrong type of file. It is {} but it must match {:?}.",
                    blob.mime_type,
                    accept
                );
            }
        }

        // Move blob from temporary to permanent storage if needed
        if let Some(temp_key) = found.temp_key {
            self.blobstore.make_permanent(&temp_key, blob.cid).await?;

            // Update database to clear temp key
            let cid_str = blob.cid.to_string();
            let did = self.did.clone();

            self.db
                .run(move |conn| {
                    diesel::update(blob::table)
                        .filter(blob::did.eq(&did))
                        .filter(blob::cid.eq(&cid_str))
                        .set(blob::temp_key.eq::<Option<String>>(None))
                        .execute(conn)
                })
                .await?;
        }

        Ok(())
    }

    /// Associate a blob with a record
    pub async fn associate_blob(&self, blob: &PreparedBlobRef, record_uri: &str) -> Result<()> {
        let cid_str = blob.cid.to_string();
        let record_uri = record_uri.to_string();
        let did = self.did.clone();

        self.db
            .run(move |conn| {
                diesel::insert_into(record_blob::table)
                    .values((
                        record_blob::blob_cid.eq(&cid_str),
                        record_blob::record_uri.eq(&record_uri),
                        record_blob::did.eq(&did),
                    ))
                    .on_conflict_do_nothing()
                    .execute(conn)
            })
            .await?;

        Ok(())
    }

    /// Update takedown status for a blob
    pub async fn update_blob_takedown_status(&self, blob: Cid, takedown: StatusAttr) -> Result<()> {
        let cid_str = blob.to_string();
        let did = self.did.clone();

        let takedownRef: Option<String> = if takedown.applied {
            Some(takedown.r#ref.unwrap_or_else(|| Uuid::new_v4().to_string()))
        } else {
            None
        };

        // Update database
        self.db
            .run(move |conn| {
                diesel::update(blob::table)
                    .filter(blob::did.eq(&did))
                    .filter(blob::cid.eq(&cid_str))
                    .set(blob::takedownRef.eq(takedownRef))
                    .execute(conn)
            })
            .await?;

        // Update blob storage
        if takedown.applied {
            self.blobstore.quarantine(blob).await?;
        } else {
            self.blobstore.unquarantine(blob).await?;
        }

        Ok(())
    }
}

/// Verify MIME type against accepted formats
async fn accepted_mime(mime: &str, accepted: &[String]) -> bool {
    // Accept any type
    if accepted.contains(&"*/*".to_string()) {
        return true;
    }

    // Check for glob patterns (e.g., "image/*")
    for glob in accepted {
        if glob.ends_with("/*") {
            let prefix = glob.split('/').next().unwrap();
            if mime.starts_with(&format!("{}/", prefix)) {
                return true;
            }
        }
    }

    // Check for exact match
    accepted.contains(&mime.to_string())
}
