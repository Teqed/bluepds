//! Blob operations for the actor store
//! Based on https://github.com/blacksky-algorithms/rsky/blob/main/rsky-pds/src/actor_store/blob/mod.rs
//! blacksky-algorithms/rsky is licensed under the Apache License 2.0
//!
//! Modified for SQLite backend

use anyhow::{Result, bail};
use cidv10::Cid;
use diesel::dsl::{count_distinct, exists, not};
use diesel::sql_types::{Integer, Nullable, Text};
use diesel::*;
use futures::{
    stream::{self, StreamExt},
    try_join,
};
use rsky_common::ipld::sha256_raw_to_cid;
use rsky_common::now;
use rsky_lexicon::blob_refs::BlobRef;
use rsky_lexicon::com::atproto::admin::StatusAttr;
use rsky_lexicon::com::atproto::repo::ListMissingBlobsRefRecordBlob;
use rsky_pds::actor_store::blob::{
    BlobMetadata, GetBlobMetadataOutput, ListBlobsOpts, ListMissingBlobsOpts, sha256_stream,
    verify_blob,
};
use rsky_pds::image;
use rsky_pds::models::models;
use rsky_repo::error::BlobError;
use rsky_repo::types::{PreparedBlobRef, PreparedWrite};
use std::str::FromStr as _;

use super::sql_blob::{BlobStoreSql, ByteStream};

pub struct GetBlobOutput {
    pub size: i32,
    pub mime_type: Option<String>,
    pub stream: ByteStream,
}

/// Handles blob operations for an actor store
pub struct BlobReader {
    /// SQL-based blob storage
    pub blobstore: BlobStoreSql,
    /// DID of the actor
    pub did: String,
    /// Database connection
    pub db: deadpool_diesel::Pool<
        deadpool_diesel::Manager<SqliteConnection>,
        deadpool_diesel::sqlite::Object,
    >,
}

impl BlobReader {
    /// Create a new blob reader
    pub fn new(
        blobstore: BlobStoreSql,
        db: deadpool_diesel::Pool<
            deadpool_diesel::Manager<SqliteConnection>,
            deadpool_diesel::sqlite::Object,
        >,
    ) -> Self {
        Self {
            did: blobstore.did.clone(),
            blobstore,
            db,
        }
    }

    /// Get metadata for a blob by CID
    pub async fn get_blob_metadata(&self, cid: Cid) -> Result<GetBlobMetadataOutput> {
        use rsky_pds::schema::pds::blob::dsl as BlobSchema;

        let did = self.did.clone();
        let found = self
            .db
            .get()
            .await?
            .interact(move |conn| {
                BlobSchema::blob
                    .filter(BlobSchema::did.eq(did))
                    .filter(BlobSchema::cid.eq(cid.to_string()))
                    .filter(BlobSchema::takedownRef.is_null())
                    .select(models::Blob::as_select())
                    .first(conn)
                    .optional()
            })
            .await
            .expect("Failed to get blob metadata")?;

        match found {
            None => bail!("Blob not found"),
            Some(found) => Ok(GetBlobMetadataOutput {
                size: found.size,
                mime_type: Some(found.mime_type),
            }),
        }
    }

    /// Get a blob by CID with metadata and content
    pub async fn get_blob(&self, cid: Cid) -> Result<GetBlobOutput> {
        let metadata = self.get_blob_metadata(cid).await?;
        let blob_stream = match self.blobstore.get_stream(cid).await {
            Ok(stream) => stream,
            Err(e) => bail!("Failed to get blob: {}", e),
        };

        Ok(GetBlobOutput {
            size: metadata.size,
            mime_type: metadata.mime_type,
            stream: blob_stream,
        })
    }

    /// Get all records that reference a specific blob
    pub async fn get_records_for_blob(&self, cid: Cid) -> Result<Vec<String>> {
        use rsky_pds::schema::pds::record_blob::dsl as RecordBlobSchema;

        let did = self.did.clone();
        let res = self
            .db
            .get()
            .await?
            .interact(move |conn| {
                let results = RecordBlobSchema::record_blob
                    .filter(RecordBlobSchema::blobCid.eq(cid.to_string()))
                    .filter(RecordBlobSchema::did.eq(did))
                    .select(models::RecordBlob::as_select())
                    .get_results(conn)?;
                Ok::<_, result::Error>(results.into_iter().map(|row| row.record_uri))
            })
            .await
            .expect("Failed to get records for blob")?
            .collect::<Vec<String>>();

        Ok(res)
    }

    /// Upload a blob and get its metadata
    pub async fn upload_blob_and_get_metadata(
        &self,
        user_suggested_mime: String,
        blob: Vec<u8>,
    ) -> Result<BlobMetadata> {
        let bytes = blob;
        let size = bytes.len() as i64;

        let (temp_key, sha256, img_info, sniffed_mime) = try_join!(
            self.blobstore.put_temp(bytes.clone()),
            sha256_stream(bytes.clone()),
            image::maybe_get_info(bytes.clone()),
            image::mime_type_from_bytes(bytes.clone())
        )?;

        let cid = sha256_raw_to_cid(sha256);
        let mime_type = sniffed_mime.unwrap_or(user_suggested_mime);

        Ok(BlobMetadata {
            temp_key,
            size,
            cid,
            mime_type,
            width: img_info.as_ref().map(|info| info.width as i32),
            height: if let Some(info) = img_info {
                Some(info.height as i32)
            } else {
                None
            },
        })
    }

    /// Track a blob that hasn't been associated with any records yet
    pub async fn track_untethered_blob(&self, metadata: BlobMetadata) -> Result<BlobRef> {
        use rsky_pds::schema::pds::blob::dsl as BlobSchema;

        let did = self.did.clone();
        self.db.get().await?.interact(move |conn| {
            let BlobMetadata {
                temp_key,
                size,
                cid,
                mime_type,
                width,
                height,
            } = metadata;
            let created_at = now();

            let found = BlobSchema::blob
                .filter(BlobSchema::did.eq(&did))
                .filter(BlobSchema::cid.eq(&cid.to_string()))
                .select(models::Blob::as_select())
                .first(conn)
                .optional()?;

            if let Some(found) = found {
                if found.takedown_ref.is_some() {
                    bail!("Blob has been takendown, cannot re-upload")
                }
            }

            let upsert = sql_query("INSERT INTO pds.blob (cid, did, \"mimeType\", size, \"tempKey\", width, height, \"createdAt\", \"takedownRef\") \
        VALUES \
            ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
        ON CONFLICT (cid, did) DO UPDATE \
        SET \"tempKey\" = EXCLUDED.\"tempKey\" \
            WHERE pds.blob.\"tempKey\" is not null;");
            #[expect(trivial_casts)]
            let _ = upsert
                .bind::<Text, _>(&cid.to_string())
                .bind::<Text, _>(&did)
                .bind::<Text, _>(&mime_type)
                .bind::<Integer, _>(size as i32)
                .bind::<Nullable<Text>, _>(Some(temp_key))
                .bind::<Nullable<Integer>, _>(width)
                .bind::<Nullable<Integer>, _>(height)
                .bind::<Text, _>(created_at)
                .bind::<Nullable<Text>, _>(None as Option<String>)
                .execute(conn)?;

            Ok(BlobRef::new(cid, mime_type, size, None))
        }).await.expect("Failed to track untethered blob")
    }

    /// Process blobs associated with writes
    pub async fn process_write_blobs(&self, writes: Vec<PreparedWrite>) -> Result<()> {
        self.delete_dereferenced_blobs(writes.clone()).await?;

        drop(
            stream::iter(writes)
                .then(async move |write| {
                    match write {
                        PreparedWrite::Create(w) => {
                            for blob in w.blobs {
                                self.verify_blob_and_make_permanent(blob.clone()).await?;
                                self.associate_blob(blob, w.uri.clone()).await?;
                            }
                        }
                        PreparedWrite::Update(w) => {
                            for blob in w.blobs {
                                self.verify_blob_and_make_permanent(blob.clone()).await?;
                                self.associate_blob(blob, w.uri.clone()).await?;
                            }
                        }
                        _ => (),
                    };
                    Ok::<(), anyhow::Error>(())
                })
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?,
        );

        Ok(())
    }

    /// Delete blobs that are no longer referenced by any records
    pub async fn delete_dereferenced_blobs(&self, writes: Vec<PreparedWrite>) -> Result<()> {
        use rsky_pds::schema::pds::blob::dsl as BlobSchema;
        use rsky_pds::schema::pds::record_blob::dsl as RecordBlobSchema;

        // Extract URIs
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

        // In SQLite, we can't do DELETE...RETURNING
        // So we need to fetch the records first, then delete
        let did = self.did.clone();
        let uris_clone = uris.clone();
        let deleted_repo_blobs: Vec<models::RecordBlob> = self
            .db
            .get()
            .await?
            .interact(move |conn| {
                RecordBlobSchema::record_blob
                    .filter(RecordBlobSchema::recordUri.eq_any(&uris_clone))
                    .filter(RecordBlobSchema::did.eq(&did))
                    .load::<models::RecordBlob>(conn)
            })
            .await
            .expect("Failed to get deleted repo blobs")?;

        if deleted_repo_blobs.is_empty() {
            return Ok(());
        }

        // Now perform the delete
        let uris_clone = uris.clone();
        _ = self
            .db
            .get()
            .await?
            .interact(move |conn| {
                delete(RecordBlobSchema::record_blob)
                    .filter(RecordBlobSchema::recordUri.eq_any(uris_clone))
                    .execute(conn)
            })
            .await
            .expect("Failed to delete repo blobs")?;

        // Extract blob cids from the deleted records
        let deleted_repo_blob_cids: Vec<String> = deleted_repo_blobs
            .into_iter()
            .map(|row| row.blob_cid)
            .collect();

        // Find duplicates (blobs referenced by other records)
        let cids_clone = deleted_repo_blob_cids.clone();
        let did_clone = self.did.clone();
        let duplicated_cids: Vec<String> = self
            .db
            .get()
            .await?
            .interact(move |conn| {
                RecordBlobSchema::record_blob
                    .filter(RecordBlobSchema::blobCid.eq_any(cids_clone))
                    .filter(RecordBlobSchema::did.eq(did_clone))
                    .select(RecordBlobSchema::blobCid)
                    .load::<String>(conn)
            })
            .await
            .expect("Failed to get duplicated cids")?;

        // Extract new blob cids from writes (creates and updates)
        let new_blob_cids: Vec<String> = writes
            .iter()
            .flat_map(|w| match w {
                PreparedWrite::Create(w) => w.blobs.clone(),
                PreparedWrite::Update(w) => w.blobs.clone(),
                _ => Vec::new(),
            })
            .map(|b| b.cid.to_string())
            .collect();

        // Determine which blobs to keep vs delete
        let cids_to_keep: Vec<String> = [&new_blob_cids[..], &duplicated_cids[..]].concat();
        let cids_to_delete: Vec<String> = deleted_repo_blob_cids
            .into_iter()
            .filter(|cid| !cids_to_keep.contains(cid))
            .collect();

        if cids_to_delete.is_empty() {
            return Ok(());
        }

        // Delete from the blob table
        let cids = cids_to_delete.clone();
        let did_clone = self.did.clone();
        _ = self
            .db
            .get()
            .await?
            .interact(move |conn| {
                delete(BlobSchema::blob)
                    .filter(BlobSchema::cid.eq_any(cids))
                    .filter(BlobSchema::did.eq(did_clone))
                    .execute(conn)
            })
            .await
            .expect("Failed to delete blobs")?;

        // Delete from blob storage
        // Ideally we'd use a background queue here, but for now:
        drop(
            stream::iter(cids_to_delete)
                .then(async move |cid| match Cid::from_str(&cid) {
                    Ok(cid) => self.blobstore.delete(cid.to_string()).await,
                    Err(e) => Err(anyhow::Error::new(e)),
                })
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?,
        );

        Ok(())
    }

    /// Verify a blob and make it permanent
    pub async fn verify_blob_and_make_permanent(&self, blob: PreparedBlobRef) -> Result<()> {
        use rsky_pds::schema::pds::blob::dsl as BlobSchema;

        let found = self
            .db
            .get()
            .await?
            .interact(move |conn| {
                BlobSchema::blob
                    .filter(
                        BlobSchema::cid
                            .eq(blob.cid.to_string())
                            .and(BlobSchema::takedownRef.is_null()),
                    )
                    .select(models::Blob::as_select())
                    .first(conn)
                    .optional()
            })
            .await
            .expect("Failed to verify blob")?;

        if let Some(found) = found {
            verify_blob(&blob, &found).await?;
            if let Some(ref temp_key) = found.temp_key {
                self.blobstore
                    .make_permanent(temp_key.clone(), blob.cid)
                    .await?;
            }
            _ = self
                .db
                .get()
                .await?
                .interact(move |conn| {
                    update(BlobSchema::blob)
                        .filter(BlobSchema::tempKey.eq(found.temp_key))
                        .set(BlobSchema::tempKey.eq::<Option<String>>(None))
                        .execute(conn)
                })
                .await
                .expect("Failed to update blob")?;
            Ok(())
        } else {
            bail!("Could not find blob: {:?}", blob.cid.to_string())
        }
    }

    /// Associate a blob with a record
    pub async fn associate_blob(&self, blob: PreparedBlobRef, record_uri: String) -> Result<()> {
        use rsky_pds::schema::pds::record_blob::dsl as RecordBlobSchema;

        let cid = blob.cid.to_string();
        let did = self.did.clone();

        _ = self
            .db
            .get()
            .await?
            .interact(move |conn| {
                insert_into(RecordBlobSchema::record_blob)
                    .values((
                        RecordBlobSchema::blobCid.eq(cid),
                        RecordBlobSchema::recordUri.eq(record_uri),
                        RecordBlobSchema::did.eq(&did),
                    ))
                    .on_conflict_do_nothing()
                    .execute(conn)
            })
            .await
            .expect("Failed to associate blob")?;

        Ok(())
    }

    /// Count all blobs for this actor
    pub async fn blob_count(&self) -> Result<i64> {
        use rsky_pds::schema::pds::blob::dsl as BlobSchema;

        let did = self.did.clone();
        self.db
            .get()
            .await?
            .interact(move |conn| {
                let res = BlobSchema::blob
                    .filter(BlobSchema::did.eq(&did))
                    .count()
                    .get_result(conn)?;
                Ok(res)
            })
            .await
            .expect("Failed to count blobs")
    }

    /// Count blobs associated with records
    pub async fn record_blob_count(&self) -> Result<i64> {
        use rsky_pds::schema::pds::record_blob::dsl as RecordBlobSchema;

        let did = self.did.clone();
        self.db
            .get()
            .await?
            .interact(move |conn| {
                let res: i64 = RecordBlobSchema::record_blob
                    .filter(RecordBlobSchema::did.eq(&did))
                    .select(count_distinct(RecordBlobSchema::blobCid))
                    .get_result(conn)?;
                Ok(res)
            })
            .await
            .expect("Failed to count record blobs")
    }

    /// List blobs that are referenced but missing
    pub async fn list_missing_blobs(
        &self,
        opts: ListMissingBlobsOpts,
    ) -> Result<Vec<ListMissingBlobsRefRecordBlob>> {
        use rsky_pds::schema::pds::blob::dsl as BlobSchema;
        use rsky_pds::schema::pds::record_blob::dsl as RecordBlobSchema;

        let did = self.did.clone();
        self.db
            .get()
            .await?
            .interact(move |conn| {
                let ListMissingBlobsOpts { cursor, limit } = opts;

                if limit > 1000 {
                    bail!("Limit too high. Max: 1000.");
                }

                // TODO: Improve this query

                // SQLite doesn't support DISTINCT ON, so we use GROUP BY instead
                let query = RecordBlobSchema::record_blob
                    .filter(not(exists(
                        BlobSchema::blob
                            .filter(BlobSchema::cid.eq(RecordBlobSchema::blobCid))
                            .filter(BlobSchema::did.eq(&did)),
                    )))
                    .filter(RecordBlobSchema::did.eq(&did))
                    .into_boxed();

                // Apply cursor filtering if provided
                let query = if let Some(cursor) = cursor {
                    query.filter(RecordBlobSchema::blobCid.gt(cursor))
                } else {
                    query
                };

                // For SQLite, use a simplified approach without GROUP BY to avoid recursion limit issues
                let res = query
                    .select((RecordBlobSchema::blobCid, RecordBlobSchema::recordUri))
                    .order(RecordBlobSchema::blobCid.asc())
                    .limit(limit as i64)
                    .load::<(String, String)>(conn)?;

                // Process results to get distinct cids with their first record URI
                let mut result = Vec::new();
                let mut last_cid = None;

                for (cid, uri) in res {
                    if last_cid.as_ref() != Some(&cid) {
                        result.push(ListMissingBlobsRefRecordBlob {
                            cid: cid.clone(),
                            record_uri: uri,
                        });
                        last_cid = Some(cid);
                    }
                }

                Ok(result)
            })
            .await
            .expect("Failed to list missing blobs")
    }

    /// List all blobs with optional filtering
    pub async fn list_blobs(&self, opts: ListBlobsOpts) -> Result<Vec<String>> {
        use rsky_pds::schema::pds::record::dsl as RecordSchema;
        use rsky_pds::schema::pds::record_blob::dsl as RecordBlobSchema;

        let ListBlobsOpts {
            since,
            cursor,
            limit,
        } = opts;

        let res: Vec<String> = if let Some(since) = since {
            let mut builder = RecordBlobSchema::record_blob
                .inner_join(
                    RecordSchema::record.on(RecordSchema::uri.eq(RecordBlobSchema::recordUri)),
                )
                .filter(RecordSchema::repoRev.gt(since))
                .select(RecordBlobSchema::blobCid)
                .distinct()
                .order(RecordBlobSchema::blobCid.asc())
                .limit(limit as i64)
                .into_boxed();

            if let Some(cursor) = cursor {
                builder = builder.filter(RecordBlobSchema::blobCid.gt(cursor));
            }
            self.db
                .get()
                .await?
                .interact(move |conn| builder.load(conn))
                .await
                .expect("Failed to list blobs")?
        } else {
            let mut builder = RecordBlobSchema::record_blob
                .select(RecordBlobSchema::blobCid)
                .distinct()
                .order(RecordBlobSchema::blobCid.asc())
                .limit(limit as i64)
                .into_boxed();

            if let Some(cursor) = cursor {
                builder = builder.filter(RecordBlobSchema::blobCid.gt(cursor));
            }
            self.db
                .get()
                .await?
                .interact(move |conn| builder.load(conn))
                .await
                .expect("Failed to list blobs")?
        };

        Ok(res)
    }

    /// Get the takedown status of a blob
    pub async fn get_blob_takedown_status(&self, cid: Cid) -> Result<Option<StatusAttr>> {
        use rsky_pds::schema::pds::blob::dsl as BlobSchema;

        self.db
            .get()
            .await?
            .interact(move |conn| {
                let res = BlobSchema::blob
                    .filter(BlobSchema::cid.eq(cid.to_string()))
                    .select(models::Blob::as_select())
                    .first(conn)
                    .optional()?;

                match res {
                    None => Ok(None),
                    Some(res) => res.takedown_ref.map_or_else(
                        || {
                            Ok(Some(StatusAttr {
                                applied: false,
                                r#ref: None,
                            }))
                        },
                        |takedown_ref| {
                            Ok(Some(StatusAttr {
                                applied: true,
                                r#ref: Some(takedown_ref),
                            }))
                        },
                    ),
                }
            })
            .await
            .expect("Failed to get blob takedown status")
    }

    /// Update the takedown status of a blob
    pub async fn update_blob_takedown_status(&self, blob: Cid, takedown: StatusAttr) -> Result<()> {
        use rsky_pds::schema::pds::blob::dsl as BlobSchema;

        let takedown_ref: Option<String> = match takedown.applied {
            true => takedown.r#ref.map_or_else(|| Some(now()), Some),
            false => None,
        };

        let blob_cid = blob.to_string();
        let did_clone = self.did.clone();

        _ = self
            .db
            .get()
            .await?
            .interact(move |conn| {
                _ = update(BlobSchema::blob)
                    .filter(BlobSchema::cid.eq(blob_cid))
                    .filter(BlobSchema::did.eq(did_clone))
                    .set(BlobSchema::takedownRef.eq(takedown_ref))
                    .execute(conn)?;
                Ok::<_, result::Error>(blob)
            })
            .await
            .expect("Failed to update blob takedown status")?;

        let res = match takedown.applied {
            true => self.blobstore.quarantine(blob).await,
            false => self.blobstore.unquarantine(blob).await,
        };

        match res {
            Ok(_) => Ok(()),
            Err(e) => match e.downcast_ref() {
                Some(BlobError::BlobNotFoundError) => Ok(()),
                None => Err(e),
            },
        }
    }
}
