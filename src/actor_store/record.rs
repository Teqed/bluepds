//! Record storage and retrieval for the actor store.

use anyhow::{Context as _, Result, bail};
use atrium_api::com::atproto::admin::defs::StatusAttr;
use atrium_repo::Cid;
use diesel::associations::HasTable;
use diesel::prelude::*;
use rsky_pds::models::{Backlink, Record};
use rsky_pds::schema::pds::repo_block::dsl::repo_block;
use rsky_pds::schema::pds::{backlink, record};
use rsky_repo::types::WriteOpAction;
use rsky_syntax::aturi::AtUri;
use std::str::FromStr;

use crate::actor_store::blob::BlobStorePlaceholder;
use crate::actor_store::db::ActorDb;

/// Combined handler for record operations with both read and write capabilities.
pub(crate) struct RecordHandler {
    /// Database connection.
    pub db: ActorDb,
    /// DID of the actor.
    pub did: String,
    /// Blob store for handling blobs.
    pub blobstore: Option<BlobStorePlaceholder>,
}

/// Record descriptor containing URI, path, and CID.
pub(crate) struct RecordDescript {
    /// Record URI.
    pub uri: String,
    /// Record path.
    pub path: String,
    /// Record CID.
    pub cid: Cid,
}

/// Record data with values.
#[derive(Debug, Clone)]
pub(crate) struct RecordData {
    /// Record URI.
    pub uri: String,
    /// Record CID.
    pub cid: String,
    /// Record value as JSON.
    pub value: serde_json::Value,
    /// When the record was indexed.
    pub indexedAt: String,
    /// Reference for takedown, if any.
    pub takedownRef: Option<String>,
}

/// Options for listing records in a collection.
#[derive(Debug, Clone)]
pub(crate) struct ListRecordsOptions {
    /// Collection to list records from.
    pub collection: String,
    /// Maximum number of records to return.
    pub limit: i64,
    /// Whether to reverse the sort order.
    pub reverse: bool,
    /// Cursor for pagination.
    pub cursor: Option<String>,
    /// Start key (deprecated).
    pub rkey_start: Option<String>,
    /// End key (deprecated).
    pub rkey_end: Option<String>,
    /// Whether to include soft-deleted records.
    pub include_soft_deleted: bool,
}

impl RecordHandler {
    /// Create a new record handler.
    pub(crate) fn new(db: ActorDb, did: String) -> Self {
        Self {
            db,
            did,
            blobstore: None,
        }
    }

    /// Create a new record handler with blobstore support.
    pub(crate) fn new_with_blobstore(
        db: ActorDb,
        blobstore: BlobStorePlaceholder,
        did: String,
    ) -> Self {
        Self {
            db,
            did,
            blobstore: Some(blobstore),
        }
    }

    /// Count the total number of records.
    pub(crate) async fn record_count(&self) -> Result<i64> {
        let did = self.did.clone();

        self.db
            .run(move |conn| {
                use rsky_pds::schema::pds::record::dsl::*;

                record.filter(did.eq(&did)).count().get_result(conn)
            })
            .await
    }

    /// List all records.
    pub(crate) async fn list_all(&self) -> Result<Vec<RecordDescript>> {
        let did = self.did.clone();
        let mut records = Vec::new();
        let mut current_cursor = Some("".to_string());

        while let Some(cursor) = current_cursor.take() {
            let cursor_clone = cursor.clone();
            let did_clone = did.clone();

            let rows = self
                .db
                .run(move |conn| {
                    use rsky_pds::schema::pds::record::dsl::*;

                    record
                        .filter(did.eq(&did_clone))
                        .filter(uri.gt(&cursor_clone))
                        .order(uri.asc())
                        .limit(1000)
                        .select((uri, cid))
                        .load::<(String, String)>(conn)
                })
                .await?;

            for (uri_str, cid_str) in &rows {
                let uri = uri_str.clone();
                let parts: Vec<&str> = uri.rsplitn(2, '/').collect();
                let path = if parts.len() == 2 {
                    format!("{}/{}", parts[1], parts[0])
                } else {
                    uri.clone()
                };

                match Cid::from_str(&cid_str) {
                    Ok(cid) => records.push(RecordDescript { uri, path, cid }),
                    Err(e) => tracing::warn!("Invalid CID in database: {}", e),
                }
            }

            if let Some(last) = rows.last() {
                current_cursor = Some(last.0.clone());
            } else {
                break;
            }
        }

        Ok(records)
    }

    /// List all collections in the repository.
    pub(crate) async fn list_collections(&self) -> Result<Vec<String>> {
        let did = self.did.clone();

        self.db
            .run(move |conn| {
                use rsky_pds::schema::pds::record::dsl::*;

                record
                    .filter(did.eq(&did))
                    .group_by(collection)
                    .select(collection)
                    .load::<String>(conn)
            })
            .await
    }

    /// List records for a specific collection.
    pub(crate) async fn list_records_for_collection(
        &self,
        opts: ListRecordsOptions,
    ) -> Result<Vec<RecordData>> {
        let did = self.did.clone();

        self.db
            .run(move |conn| {
                // Start building the query
                let mut query = record::table
                    .inner_join(repo_block::table.on(repo_block::cid.eq(record::cid)))
                    .filter(record::did.eq(&did))
                    .filter(record::collection.eq(&opts.collection))
                    .into_boxed();

                // Handle soft-deleted records
                if !opts.include_soft_deleted {
                    query = query.filter(record::takedownRef.is_null());
                }

                // Handle cursor-based pagination first
                if let Some(cursor) = &opts.cursor {
                    if opts.reverse {
                        query = query.filter(record::rkey.gt(cursor));
                    } else {
                        query = query.filter(record::rkey.lt(cursor));
                    }
                } else {
                    // Fall back to deprecated rkey-based pagination
                    if let Some(start) = &opts.rkey_start {
                        query = query.filter(record::rkey.gt(start));
                    }
                    if let Some(end) = &opts.rkey_end {
                        query = query.filter(record::rkey.lt(end));
                    }
                }

                // Add order and limit
                if opts.reverse {
                    query = query.order(record::rkey.asc());
                } else {
                    query = query.order(record::rkey.desc());
                }

                query = query.limit(opts.limit);

                // Execute the query
                let results = query
                    .select((
                        record::uri,
                        record::cid,
                        record::indexedAt,
                        record::takedownRef,
                        repo_block::content,
                    ))
                    .load::<(String, String, String, Option<String>, Vec<u8>)>(conn)?;

                // Convert results to RecordData
                let records = results
                    .into_iter()
                    .map(|(uri, cid, indexedAt, takedownRef, content)| {
                        let value = serde_json::from_slice(&content)
                            .with_context(|| format!("Failed to decode record {}", cid))?;

                        Ok(RecordData {
                            uri,
                            cid,
                            value,
                            indexedAt,
                            takedownRef,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(records)
            })
            .await
    }

    /// Get a specific record by URI.
    pub(crate) async fn get_record(
        &self,
        uri: &AtUri,
        cid: Option<&str>,
        include_soft_deleted: bool,
    ) -> Result<Option<RecordData>> {
        let did = self.did.clone();
        let uri_str = uri.to_string();
        let cid_opt = cid.map(|c| c.to_string());

        self.db
            .run(move |conn| {
                let mut query = record::table
                    .inner_join(repo_block::table.on(repo_block::cid.eq(record::cid)))
                    .filter(record::did.eq(&did))
                    .filter(record::uri.eq(&uri_str))
                    .into_boxed();

                if !include_soft_deleted {
                    query = query.filter(record::takedownRef.is_null());
                }

                if let Some(cid_val) = cid_opt {
                    query = query.filter(record::cid.eq(cid_val));
                }

                let result = query
                    .select((
                        record::uri,
                        record::cid,
                        record::indexedAt,
                        record::takedownRef,
                        repo_block::content,
                    ))
                    .first::<(String, String, String, Option<String>, Vec<u8>)>(conn)
                    .optional()?;

                if let Some((uri, cid, indexedAt, takedownRef, content)) = result {
                    let value = serde_json::from_slice(&content)
                        .with_context(|| format!("Failed to decode record {}", cid))?;

                    Ok(Some(RecordData {
                        uri,
                        cid,
                        value,
                        indexedAt,
                        takedownRef,
                    }))
                } else {
                    Ok(None)
                }
            })
            .await
    }

    /// Check if a record exists.
    pub(crate) async fn has_record(
        &self,
        uri: &str,
        cid: Option<&str>,
        include_soft_deleted: bool,
    ) -> Result<bool> {
        let did = self.did.clone();
        let uri_str = uri.to_string();
        let cid_opt = cid.map(|c| c.to_string());

        self.db
            .run(move |conn| {
                let mut query = record::table
                    .filter(record::did.eq(&did))
                    .filter(record::uri.eq(&uri_str))
                    .into_boxed();

                if !include_soft_deleted {
                    query = query.filter(record::takedownRef.is_null());
                }

                if let Some(cid_val) = cid_opt {
                    query = query.filter(record::cid.eq(cid_val));
                }

                let exists = query
                    .select(record::uri)
                    .first::<String>(conn)
                    .optional()?
                    .is_some();

                Ok(exists)
            })
            .await
    }

    /// Get the takedown status of a record.
    pub(crate) async fn get_record_takedown_status(&self, uri: &str) -> Result<Option<StatusAttr>> {
        let did = self.did.clone();
        let uri_str = uri.to_string();

        self.db
            .run(move |conn| {
                let result = record::table
                    .filter(record::did.eq(&did))
                    .filter(record::uri.eq(&uri_str))
                    .select(record::takedownRef)
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

    /// Get the current CID for a record URI.
    pub(crate) async fn get_current_record_cid(&self, uri: &str) -> Result<Option<Cid>> {
        let did = self.did.clone();
        let uri_str = uri.to_string();

        self.db
            .run(move |conn| {
                let result = record::table
                    .filter(record::did.eq(&did))
                    .filter(record::uri.eq(&uri_str))
                    .select(record::cid)
                    .first::<String>(conn)
                    .optional()?;

                match result {
                    Some(cid_str) => {
                        let cid = Cid::from_str(&cid_str)?;
                        Ok(Some(cid))
                    }
                    None => Ok(None),
                }
            })
            .await
    }

    /// Get backlinks for a record.
    pub(crate) async fn get_record_backlinks(
        &self,
        collection: &str,
        path: &str,
        linkTo: &str,
    ) -> Result<Vec<Record>> {
        let did = self.did.clone();
        let collection_str = collection.to_string();
        let path_str = path.to_string();
        let linkTo_str = linkTo.to_string();

        self.db
            .run(move |conn| {
                backlink::table
                    .inner_join(record::table.on(backlink::uri.eq(record::uri)))
                    .filter(backlink::path.eq(&path_str))
                    .filter(backlink::linkTo.eq(&linkTo_str))
                    .filter(record::collection.eq(&collection_str))
                    .filter(record::did.eq(&did))
                    .select(Record::as_select())
                    .load::<Record>(conn)
            })
            .await
    }

    /// Get backlink conflicts for a record.
    pub(crate) async fn get_backlink_conflicts(
        &self,
        uri: &AtUri,
        record: &serde_json::Value,
    ) -> Result<Vec<String>> {
        let backlinks = get_backlinks(uri, record)?;
        if backlinks.is_empty() {
            return Ok(Vec::new());
        }

        let did = self.did.clone();
        let uri_collection = uri.get_collection().to_string();
        let mut conflicts = Vec::new();

        for backlink in backlinks {
            let path_str = backlink.path.clone();
            let linkTo_str = backlink.linkTo.clone();

            let results = self
                .db
                .run(move |conn| {
                    backlink::table
                        .inner_join(record::table.on(backlink::uri.eq(record::uri)))
                        .filter(backlink::path.eq(&path_str))
                        .filter(backlink::linkTo.eq(&linkTo_str))
                        .filter(record::collection.eq(&uri_collection))
                        .filter(record::did.eq(&did))
                        .select(record::uri)
                        .load::<String>(conn)
                })
                .await?;

            conflicts.extend(results);
        }

        Ok(conflicts)
    }

    /// List existing blocks in the repository.
    pub(crate) async fn list_existing_blocks(&self) -> Result<Vec<Cid>> {
        let did = self.did.clone();
        let mut blocks = Vec::new();
        let mut current_cursor = Some("".to_string());

        while let Some(cursor) = current_cursor.take() {
            let cursor_clone = cursor.clone();
            let did_clone = did.clone();

            let rows = self
                .db
                .run(move |conn| {
                    use rsky_pds::schema::pds::repo_block::dsl::*;

                    repo_block
                        .filter(did.eq(&did_clone))
                        .filter(cid.gt(&cursor_clone))
                        .order(cid.asc())
                        .limit(1000)
                        .select(cid)
                        .load::<String>(conn)
                })
                .await?;

            for cid_str in &rows {
                match Cid::from_str(cid_str) {
                    Ok(cid) => blocks.push(cid),
                    Err(e) => tracing::warn!("Invalid CID in database: {}", e),
                }
            }

            if let Some(last) = rows.last() {
                current_cursor = Some(last.clone());
            } else {
                break;
            }
        }

        Ok(blocks)
    }

    /// Get the profile record for this repository
    pub(crate) async fn get_profile_record(&self) -> Result<Option<serde_json::Value>> {
        let did = self.did.clone();

        self.db
            .run(move |conn| {
                let result = record::table
                    .inner_join(repo_block::table.on(repo_block::cid.eq(record::cid)))
                    .filter(record::did.eq(&did))
                    .filter(record::collection.eq("app.bsky.actor.profile"))
                    .filter(record::rkey.eq("self"))
                    .select(repo_block::content)
                    .first::<Vec<u8>>(conn)
                    .optional()?;

                if let Some(content) = result {
                    let value = serde_json::from_slice(&content)
                        .context("Failed to decode profile record")?;
                    Ok(Some(value))
                } else {
                    Ok(None)
                }
            })
            .await
    }

    /// Get records created or updated since a specific revision
    pub(crate) async fn get_records_since_rev(&self, rev: &str) -> Result<Vec<RecordData>> {
        let did = self.did.clone();
        let rev_str = rev.to_string();

        // First check if the revision exists
        let exists = self
            .db
            .run({
                let did_clone = did.clone();
                let rev_clone = rev_str.clone();

                move |conn| {
                    record::table
                        .filter(record::did.eq(&did_clone))
                        .filter(record::repoRev.le(&rev_clone))
                        .count()
                        .get_result::<i64>(conn)
                        .map(|count| count > 0)
                }
            })
            .await?;

        if !exists {
            // No records before this revision - possible account migration case
            return Ok(Vec::new());
        }

        // Get records since the revision
        self.db
            .run(move |conn| {
                let results = record::table
                    .inner_join(repo_block::table.on(repo_block::cid.eq(record::cid)))
                    .filter(record::did.eq(&did))
                    .filter(record::repoRev.gt(&rev_str))
                    .order(record::repoRev.asc())
                    .limit(10)
                    .select((
                        record::uri,
                        record::cid,
                        record::indexedAt,
                        repo_block::content,
                    ))
                    .load::<(String, String, String, Vec<u8>)>(conn)?;

                let records = results
                    .into_iter()
                    .map(|(uri, cid, indexedAt, content)| {
                        let value = serde_json::from_slice(&content)
                            .with_context(|| format!("Failed to decode record {}", cid))?;

                        Ok(RecordData {
                            uri,
                            cid,
                            value,
                            indexedAt,
                            takedownRef: None, // Not included in the query
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(records)
            })
            .await
    }

    // Transactor methods
    // -----------------

    /// Index a record in the database.
    pub(crate) async fn index_record(
        &self,
        uri: AtUri,
        cid: Cid,
        record: Option<&serde_json::Value>,
        action: WriteOpAction,
        repoRev: &str,
        timestamp: Option<String>,
    ) -> Result<()> {
        let uri_str = uri.to_string();
        tracing::debug!("Indexing record {}", uri_str);

        if !uri_str.starts_with("at://did:") {
            return Err(anyhow::anyhow!("Expected indexed URI to contain DID"));
        }

        let collection = uri.get_collection().to_string();
        let rkey = uri.get_rkey().to_string();

        if collection.is_empty() {
            return Err(anyhow::anyhow!(
                "Expected indexed URI to contain a collection"
            ));
        } else if rkey.is_empty() {
            return Err(anyhow::anyhow!(
                "Expected indexed URI to contain a record key"
            ));
        }

        let cid_str = cid.to_string();
        let now = timestamp.unwrap_or_else(|| chrono::Utc::now().to_rfc3339());
        let did = self.did.clone();
        let repoRev = repoRev.to_string();

        // Create the record for database insertion
        let record_values = (
            record::did.eq(&did),
            record::uri.eq(&uri_str),
            record::cid.eq(&cid_str),
            record::collection.eq(&collection),
            record::rkey.eq(&rkey),
            record::repoRev.eq(&repoRev),
            record::indexedAt.eq(&now),
        );

        self.db
            .transaction(move |conn| {
                // Track current version of record
                diesel::insert_into(record::table)
                    .values(&record_values)
                    .on_conflict(record::uri)
                    .do_update()
                    .set((
                        record::cid.eq(&cid_str),
                        record::repoRev.eq(&repoRev),
                        record::indexedAt.eq(&now),
                    ))
                    .execute(conn)
                    .context("Failed to insert/update record")?;

                // Maintain backlinks if record is provided
                if let Some(record_value) = record {
                    let backlinks = get_backlinks(&uri, record_value)?;

                    if action == WriteOpAction::Update {
                        // On update, clear old backlinks first
                        diesel::delete(backlink::table)
                            .filter(backlink::uri.eq(&uri_str))
                            .execute(conn)
                            .context("Failed to delete existing backlinks")?;
                    }

                    if !backlinks.is_empty() {
                        // Insert all backlinks at once
                        let backlink_values: Vec<_> = backlinks
                            .into_iter()
                            .map(|backlink| {
                                (
                                    backlink::uri.eq(&uri_str),
                                    backlink::path.eq(&backlink.path),
                                    backlink::linkTo.eq(&backlink.linkTo),
                                )
                            })
                            .collect();

                        diesel::insert_into(backlink::table)
                            .values(&backlink_values)
                            .on_conflict_do_nothing()
                            .execute(conn)
                            .context("Failed to insert backlinks")?;
                    }
                }

                tracing::info!("Indexed record {}", uri_str);
                Ok(())
            })
            .await
    }

    /// Delete a record from the database.
    pub(crate) async fn delete_record(&self, uri: &AtUri) -> Result<()> {
        let uri_str = uri.to_string();
        tracing::debug!("Deleting indexed record {}", uri_str);

        self.db
            .transaction(move |conn| {
                // Delete from record table
                diesel::delete(record::table)
                    .filter(record::uri.eq(&uri_str))
                    .execute(conn)
                    .context("Failed to delete record")?;

                // Delete from backlink table
                diesel::delete(backlink::table)
                    .filter(backlink::uri.eq(&uri_str))
                    .execute(conn)
                    .context("Failed to delete record backlinks")?;

                tracing::info!("Deleted indexed record {}", uri_str);
                Ok(())
            })
            .await
    }

    /// Remove backlinks for a URI.
    pub(crate) async fn remove_backlinks_by_uri(&self, uri: &str) -> Result<()> {
        let uri_str = uri.to_string();

        self.db
            .run(move |conn| {
                diesel::delete(backlink::table)
                    .filter(backlink::uri.eq(&uri_str))
                    .execute(conn)
                    .context("Failed to remove backlinks")?;

                Ok(())
            })
            .await
    }

    /// Add backlinks to the database.
    pub(crate) async fn add_backlinks(&self, backlinks: Vec<Backlink>) -> Result<()> {
        if backlinks.is_empty() {
            return Ok(());
        }

        self.db
            .run(move |conn| {
                let backlink_values: Vec<_> = backlinks
                    .into_iter()
                    .map(|backlink| {
                        (
                            backlink::uri.eq(&backlink.uri),
                            backlink::path.eq(&backlink.path),
                            backlink::linkTo.eq(&backlink.linkTo),
                        )
                    })
                    .collect();

                diesel::insert_into(backlink::table)
                    .values(&backlink_values)
                    .on_conflict_do_nothing()
                    .execute(conn)
                    .context("Failed to add backlinks")?;

                Ok(())
            })
            .await
    }

    /// Update the takedown status of a record.
    pub(crate) async fn update_record_takedown_status(
        &self,
        uri: &AtUri,
        takedown: StatusAttr,
    ) -> Result<()> {
        let uri_str = uri.to_string();
        let did = self.did.clone();
        let takedownRef = if takedown.applied {
            takedown
                .r#ref
                .or_else(|| Some(chrono::Utc::now().to_rfc3339()))
        } else {
            None
        };

        self.db
            .run(move |conn| {
                diesel::update(record::table)
                    .filter(record::did.eq(&did))
                    .filter(record::uri.eq(&uri_str))
                    .set(record::takedownRef.eq(takedownRef))
                    .execute(conn)
                    .context("Failed to update record takedown status")?;

                Ok(())
            })
            .await
    }
}

/// Extract backlinks from a record.
pub(super) fn get_backlinks(uri: &AtUri, record: &serde_json::Value) -> Result<Vec<Backlink>> {
    let mut backlinks = Vec::new();

    // Check for record type
    if let Some(record_type) = record.get("$type").and_then(|t| t.as_str()) {
        // Handle follow and block records
        if record_type == "app.bsky.graph.follow" || record_type == "app.bsky.graph.block" {
            if let Some(subject) = record.get("subject").and_then(|s| s.as_str()) {
                // Verify it's a valid DID
                if subject.starts_with("did:") {
                    backlinks.push(Backlink {
                        uri: uri.to_string(),
                        path: "subject".to_string(),
                        linkTo: subject.to_string(),
                    });
                }
            }
        }
        // Handle like and repost records
        else if record_type == "app.bsky.feed.like" || record_type == "app.bsky.feed.repost" {
            if let Some(subject) = record.get("subject") {
                if let Some(subject_uri) = subject.get("uri").and_then(|u| u.as_str()) {
                    // Verify it's a valid AT URI
                    if subject_uri.starts_with("at://") {
                        backlinks.push(Backlink {
                            uri: uri.to_string(),
                            path: "subject.uri".to_string(),
                            linkTo: subject_uri.to_string(),
                        });
                    }
                }
            }
        }
    }

    Ok(backlinks)
}
