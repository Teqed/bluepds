//! Reader for record data in the actor store.

use anyhow::{Context as _, Result};
use atrium_repo::Cid;
use rsky_syntax::aturi::AtUri;
use sqlx::{Row, SqlitePool};
use std::str::FromStr;

use crate::actor_store::db::schema::Backlink;

/// Reader for record data.
pub(crate) struct RecordReader {
    /// Database connection.
    pub db: SqlitePool,
    /// DID of the repository owner.
    pub did: String,
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
    pub indexed_at: String,
    /// Reference for takedown, if any.
    pub takedown_ref: Option<String>,
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

impl RecordReader {
    /// Create a new record reader.
    pub(crate) fn new(db: SqlitePool, did: String) -> Self {
        Self { db, did }
    }

    /// Count the total number of records.
    pub(crate) async fn record_count(&self) -> Result<i64> {
        let result = sqlx::query!(r#"SELECT COUNT(*) as count FROM record"#)
            .fetch_one(&self.db)
            .await
            .context("failed to count records")?;

        Ok(result.count)
    }

    /// List all records.
    pub(crate) async fn list_all(&self) -> Result<Vec<RecordDescript>> {
        let mut records = Vec::new();
        let mut cursor = Some("".to_string());

        while let Some(current_cursor) = cursor.take() {
            let rows = sqlx::query!(
                "SELECT uri, cid FROM record WHERE uri > ? ORDER BY uri ASC LIMIT 1000",
                current_cursor
            )
            .fetch_all(&self.db)
            .await
            .context("failed to fetch records")?;

            for row in &rows {
                let uri = row.uri.clone();
                let parts: Vec<&str> = uri.rsplitn(2, '/').collect();
                let path = if parts.len() == 2 {
                    format!("{}/{}", parts[1], parts[0])
                } else {
                    uri.clone()
                };

                match Cid::from_str(&row.cid) {
                    Ok(cid) => records.push(RecordDescript { uri, path, cid }),
                    Err(e) => tracing::warn!("Invalid CID in database: {}", e),
                }
            }

            if let Some(last) = rows.last() {
                cursor = Some(last.uri.clone());
            }
        }

        Ok(records)
    }

    /// List all collections in the repository.
    pub(crate) async fn list_collections(&self) -> Result<Vec<String>> {
        let rows = sqlx::query!("SELECT collection FROM record GROUP BY collection")
            .fetch_all(&self.db)
            .await
            .context("failed to list collections")?;

        Ok(rows.into_iter().map(|row| row.collection).collect())
    }

    /// List records for a specific collection.
    pub(crate) async fn list_records_for_collection(
        &self,
        opts: ListRecordsOptions,
    ) -> Result<Vec<RecordData>> {
        let mut query = sqlx::QueryBuilder::new(
            "SELECT r.uri, r.cid, r.indexed_at, r.takedown_ref, b.content
             FROM record r
             INNER JOIN repo_block b ON b.cid = r.cid
             WHERE r.collection = ",
        );

        query.push_bind(&opts.collection);

        if !opts.include_soft_deleted {
            query.push(" AND r.takedown_ref IS NULL");
        }

        // Handle cursor-based pagination first
        if let Some(cursor) = &opts.cursor {
            if opts.reverse {
                query.push(" AND r.rkey > ");
            } else {
                query.push(" AND r.rkey < ");
            }
            query.push_bind(cursor);
        } else {
            // Fall back to deprecated rkey-based pagination
            if let Some(start) = &opts.rkey_start {
                query.push(" AND r.rkey > ");
                query.push_bind(start);
            }
            if let Some(end) = &opts.rkey_end {
                query.push(" AND r.rkey < ");
                query.push_bind(end);
            }
        }

        // Add order and limit
        if opts.reverse {
            query.push(" ORDER BY r.rkey ASC");
        } else {
            query.push(" ORDER BY r.rkey DESC");
        }

        query.push(" LIMIT ");
        query.push_bind(opts.limit);

        let rows = query
            .build()
            .fetch_all(&self.db)
            .await
            .context("failed to list records")?;

        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let uri: String = row.get("uri");
            let cid: String = row.get("cid");
            let indexed_at: String = row.get("indexed_at");
            let takedown_ref: Option<String> = row.get("takedown_ref");
            let content: Vec<u8> = row.get("content");

            let value = serde_json::from_slice(&content)
                .context(format!("failed to decode record {}", cid))?;

            records.push(RecordData {
                uri,
                cid,
                value,
                indexed_at,
                takedown_ref,
            });
        }

        Ok(records)
    }

    /// Get a specific record by URI.
    pub(crate) async fn get_record(
        &self,
        uri: &str,
        cid: Option<&str>,
        include_soft_deleted: bool,
    ) -> Result<Option<RecordData>> {
        let mut query = sqlx::QueryBuilder::new(
            "SELECT r.uri, r.cid, r.indexed_at, r.takedown_ref, b.content
             FROM record r
             INNER JOIN repo_block b ON b.cid = r.cid
             WHERE r.uri = ",
        );

        query.push_bind(uri);

        if !include_soft_deleted {
            query.push(" AND r.takedown_ref IS NULL");
        }

        if let Some(cid_str) = cid {
            query.push(" AND r.cid = ");
            query.push_bind(cid_str);
        }

        let row = query
            .build()
            .fetch_optional(&self.db)
            .await
            .context("failed to fetch record")?;

        if let Some(row) = row {
            let uri = row.get::<String, _>("uri");
            let cid = row.get::<String, _>("cid");
            let indexed_at = row.get::<String, _>("indexed_at");
            let takedown_ref = row.get::<Option<String>, _>("takedown_ref");
            let content: Vec<u8> = row.get("content");

            // Convert CBOR to Lexicon record
            let value = serde_json::from_slice(&content)
                .context(format!("failed to decode record {}", cid))?;

            Ok(Some(RecordData {
                uri,
                cid,
                value,
                indexed_at,
                takedown_ref,
            }))
        } else {
            Ok(None)
        }
    }

    /// Check if a record exists.
    pub(crate) async fn has_record(
        &self,
        uri: &str,
        cid: Option<&str>,
        include_soft_deleted: bool,
    ) -> Result<bool> {
        let mut query = sqlx::QueryBuilder::new("SELECT uri FROM record WHERE uri = ");

        query.push_bind(uri);

        if !include_soft_deleted {
            query.push(" AND takedown_ref IS NULL");
        }

        if let Some(cid_str) = cid {
            query.push(" AND cid = ");
            query.push_bind(cid_str);
        }

        let result = query
            .build()
            .fetch_optional(&self.db)
            .await
            .context("failed to check record existence")?;

        Ok(result.is_some())
    }

    /// Get the takedown status of a record.
    pub(crate) async fn get_record_takedown_status(&self, uri: &str) -> Result<Option<StatusAttr>> {
        let result = sqlx::query!("SELECT takedownRef FROM record WHERE uri = ?", uri)
            .fetch_optional(&self.db)
            .await
            .context("failed to fetch takedown status")?;

        match result {
            Some(row) => {
                if let Some(takedown_ref) = row.takedownRef {
                    Ok(Some(StatusAttr {
                        applied: true,
                        r#ref: Some(takedown_ref),
                    }))
                } else {
                    Ok(Some(StatusAttr {
                        applied: false,
                        r#ref: None,
                    }))
                }
            }
            None => Ok(None),
        }
    }

    /// Get the current CID for a record URI.
    pub(crate) async fn get_current_record_cid(&self, uri: &str) -> Result<Option<Cid>> {
        let result = sqlx::query!("SELECT cid FROM record WHERE uri = ?", uri)
            .fetch_optional(&self.db)
            .await
            .context("failed to fetch record CID")?;

        match result {
            Some(row) => {
                let cid = Cid::from_str(&row.cid)?;
                Ok(Some(cid))
            }
            None => Ok(None),
        }
    }

    /// Get backlinks for a record.
    pub(crate) async fn get_record_backlinks(
        &self,
        collection: &str,
        path: &str,
        link_to: &str,
    ) -> Result<Vec<Record>> {
        let rows = sqlx::query!(
            r#"
            SELECT r.*
            FROM record r
            INNER JOIN backlink b ON b.uri = r.uri
            WHERE b.path = ?
            AND b.linkTo = ?
            AND r.collection = ?
            "#,
            path,
            link_to,
            collection
        )
        .fetch_all(&self.db)
        .await
        .context("failed to fetch record backlinks")?;

        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            records.push(Record {
                uri: row.uri,
                cid: row.cid,
                collection: row.collection,
                rkey: row.rkey,
                repo_rev: Some(row.repoRev),
                indexed_at: row.indexedAt,
                takedown_ref: row.takedownRef,
                did: self.did.clone(),
            });
        }

        Ok(records)
    }

    /// Get backlink conflicts for a record.
    pub(crate) async fn get_backlink_conflicts(
        &self,
        uri: AtUri,
        record: &serde_json::Value,
    ) -> Result<Vec<String>> {
        let backlinks = get_backlinks(&uri, record)?;
        if backlinks.is_empty() {
            return Ok(Vec::new());
        }

        let mut conflicts = Vec::new();
        for backlink in &backlinks {
            let uri_collection = &uri.get_collection();
            let rows = sqlx::query!(
                r#"
                SELECT r.uri
                FROM record r
                INNER JOIN backlink b ON b.uri = r.uri
                WHERE b.path = ?
                AND b.linkTo = ?
                AND r.collection = ?
                "#,
                backlink.path,
                backlink.link_to,
                uri_collection
            )
            .fetch_all(&self.db)
            .await
            .context("failed to fetch backlink conflicts")?;

            for row in rows {
                conflicts.push(row.uri);
            }
        }

        Ok(conflicts)
    }

    /// List existing blocks in the repository.
    pub(crate) async fn list_existing_blocks(&self) -> Result<Vec<Cid>> {
        let mut blocks = Vec::new();
        let mut cursor = Some("".to_string());

        while let Some(current_cursor) = cursor.take() {
            let rows = sqlx::query!(
                "SELECT cid FROM repo_block WHERE cid > ? ORDER BY cid ASC LIMIT 1000",
                current_cursor
            )
            .fetch_all(&self.db)
            .await
            .context("failed to fetch blocks")?;

            for row in &rows {
                match Cid::from_str(&row.cid) {
                    Ok(cid) => blocks.push(cid),
                    Err(e) => tracing::warn!("Invalid CID in database: {}", e),
                }
            }

            if let Some(last) = rows.last() {
                cursor = Some(last.cid.clone());
            }
        }

        Ok(blocks)
    }

    /// Get the profile record for this repository
    pub(crate) async fn get_profile_record(&self) -> Result<Option<serde_json::Value>> {
        let row = sqlx::query!(
            r#"
            SELECT b.content
            FROM record r
            LEFT JOIN repo_block b ON b.cid = r.cid
            WHERE r.collection = 'app.bsky.actor.profile'
            AND r.rkey = 'self'
            LIMIT 1
            "#
        )
        .fetch_optional(&self.db)
        .await
        .context("failed to fetch profile record")?;

        if let Some(row) = row {
            if let Some(content) = row.content {
                // Convert CBOR to JSON
                let value =
                    serde_json::from_slice(&content).context("failed to decode profile record")?;
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    /// Get records created or updated since a specific revision
    pub(crate) async fn get_records_since_rev(&self, rev: &str) -> Result<Vec<RecordData>> {
        // First check if the revision exists
        let sanity_check = sqlx::query!(
            r#"SELECT repoRev FROM record WHERE repoRev <= ? LIMIT 1"#,
            rev
        )
        .fetch_optional(&self.db)
        .await
        .context("failed to check revision existence")?;

        if sanity_check.is_none() {
            // No records before this revision - possible account migration case
            return Ok(Vec::new());
        }

        let rows = sqlx::query!(
            r#"
            SELECT r.uri, r.cid, r.indexedAt, b.content
            FROM record r
            INNER JOIN repo_block b ON b.cid = r.cid
            WHERE r.repoRev > ?
            ORDER BY r.repoRev ASC
            LIMIT 10
            "#,
            rev
        )
        .fetch_all(&self.db)
        .await
        .context("failed to fetch records since revision")?;

        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            let value = serde_json::from_slice(&row.content)
                .context(format!("failed to decode record {}", row.cid))?;

            records.push(RecordData {
                uri: row.uri,
                cid: row.cid,
                value,
                indexed_at: row.indexedAt,
                takedown_ref: None, // Not included in the query
            });
        }

        Ok(records)
    }
}

/// Database record structure.
#[derive(Debug, Clone)]
pub(crate) struct Record {
    /// Record URI.
    pub uri: String,
    /// Record CID.
    pub cid: String,
    /// Record collection.
    pub collection: String,
    /// Record key.
    pub rkey: String,
    /// Repository revision.
    pub repo_rev: Option<String>,
    /// When the record was indexed.
    pub indexed_at: String,
    /// Reference for takedown, if any.
    pub takedown_ref: Option<String>,
    /// DID of the repository owner.
    pub did: String,
}

/// Status attribute for takedowns
#[derive(Debug, Clone)]
pub(crate) struct StatusAttr {
    /// Whether the takedown is applied
    pub applied: bool,
    /// Reference for the takedown
    pub r#ref: Option<String>,
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
                        link_to: subject.to_string(),
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
                            link_to: subject_uri.to_string(),
                        });
                    }
                }
            }
        }
    }

    Ok(backlinks)
}
