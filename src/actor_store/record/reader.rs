//! Reader for record data in the actor store.

use anyhow::{Context as _, Result};
use atrium_repo::Cid;
use sqlx::{Row, SqlitePool};
use std::str::FromStr;

use crate::actor_store::db::schema::Backlink;

/// Reader for record data.
pub struct RecordReader {
    /// Database connection.
    pub db: SqlitePool,
    /// DID of the repository owner.
    pub did: String,
}

/// Record descriptor containing URI, path, and CID.
pub struct RecordDescript {
    /// Record URI.
    pub uri: String,
    /// Record path.
    pub path: String,
    /// Record CID.
    pub cid: Cid,
}

/// Record data with values.
#[derive(Debug, Clone)]
pub struct RecordData {
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
pub struct ListRecordsOptions {
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
    pub fn new(db: SqlitePool, did: String) -> Self {
        Self { db, did }
    }

    /// Count the total number of records.
    pub async fn record_count(&self) -> Result<i64> {
        todo!()
    }

    /// List all records.
    pub async fn list_all(&self) -> Result<Vec<RecordDescript>> {
        todo!()
    }

    /// List all collections in the repository.
    pub async fn list_collections(&self) -> Result<Vec<String>> {
        todo!()
    }

    /// List records for a specific collection.
    pub async fn list_records_for_collection(
        &self,
        opts: ListRecordsOptions,
    ) -> Result<Vec<RecordData>> {
        todo!()
    }

    /// Get a specific record by URI.
    pub async fn get_record(
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
            // TODO: Implement proper CBOR to JSON conversion
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
    pub async fn has_record(
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
    pub async fn get_record_takedown_status(&self, uri: &str) -> Result<Option<_>> {
        let result = sqlx::query!("SELECT takedownRef FROM record WHERE uri = ?", uri)
            .fetch_optional(&self.db)
            .await
            .context("failed to fetch takedown status")?;

        match result {
            Some(row) => {
                if let Some(takedown_ref) = row.takedownRef {
                    Ok(Some(_ {
                        applied: true,
                        r#ref: Some(takedown_ref),
                    }))
                } else {
                    Ok(Some(_ {
                        applied: false,
                        r#ref: None,
                    }))
                }
            }
            None => Ok(None),
        }
    }

    /// Get the current CID for a record URI.
    pub async fn get_current_record_cid(&self, uri: &str) -> Result<Option<Cid>> {
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
    pub async fn get_record_backlinks(
        &self,
        collection: &str,
        path: &str,
        link_to: &str,
    ) -> Result<Vec<Record>> {
        todo!()
    }

    /// Get backlink conflicts for a record.
    pub async fn get_backlink_conflicts(&self) -> Result<Vec<_>> {
        todo!()
    }

    /// List existing blocks in the repository.
    pub async fn list_existing_blocks(&self) -> Result<Vec<Cid>> {
        let mut blocks = Vec::new();
        let mut cursor: Option<String> = Some("".to_string());

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
}

/// Database record structure.
#[derive(Debug, Clone)]
pub struct Record {
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

/// Extract backlinks from a record.
pub fn get_backlinks(uri: _, record: &serde_json::Value) -> Result<Vec<Backlink>> {
    todo!()
}
