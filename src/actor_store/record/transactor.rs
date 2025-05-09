//! Transactor for record operations in the actor store.

use anyhow::{Context as _, Result};
use atrium_repo::Cid;
use rsky_repo::types::WriteOpAction;
use rsky_syntax::aturi::AtUri;
use sqlx::SqlitePool;

use crate::actor_store::db::schema::Backlink;
use crate::actor_store::record::reader::{RecordReader, StatusAttr, get_backlinks};

/// Transaction handler for record operations.
pub(crate) struct RecordTransactor {
    /// The record reader.
    pub reader: RecordReader,
    /// The blob store.
    pub blobstore: SqlitePool, // This will be replaced with proper BlobStore type
}

impl RecordTransactor {
    /// Create a new record transactor.
    pub(crate) fn new(db: SqlitePool, blobstore: SqlitePool, did: String) -> Self {
        Self {
            reader: RecordReader::new(db, did),
            blobstore,
        }
    }

    /// Index a record in the database.
    pub(crate) async fn index_record(
        &self,
        uri: AtUri,
        cid: Cid,
        record: Option<serde_json::Value>,
        action: WriteOpAction,
        repo_rev: &str,
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

        // Track current version of record
        _ = sqlx::query!(
            r#"
            INSERT INTO record (uri, cid, collection, rkey, repoRev, indexedAt)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (uri) DO UPDATE SET
                cid = ?,
                repoRev = ?,
                indexedAt = ?
            "#,
            uri_str,
            cid_str,
            collection,
            rkey,
            repo_rev,
            now,
            cid_str,
            repo_rev,
            now
        )
        .execute(&self.reader.db)
        .await
        .context("failed to index record")?;

        // Maintain backlinks if record is provided
        if let Some(record_value) = record {
            let backlinks = get_backlinks(&uri, &record_value)?;

            if action == WriteOpAction::Update {
                // On update, clear old backlinks first
                self.remove_backlinks_by_uri(&uri_str).await?;
            }

            if !backlinks.is_empty() {
                self.add_backlinks(backlinks).await?;
            }
        }

        tracing::info!("Indexed record {}", uri_str);
        Ok(())
    }

    /// Delete a record from the database.
    pub(crate) async fn delete_record(&self, uri: &AtUri) -> Result<()> {
        let uri_str = uri.to_string();
        tracing::debug!("Deleting indexed record {}", uri_str);

        // Delete the record and its backlinks in a transaction
        let mut tx = self.reader.db.begin().await?;

        // Delete from record table
        sqlx::query!("DELETE FROM record WHERE uri = ?", uri_str)
            .execute(&mut *tx)
            .await
            .context("failed to delete record")?;

        // Delete from backlink table
        sqlx::query!("DELETE FROM backlink WHERE uri = ?", uri_str)
            .execute(&mut *tx)
            .await
            .context("failed to delete record backlinks")?;

        tx.commit().await.context("failed to commit transaction")?;

        tracing::info!("Deleted indexed record {}", uri_str);
        Ok(())
    }

    /// Remove backlinks for a URI.
    pub(crate) async fn remove_backlinks_by_uri(&self, uri: &str) -> Result<()> {
        sqlx::query!("DELETE FROM backlink WHERE uri = ?", uri)
            .execute(&self.reader.db)
            .await
            .context("failed to remove backlinks")?;

        Ok(())
    }

    /// Add backlinks to the database.
    pub(crate) async fn add_backlinks(&self, backlinks: Vec<Backlink>) -> Result<()> {
        if backlinks.is_empty() {
            return Ok(());
        }

        let mut query =
            sqlx::QueryBuilder::new("INSERT INTO backlink (uri, path, link_to) VALUES ");

        for (i, backlink) in backlinks.iter().enumerate() {
            if i > 0 {
                query.push(", ");
            }

            query
                .push("(")
                .push_bind(&backlink.uri)
                .push(", ")
                .push_bind(&backlink.path)
                .push(", ")
                .push_bind(&backlink.link_to)
                .push(")");
        }

        query.push(" ON CONFLICT DO NOTHING");

        query
            .build()
            .execute(&self.reader.db)
            .await
            .context("failed to add backlinks")?;

        Ok(())
    }

    /// Update the takedown status of a record.
    pub(crate) async fn update_record_takedown_status(
        &self,
        uri: &AtUri,
        takedown: StatusAttr,
    ) -> Result<()> {
        let uri_str = uri.to_string();
        let takedown_ref = if takedown.applied {
            takedown
                .r#ref
                .or_else(|| Some(chrono::Utc::now().to_rfc3339()))
        } else {
            None
        };

        sqlx::query!(
            "UPDATE record SET takedownRef = ? WHERE uri = ?",
            takedown_ref,
            uri_str
        )
        .execute(&self.reader.db)
        .await
        .context("failed to update record takedown status")?;

        Ok(())
    }
}
