//! Transactor for record operations in the actor store.

use anyhow::{Context as _, Result};
use atrium_repo::Cid;
use sqlx::SqlitePool;

use crate::actor_store::db::schema::Backlink;
use crate::actor_store::record::reader::{RecordReader, get_backlinks};

/// Transaction handler for record operations.
pub struct RecordTransactor {
    /// The record reader.
    pub reader: RecordReader,
    /// The blob store.
    pub blobstore: _,
}

impl RecordTransactor {
    /// Create a new record transactor.
    pub fn new(db: SqlitePool, blobstore: _, did: String) -> Self {
        Self {
            reader: RecordReader::new(db, did),
            blobstore,
        }
    }

    /// Index a record in the database.
    pub async fn index_record(
        &self,
        uri: _,
        cid: Cid,
        record: Option<serde_json::Value>,
        action: atrium_repo::WriteOpAction,
        repo_rev: &str,
        timestamp: Option<String>,
    ) -> Result<()> {
        todo!()
    }

    /// Delete a record from the database.
    pub async fn delete_record(&self, uri: &_) -> Result<()> {
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

        tracing::debug!("Deleted indexed record {}", uri_str);
        Ok(())
    }

    /// Remove backlinks for a URI.
    pub async fn remove_backlinks_by_uri(&self, uri: &str) -> Result<()> {
        sqlx::query!("DELETE FROM backlink WHERE uri = ?", uri)
            .execute(&self.reader.db)
            .await
            .context("failed to remove backlinks")?;

        Ok(())
    }

    /// Add backlinks to the database.
    pub async fn add_backlinks(&self, backlinks: Vec<Backlink>) -> Result<()> {
        todo!()
    }

    /// Update the takedown status of a record.
    pub async fn update_record_takedown_status(&self, uri: &_, takedown: _) -> Result<()> {
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
