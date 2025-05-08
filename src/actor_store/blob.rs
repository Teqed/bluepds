// src/actor_store/blob.rs
use anyhow::Result;
use atrium_api::types::string::Cid;
use sqlx::Row;

use crate::actor_store::db::ActorDb;

#[derive(Clone)]
pub struct BlobReader {
    db: ActorDb,
}

impl BlobReader {
    pub fn new(db: ActorDb) -> Self {
        Self { db }
    }

    pub async fn get_blob_metadata(&self, cid: &Cid) -> Result<Option<BlobMetadata>> {
        let cid_str = format!("{:?}", cid);
        let row = sqlx::query(
            "SELECT mime_type, size, width, height
             FROM blob
             WHERE cid = ? AND takedown_ref IS NULL",
        )
        .bind(&cid_str)
        .fetch_optional(&self.db.pool)
        .await?;

        match row {
            Some(row) => Ok(Some(BlobMetadata {
                size: row.get::<i64, _>("size") as u64,
                mime_type: row.get("mime_type"),
            })),
            None => Ok(None),
        }
    }

    pub async fn get_blobs_for_record(&self, record_uri: &str) -> Result<Vec<String>> {
        let rows = sqlx::query(
            "SELECT blob.cid
             FROM blob
             INNER JOIN record_blob ON record_blob.blob_cid = blob.cid
             WHERE record_uri = ?",
        )
        .bind(record_uri)
        .fetch_all(&self.db.pool)
        .await?;

        Ok(rows.into_iter().map(|row| row.get("cid")).collect())
    }
}

#[derive(Clone)]
pub struct BlobTransactor {
    db: ActorDb,
}

impl BlobTransactor {
    pub fn new(db: ActorDb) -> Self {
        Self { db }
    }

    pub async fn associate_blob(&self, blob_cid: &str, record_uri: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO record_blob (blob_cid, record_uri)
             VALUES (?, ?)
             ON CONFLICT DO NOTHING",
        )
        .bind(blob_cid)
        .bind(record_uri)
        .execute(&self.db.pool)
        .await?;

        Ok(())
    }

    pub async fn register_blob(&self, cid: &str, mime_type: &str, size: u64) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();

        sqlx::query(
            "INSERT INTO blob (cid, mime_type, size, created_at)
             VALUES (?, ?, ?, ?)
             ON CONFLICT DO NOTHING",
        )
        .bind(cid)
        .bind(mime_type)
        .bind(size as i64)
        .bind(now)
        .execute(&self.db.pool)
        .await?;

        Ok(())
    }
}

pub struct BlobMetadata {
    pub size: u64,
    pub mime_type: String,
}
