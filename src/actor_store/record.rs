// src/actor_store/record.rs
use anyhow::Result;
use sqlx::Row;

use crate::actor_store::db::ActorDb;

#[derive(Clone)]
pub struct RecordReader {
    db: ActorDb,
}

impl RecordReader {
    pub fn new(db: ActorDb) -> Self {
        Self { db }
    }

    pub async fn get_record(&self, uri: &str) -> Result<Option<RecordData>> {
        let row = sqlx::query(
            "SELECT record.*, repo_block.content
             FROM record
             INNER JOIN repo_block ON repo_block.cid = record.cid
             WHERE record.uri = ? AND record.takedown_ref IS NULL",
        )
        .bind(uri)
        .fetch_optional(&self.db.pool)
        .await?;

        match row {
            Some(row) => Ok(Some(RecordData {
                uri: row.get("uri"),
                cid: row.get("cid"),
                collection: row.get("collection"),
                rkey: row.get("rkey"),
                content: row.get("content"),
            })),
            None => Ok(None),
        }
    }

    pub async fn list_collections(&self) -> Result<Vec<String>> {
        let rows = sqlx::query("SELECT DISTINCT collection FROM record")
            .fetch_all(&self.db.pool)
            .await?;

        Ok(rows.into_iter().map(|row| row.get("collection")).collect())
    }
}

#[derive(Clone)]
pub struct RecordTransactor {
    db: ActorDb,
}

impl RecordTransactor {
    pub fn new(db: ActorDb) -> Self {
        Self { db }
    }

    pub async fn index_record(
        &self,
        uri: &str,
        cid: &str,
        collection: &str,
        rkey: &str,
        repo_rev: &str,
    ) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();

        sqlx::query(
            "INSERT INTO record (uri, cid, collection, rkey, repo_rev, indexed_at)
             VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT (uri) DO UPDATE SET
                cid = ?,
                repo_rev = ?,
                indexed_at = ?",
        )
        .bind(uri)
        .bind(cid)
        .bind(collection)
        .bind(rkey)
        .bind(repo_rev)
        .bind(&now)
        .bind(cid)
        .bind(repo_rev)
        .bind(&now)
        .execute(&self.db.pool)
        .await?;

        Ok(())
    }

    pub async fn delete_record(&self, uri: &str) -> Result<()> {
        sqlx::query("DELETE FROM record WHERE uri = ?")
            .bind(uri)
            .execute(&self.db.pool)
            .await?;

        sqlx::query("DELETE FROM backlink WHERE uri = ?")
            .bind(uri)
            .execute(&self.db.pool)
            .await?;

        Ok(())
    }
}

pub struct RecordData {
    pub uri: String,
    pub cid: String,
    pub collection: String,
    pub rkey: String,
    pub content: Vec<u8>,
}
