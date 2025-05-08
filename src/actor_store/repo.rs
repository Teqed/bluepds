// src/actor_store/repo.rs
use anyhow::Result;
use atrium_crypto::keypair::Keypair;
use k256::Secp256k1;
use sqlx::Row;
use std::sync::Arc;

use crate::actor_store::db::ActorDb;

#[derive(Clone)]
pub struct RepoReader {
    db: ActorDb,
    did: String,
}

impl RepoReader {
    pub fn new(db: ActorDb, did: String) -> Self {
        Self { db, did }
    }

    pub async fn get_root(&self) -> Result<Option<RepoRoot>> {
        let row = sqlx::query("SELECT cid, rev, indexed_at FROM repo_root WHERE did = ?")
            .bind(&self.did)
            .fetch_optional(&self.db.pool)
            .await?;

        match row {
            Some(row) => Ok(Some(RepoRoot {
                cid: row.get("cid"),
                rev: row.get("rev"),
                indexed_at: row.get("indexed_at"),
            })),
            None => Ok(None),
        }
    }

    pub async fn get_block(&self, cid: &str) -> Result<Option<Vec<u8>>> {
        let row = sqlx::query("SELECT content FROM repo_block WHERE cid = ?")
            .bind(cid)
            .fetch_optional(&self.db.pool)
            .await?;

        Ok(row.map(|r| r.get("content")))
    }
}

#[derive(Clone)]
pub struct RepoTransactor {
    db: ActorDb,
    keypair: Arc<Keypair<Secp256k1>>,
    did: String,
}

impl RepoTransactor {
    pub fn new(db: ActorDb, keypair: Arc<Keypair<Secp256k1>>, did: String) -> Self {
        Self { db, keypair, did }
    }

    pub async fn update_root(&self, cid: &str, rev: &str) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();

        sqlx::query(
            "INSERT INTO repo_root (did, cid, rev, indexed_at)
             VALUES (?, ?, ?, ?)
             ON CONFLICT (did) DO UPDATE SET
                cid = ?,
                rev = ?,
                indexed_at = ?",
        )
        .bind(&self.did)
        .bind(cid)
        .bind(rev)
        .bind(&now)
        .bind(cid)
        .bind(rev)
        .bind(&now)
        .execute(&self.db.pool)
        .await?;

        Ok(())
    }

    pub async fn put_block(&self, cid: &str, content: &[u8], repo_rev: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO repo_block (cid, repo_rev, size, content)
             VALUES (?, ?, ?, ?)
             ON CONFLICT (cid) DO NOTHING",
        )
        .bind(cid)
        .bind(repo_rev)
        .bind(content.len() as i64)
        .bind(content)
        .execute(&self.db.pool)
        .await?;

        Ok(())
    }
}

pub struct RepoRoot {
    pub cid: String,
    pub rev: String,
    pub indexed_at: String,
}
