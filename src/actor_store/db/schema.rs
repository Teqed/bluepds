//! Database schema definitions for the actor store.

use anyhow::{Context as _, Result};
use sqlx::SqlitePool;

use super::ActorDb;

/// Repository root information
#[derive(Debug, Clone)]
pub struct RepoRoot {
    pub did: String,
    pub cid: String,
    pub rev: String,
    pub indexed_at: String,
}

/// Repository block (IPLD block)
#[derive(Debug, Clone)]
pub struct RepoBlock {
    pub cid: String,
    pub repo_rev: String,
    pub size: i64,
    pub content: Vec<u8>,
}

/// Record information
#[derive(Debug, Clone)]
pub struct Record {
    pub uri: String,
    pub cid: String,
    pub collection: String,
    pub rkey: String,
    pub repo_rev: String,
    pub indexed_at: String,
    pub takedown_ref: Option<String>,
}

/// Blob information
#[derive(Debug, Clone)]
pub struct Blob {
    pub cid: String,
    pub mime_type: String,
    pub size: i64,
    pub temp_key: Option<String>,
    pub width: Option<i64>,
    pub height: Option<i64>,
    pub created_at: String,
    pub takedown_ref: Option<String>,
}

/// Record-blob association
#[derive(Debug, Clone)]
pub struct RecordBlob {
    pub blob_cid: String,
    pub record_uri: String,
}

/// Backlink between records
#[derive(Debug, Clone)]
pub struct Backlink {
    pub uri: String,
    pub path: String,
    pub link_to: String,
}

/// User preference
#[derive(Debug, Clone)]
pub struct AccountPref {
    pub id: i64,
    pub name: String,
    pub value_json: String,
}

/// Database migrator
pub struct Migrator {
    db: ActorDb,
}

impl Migrator {
    /// Create a new migrator
    pub fn new(db: ActorDb) -> Self {
        Self { db }
    }

    /// Run all migrations
    pub async fn migrate_to_latest(&self) -> Result<()> {
        // Create the tables
        self.db.create_tables().await
    }

    /// Run migrations and throw an error if any fail
    pub async fn migrate_to_latest_or_throw(&self) -> Result<()> {
        self.migrate_to_latest().await
    }
}

/// Count all items in a query
pub fn count_all() -> String {
    "COUNT(*)".to_string()
}

/// Count distinct items in a query
pub fn count_distinct(field: impl Into<String>) -> String {
    format!("COUNT(DISTINCT {})", field.into())
}
