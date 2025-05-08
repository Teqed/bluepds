//! Database schema for the actor store.

use anyhow::{Context as _, Result};
use sqlx::SqlitePool;

/// Create the SQLite tables for an actor store.
pub async fn create_tables(db: &SqlitePool) -> Result<()> {
    sqlx::query(
        "
        CREATE TABLE IF NOT EXISTS repo_root (
            did TEXT PRIMARY KEY NOT NULL,
            cid TEXT NOT NULL,
            rev TEXT NOT NULL,
            indexed_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS repo_block (
            cid TEXT PRIMARY KEY NOT NULL,
            repo_rev TEXT NOT NULL,
            size INTEGER NOT NULL,
            content BLOB NOT NULL
        );
        CREATE INDEX IF NOT EXISTS repo_block_repo_rev_idx ON repo_block (repo_rev, cid);

        CREATE TABLE IF NOT EXISTS record (
            uri TEXT PRIMARY KEY NOT NULL,
            cid TEXT NOT NULL,
            collection TEXT NOT NULL,
            rkey TEXT NOT NULL,
            repo_rev TEXT NOT NULL,
            indexed_at TEXT NOT NULL,
            takedown_ref TEXT
        );
        CREATE INDEX IF NOT EXISTS record_cid_idx ON record (cid);
        CREATE INDEX IF NOT EXISTS record_collection_idx ON record (collection);
        CREATE INDEX IF NOT EXISTS record_repo_rev_idx ON record (repo_rev);

        CREATE TABLE IF NOT EXISTS blob (
            cid TEXT PRIMARY KEY NOT NULL,
            mime_type TEXT NOT NULL,
            size INTEGER NOT NULL,
            temp_key TEXT,
            width INTEGER,
            height INTEGER,
            created_at TEXT NOT NULL,
            takedown_ref TEXT
        );
        CREATE INDEX IF NOT EXISTS blob_tempkey_idx ON blob (temp_key);

        CREATE TABLE IF NOT EXISTS record_blob (
            blob_cid TEXT NOT NULL,
            record_uri TEXT NOT NULL,
            PRIMARY KEY (blob_cid, record_uri)
        );

        CREATE TABLE IF NOT EXISTS backlink (
            uri TEXT NOT NULL,
            path TEXT NOT NULL,
            link_to TEXT NOT NULL,
            PRIMARY KEY (uri, path)
        );
        CREATE INDEX IF NOT EXISTS backlink_link_to_idx ON backlink (path, link_to);

        CREATE TABLE IF NOT EXISTS account_pref (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            value_json TEXT NOT NULL
        );

        PRAGMA journal_mode=WAL;
        ",
    )
    .execute(db)
    .await
    .context("failed to create tables")?;

    Ok(())
}

/// Database schema type definitions.
pub mod schema {
    #[derive(Debug)]
    pub struct RepoRoot {
        pub did: String,
        pub cid: String,
        pub rev: String,
        pub indexed_at: String,
    }

    #[derive(Debug)]
    pub struct RepoBlock {
        pub cid: String,
        pub repo_rev: String,
        pub size: i64,
        pub content: Vec<u8>,
    }

    #[derive(Debug)]
    pub struct Record {
        pub uri: String,
        pub cid: String,
        pub collection: String,
        pub rkey: String,
        pub repo_rev: String,
        pub indexed_at: String,
        pub takedown_ref: Option<String>,
    }

    #[derive(Debug)]
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

    #[derive(Debug)]
    pub struct RecordBlob {
        pub blob_cid: String,
        pub record_uri: String,
    }

    #[derive(Debug)]
    pub struct Backlink {
        pub uri: String,
        pub path: String,
        pub link_to: String,
    }

    #[derive(Debug)]
    pub struct AccountPref {
        pub id: i64,
        pub name: String,
        pub value_json: String,
    }
}
