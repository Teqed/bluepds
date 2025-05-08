// src/actor_store/db/migrations.rs
use anyhow::{Context as _, Result};
use sqlx::{Pool, Sqlite};

pub async fn run(pool: &Pool<Sqlite>) -> Result<()> {
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

        CREATE TABLE IF NOT EXISTS record (
            uri TEXT PRIMARY KEY NOT NULL,
            cid TEXT NOT NULL,
            collection TEXT NOT NULL,
            rkey TEXT NOT NULL,
            repo_rev TEXT NOT NULL,
            indexed_at TEXT NOT NULL,
            takedown_ref TEXT
        );

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

        CREATE TABLE IF NOT EXISTS account_pref (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            value_json TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_record_cid ON record(cid);
        CREATE INDEX IF NOT EXISTS idx_record_collection ON record(collection);
        CREATE INDEX IF NOT EXISTS idx_record_repo_rev ON record(repo_rev);
        CREATE INDEX IF NOT EXISTS idx_blob_temp_key ON blob(temp_key);
        CREATE INDEX IF NOT EXISTS idx_repo_block_repo_rev ON repo_block(repo_rev, cid);
        CREATE INDEX IF NOT EXISTS idx_backlink_link_to ON backlink(path, link_to);
        ",
    )
    .execute(pool)
    .await
    .context("Failed to run migrations")?;

    Ok(())
}
