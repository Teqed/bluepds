//! Database migrations for the actor store.
use anyhow::{Context as _, Result};
use sqlx::SqlitePool;

use crate::actor_store::db::ActorDb;

/// Migration identifier
type Migration = fn(&ActorDb) -> Result<()>;

/// Database migrator
pub(crate) struct Migrator {
    db: ActorDb,
    migrations: Vec<Migration>,
}

impl Migrator {
    /// Create a new migrator
    pub(crate) fn new(db: ActorDb) -> Self {
        Self {
            db,
            migrations: vec![_001_init],
        }
    }

    /// Run all migrations
    pub(crate) async fn migrate_to_latest(&self) -> Result<()> {
        let past_migrations = sqlx::query!("SELECT name FROM migration")
            .fetch_all(&self.db.db)
            .await?;
        let mut past_migration_names = past_migrations
            .iter()
            .map(|m| m.name.clone())
            .collect::<Vec<_>>();
        past_migration_names.sort();
        for migration in &self.migrations {
            let name = format!("{:p}", migration);
            if !past_migration_names.contains(&name) {
                migration(&self.db)?;
                sqlx::query("INSERT INTO migration (name, appliedAt) VALUES (?, ?)")
                    .bind(name)
                    .bind(chrono::Utc::now().to_rfc3339())
                    .execute(&self.db.db)
                    .await?;
            }
        }
        Ok(())
    }

    /// Run migrations and throw an error if any fail
    pub(crate) async fn migrate_to_latest_or_throw(&self) -> Result<()> {
        self.migrate_to_latest().await?;
        self.db.ensure_wal().await?;
        Ok(())
    }
}

/// Initial migration to create tables
fn _001_init(db: &ActorDb) -> Result<()> {
    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current()
            .block_on(async { crate::actor_store::db::create_tables(&db.db).await })
    })
}

/// Create the initial database tables
pub(crate) async fn create_tables(db: &SqlitePool) -> Result<()> {
    sqlx::query(
        "
        CREATE TABLE IF NOT EXISTS repo_root (
            did TEXT PRIMARY KEY NOT NULL,
            cid TEXT NOT NULL,
            rev TEXT NOT NULL,
            indexedAt TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS repo_block (
            cid TEXT PRIMARY KEY NOT NULL,
            repoRev TEXT NOT NULL,
            size INTEGER NOT NULL,
            content BLOB NOT NULL
        );

        CREATE TABLE IF NOT EXISTS record (
            uri TEXT PRIMARY KEY NOT NULL,
            cid TEXT NOT NULL,
            collection TEXT NOT NULL,
            rkey TEXT NOT NULL,
            repoRev TEXT NOT NULL,
            indexedAt TEXT NOT NULL,
            takedownRef TEXT
        );

        CREATE TABLE IF NOT EXISTS blob (
            cid TEXT PRIMARY KEY NOT NULL,
            mimeType TEXT NOT NULL,
            size INTEGER NOT NULL,
            tempKey TEXT,
            width INTEGER,
            height INTEGER,
            createdAt TEXT NOT NULL,
            takedownRef TEXT
        );

        CREATE TABLE IF NOT EXISTS record_blob (
            blobCid TEXT NOT NULL,
            recordUri TEXT NOT NULL,
            PRIMARY KEY (blobCid, recordUri)
        );

        CREATE TABLE IF NOT EXISTS backlink (
            uri TEXT NOT NULL,
            path TEXT NOT NULL,
            linkTo TEXT NOT NULL,
            PRIMARY KEY (uri, path)
        );

        CREATE TABLE IF NOT EXISTS account_pref (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            valueJson TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS migration (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            appliedAt TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_repo_block_repo_rev ON repo_block(repoRev, cid);
        CREATE INDEX IF NOT EXISTS idx_record_cid ON record(cid);
        CREATE INDEX IF NOT EXISTS idx_record_collection ON record(collection);
        CREATE INDEX IF NOT EXISTS idx_record_repo_rev ON record(repoRev);
        CREATE INDEX IF NOT EXISTS idx_blob_tempkey ON blob(tempKey);
        CREATE INDEX IF NOT EXISTS idx_backlink_link_to ON backlink(path, linkTo);
        ",
    )
    .execute(db)
    .await
    .context("failed to create tables")?;

    Ok(())
}
