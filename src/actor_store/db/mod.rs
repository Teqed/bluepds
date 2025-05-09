//! Database schema and connection management for the actor store.

pub mod schema;

use anyhow::{Context as _, Result};
use sqlx::{Sqlite, SqlitePool, Transaction};
use std::sync::Arc;

/// The database connection for the actor store.
#[derive(Clone)]
pub struct ActorDb {
    /// The database connection pool.
    pub db: SqlitePool,
    /// Track whether we're in a transaction.
    in_transaction: Arc<std::sync::atomic::AtomicBool>,
    /// Callbacks to run on commit
    #[allow(dead_code)] // For now
    on_commit_callbacks: Arc<tokio::sync::Mutex<Vec<Box<dyn FnOnce() -> () + Send + 'static>>>>,
}

impl ActorDb {
    /// Create a new actor database.
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            db: pool,
            in_transaction: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            on_commit_callbacks: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        }
    }

    /// Create a new actor database from an existing SQLite connection.
    pub fn from_pool(pool: SqlitePool) -> Self {
        Self::new(pool)
    }

    /// Assert that we're in a transaction.
    pub fn assert_transaction(&self) {
        if !self
            .in_transaction
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            panic!("Expected to be in a transaction");
        }
    }

    /// Ensure the database is in WAL mode.
    pub async fn ensure_wal(&self) -> Result<()> {
        sqlx::query("PRAGMA journal_mode=WAL")
            .execute(&self.db)
            .await
            .context("failed to set WAL mode")?;
        Ok(())
    }

    /// Close the database connection.
    pub fn close(self) {
        // Pool will be dropped when this struct is dropped
    }

    /// Start a transaction and execute the provided function.
    pub async fn transaction<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(ActorDb) -> Result<R>,
    {
        let mut tx = self
            .db
            .begin()
            .await
            .context("failed to begin transaction")?;

        let db = Self {
            db: self.db.clone(), // Not ideal but we can access the tx within the closure
            in_transaction: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            on_commit_callbacks: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        };

        let result = f(db);

        if result.is_ok() {
            tx.commit().await.context("failed to commit transaction")?;

            // Run commit callbacks
            // In a real implementation, we'd execute the callbacks here
            todo!()
        }

        result
    }

    /// Register a callback to run when the transaction is committed.
    pub fn on_commit<F>(&self, f: F)
    where
        F: FnOnce() -> () + Send + 'static,
    {
        if self
            .in_transaction
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            // In a real implementation, we'd add the callback to a list
            // to be executed after commit
            // For now, this is a no-op
            todo!()
        }
    }

    /// Get a mutable reference to the transaction.
    /// This is needed for some operations that require the transaction.
    pub fn transaction_mut(&mut self) -> Result<&mut Transaction<'static, Sqlite>> {
        Err(anyhow::anyhow!("Not implemented"))
    }

    /// Create the database tables.
    pub async fn create_tables(&self) -> Result<()> {
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
            CREATE INDEX IF NOT EXISTS idx_repo_block_repo_rev ON repo_block(repoRev, cid);

            CREATE TABLE IF NOT EXISTS record (
                uri TEXT PRIMARY KEY NOT NULL,
                cid TEXT NOT NULL,
                collection TEXT NOT NULL,
                rkey TEXT NOT NULL,
                repoRev TEXT NOT NULL,
                indexedAt TEXT NOT NULL,
                takedownRef TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_record_cid ON record(cid);
            CREATE INDEX IF NOT EXISTS idx_record_collection ON record(collection);
            CREATE INDEX IF NOT EXISTS idx_record_repo_rev ON record(repoRev);

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
            CREATE INDEX IF NOT EXISTS idx_blob_tempkey ON blob(tempKey);

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
            CREATE INDEX IF NOT EXISTS idx_backlink_link_to ON backlink(path, linkTo);

            CREATE TABLE IF NOT EXISTS account_pref (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                valueJson TEXT NOT NULL
            );
            ",
        )
        .execute(&self.db)
        .await
        .context("failed to create tables")?;

        Ok(())
    }
}

/// Get a database connection.
pub fn get_db(location: &str, disable_wal_auto_checkpoint: bool) -> ActorDb {
    todo!()
}

/// Get a migrator for the database.
pub fn get_migrator(db: ActorDb) -> schema::Migrator {
    schema::Migrator::new(db)
}
