//! Database schema and connection management for the actor store.

pub(crate) mod migrations;
pub(crate) mod schema;

use anyhow::{Context as _, Result};
use sqlx::SqlitePool;
use std::sync::Arc;

/// The database connection for the actor store.
#[derive(Clone)]
pub(crate) struct ActorDb {
    /// The database connection pool.
    pub(crate) db: SqlitePool,
    /// Track whether we're in a transaction.
    in_transaction: Arc<std::sync::atomic::AtomicBool>,
    /// Callbacks to run on commit
    on_commit_callbacks: Arc<tokio::sync::Mutex<Vec<Box<dyn FnOnce() + Send + 'static>>>>,
}

impl ActorDb {
    /// Create a new actor database.
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self {
            db: pool,
            in_transaction: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            on_commit_callbacks: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        }
    }

    /// Create a new actor database from an existing SQLite connection.
    pub(crate) fn from_pool(pool: SqlitePool) -> Self {
        Self::new(pool)
    }

    /// Assert that we're in a transaction.
    pub(crate) fn assert_transaction(&self) {
        if !self
            .in_transaction
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            panic!("Expected to be in a transaction");
        }
    }

    /// Ensure the database is in WAL mode.
    pub(crate) async fn ensure_wal(&self) -> Result<()> {
        sqlx::query("PRAGMA journal_mode=WAL")
            .execute(&self.db)
            .await
            .context("failed to set WAL mode")?;
        Ok(())
    }

    /// Close the database connection.
    pub(crate) fn close(self) {
        // Pool will be dropped when this struct is dropped
    }

    /// Start a transaction and execute the provided function.
    pub(crate) async fn transaction<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(ActorDb) -> Result<R>,
    {
        let tx = self
            .db
            .begin()
            .await
            .context("failed to begin transaction")?;

        let txn_db = ActorDb {
            db: self.db.clone(),
            in_transaction: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            on_commit_callbacks: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        };

        let result = f(txn_db.clone());

        match result {
            Ok(value) => {
                tx.commit().await.context("failed to commit transaction")?;

                // Run commit callbacks
                let callbacks = match Arc::try_unwrap(txn_db.on_commit_callbacks) {
                    Ok(mutex) => mutex.into_inner(),
                    Err(arc) => {
                        let mutex = &*arc;
                        let mut guard = mutex.try_lock().expect("lock should be available");
                        std::mem::take(&mut *guard)
                    }
                };

                for callback in callbacks {
                    callback();
                }

                Ok(value)
            }
            Err(e) => {
                // Transaction will be rolled back when dropped
                Err(e)
            }
        }
    }

    /// Register a callback to run when the transaction is committed.
    pub(crate) async fn on_commit<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        if self
            .in_transaction
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            let mut callbacks = self.on_commit_callbacks.lock().await;
            callbacks.push(Box::new(f));
        }
    }
}

/// Get a database connection.
pub(crate) async fn get_db(location: &str, disable_wal_auto_checkpoint: bool) -> Result<ActorDb> {
    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(location)
        .create_if_missing(true);

    let options = if disable_wal_auto_checkpoint {
        options.pragma("wal_autocheckpoint", "0")
    } else {
        options
    };

    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(10)
        .connect_with(options)
        .await
        .context("failed to create SQLite pool")?;

    Ok(ActorDb::new(pool))
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

/// Get a migrator for the database.
pub(crate) fn get_migrator(db: ActorDb) -> migrations::Migrator {
    migrations::Migrator::new(db)
}

/// Utility functions for database queries
pub(crate) mod util {
    /// Generate a SQL expression for counting all rows
    pub(crate) fn count_all() -> &'static str {
        "COUNT(*)"
    }

    /// Generate a SQL expression for counting distinct values
    pub(crate) fn count_distinct(field_ref: impl Into<String>) -> String {
        format!("COUNT(DISTINCT {})", field_ref.into())
    }

    /// Generate a SQL condition to exclude soft-deleted records
    pub(crate) fn not_soft_deleted_clause(field_ref: impl Into<String>) -> String {
        format!("{} IS NULL", field_ref.into())
    }
}

// Re-export commonly used types
