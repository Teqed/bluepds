//! Database schema and connection management for the actor store.

pub mod migrations;
pub mod schema;

use anyhow::{Context as _, Result};
use sqlx::SqlitePool;
use std::sync::Arc;

/// The database connection for the actor store.
#[derive(Clone)]
pub struct ActorDb {
    /// The database connection pool.
    pub db: SqlitePool,
    /// Track whether we're in a transaction.
    in_transaction: Arc<std::sync::atomic::AtomicBool>,
    /// Callbacks to run on commit
    on_commit_callbacks: Arc<tokio::sync::Mutex<Vec<Box<dyn FnOnce() + Send + 'static>>>>,
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
                let callbacks = Arc::try_unwrap(txn_db.on_commit_callbacks)
                    .unwrap_or_else(|arc| (*arc).clone())
                    .into_inner();

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
    pub async fn on_commit<F>(&self, f: F)
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
pub fn get_db(location: &str, disable_wal_auto_checkpoint: bool) -> ActorDb {
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
        .expect("failed to create SQLite pool");

    ActorDb::new(pool)
}

/// Get a migrator for the database.
pub fn get_migrator(db: ActorDb) -> migrations::Migrator {
    migrations::Migrator::new(db)
}

/// Utility functions for database queries
pub mod util {
    /// Generate a SQL expression for counting all rows
    pub fn count_all() -> &'static str {
        "COUNT(*)"
    }

    /// Generate a SQL expression for counting distinct values
    pub fn count_distinct(field_ref: impl Into<String>) -> String {
        format!("COUNT(DISTINCT {})", field_ref.into())
    }

    /// Generate a SQL condition to exclude soft-deleted records
    pub fn not_soft_deleted_clause(field_ref: impl Into<String>) -> String {
        format!("{} IS NULL", field_ref.into())
    }
}

// Re-export commonly used types
pub use schema::{AccountPref, Backlink, Blob, Record, RecordBlob, RepoBlock, RepoRoot};
