//! Database connection and transaction management.

use sqlx::{
    Sqlite, Transaction,
    sqlite::{SqliteConnectOptions, SqlitePool, SqliteQueryResult, SqliteTransactionManager},
};
use std::collections::VecDeque;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use tokio::sync::Mutex as AsyncMutex;

use crate::db::util::retry_sqlite;

/// Default pragmas for SQLite.
const DEFAULT_PRAGMAS: &[(&str, &str)] = &[
    // Add default pragmas here if needed, e.g., ("foreign_keys", "ON")
];

/// Database struct for managing SQLite connections and transactions.
pub struct Database {
    /// SQLite connection pool.
    db: Arc<SqlitePool>,
    /// Flag indicating if the database is destroyed.
    destroyed: Arc<Mutex<bool>>,
    /// Queue of commit hooks.
    commit_hooks: Arc<AsyncMutex<VecDeque<Box<dyn FnOnce() + Send>>>>,
}

impl Database {
    /// Creates a new database instance with the given location and optional pragmas.
    pub async fn new(location: &str, pragmas: Option<&[(&str, &str)]>) -> sqlx::Result<Self> {
        let mut options = SqliteConnectOptions::from_str(location)?.create_if_missing(true);

        // Apply default and user-provided pragmas.
        for &(key, value) in DEFAULT_PRAGMAS.iter().chain(pragmas.unwrap_or(&[])) {
            options = options.pragma(key.to_string(), value.to_string());
        }

        let pool = SqlitePool::connect_with(options).await?;
        Ok(Self {
            db: Arc::new(pool),
            destroyed: Arc::new(Mutex::new(false)),
            commit_hooks: Arc::new(AsyncMutex::new(VecDeque::new())),
        })
    }

    /// Ensures the database is using Write-Ahead Logging (WAL) mode.
    pub async fn ensure_wal(&self) -> sqlx::Result<()> {
        let mut conn = self.db.acquire().await?;
        sqlx::query("PRAGMA journal_mode = WAL")
            .execute(&mut *conn)
            .await?;
        Ok(())
    }

    /// Executes a transaction without retry logic.
    pub async fn transaction_no_retry<F, T>(&self, func: F) -> sqlx::Result<T>
    where
        F: FnOnce(&mut Transaction<'_, Sqlite>) -> sqlx::Result<T>,
    {
        let mut tx = self.db.begin().await?;
        let result = func(&mut tx)?;
        tx.commit().await?;
        self.run_commit_hooks().await;
        Ok(result)
    }

    /// Executes a transaction with retry logic.
    pub async fn transaction<F, T>(&self, func: F) -> sqlx::Result<T>
    where
        F: FnOnce(&mut Transaction<'_, Sqlite>) -> sqlx::Result<T> + Copy,
    {
        retry_sqlite(|| self.transaction_no_retry(func)).await
    }

    /// Executes a query with retry logic.
    pub async fn execute_with_retry<F, T>(&self, query: F) -> sqlx::Result<T>
    where
        F: Fn() -> std::pin::Pin<Box<dyn futures::Future<Output = sqlx::Result<T>> + Send>> + Copy,
    {
        retry_sqlite(|| query()).await
    }

    /// Adds a commit hook to be executed after a successful transaction.
    pub async fn on_commit<F>(&self, hook: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let mut hooks = self.commit_hooks.lock().await;
        hooks.push_back(Box::new(hook));
    }

    /// Closes the database connection pool.
    pub async fn close(&self) -> sqlx::Result<()> {
        let mut destroyed = self.destroyed.lock().unwrap();
        if *destroyed {
            return Ok(());
        }
        *destroyed = true;
        drop(self.db.clone()); // Drop the pool to close connections.
        Ok(())
    }

    /// Runs all commit hooks in the queue.
    async fn run_commit_hooks(&self) {
        let mut hooks = self.commit_hooks.lock().await;
        while let Some(hook) = hooks.pop_front() {
            hook();
        }
    }
}
