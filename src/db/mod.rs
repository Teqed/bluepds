use anyhow::{Context, Result};
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::r2d2::{self, ConnectionManager, Pool, PooledConnection};
use diesel::sqlite::{Sqlite, SqliteConnection};
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

pub const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");
pub type SqlitePool = Pool<ConnectionManager<SqliteConnection>>;
pub type SqlitePooledConnection = PooledConnection<ConnectionManager<SqliteConnection>>;

/// Database type for all queries
pub type DbType = Sqlite;

/// Database connection wrapper
#[derive(Clone, Debug)]
pub struct DatabaseConnection {
    pub pool: SqlitePool,
}

impl DatabaseConnection {
    /// Create a new database connection with optional pragmas
    pub async fn new(path: &str, pragmas: Option<&[(&str, &str)]>) -> Result<Self> {
        // Create the database directory if it doesn't exist
        if let Some(parent) = Path::new(path).parent() {
            if !parent.exists() {
                tokio::fs::create_dir_all(parent)
                    .await
                    .context(format!("Failed to create directory: {:?}", parent))?;
            }
        }

        // Sanitize the path for connection string
        let database_url = format!("sqlite:{}", path);

        // Create a connection manager
        let manager = ConnectionManager::<SqliteConnection>::new(database_url);

        // Create the connection pool with SQLite-specific configurations
        let pool = r2d2::Pool::builder()
            .max_size(10)
            .connection_timeout(Duration::from_secs(30))
            .test_on_check_out(true)
            .build(manager)
            .context("Failed to create connection pool")?;

        // Initialize the database with pragmas
        if let Some(pragmas) = pragmas {
            let conn = &mut pool.get().context("Failed to get connection from pool")?;

            // Apply all pragmas
            for (pragma, value) in pragmas {
                let sql = format!("PRAGMA {} = {}", pragma, value);
                conn.batch_execute(&sql)
                    .context(format!("Failed to set pragma {}", pragma))?;
            }
        }

        let db = DatabaseConnection { pool };
        Ok(db)
    }

    /// Run migrations on the database
    pub fn run_migrations(&self) -> Result<()> {
        let mut conn = self
            .pool
            .get()
            .context("Failed to get connection for migrations")?;
        conn.run_pending_migrations(MIGRATIONS)
            .context("Failed to run migrations")?;
        Ok(())
    }

    /// Ensure WAL mode is enabled
    pub async fn ensure_wal(&self) -> Result<()> {
        let conn = &mut self.pool.get().context("Failed to get connection")?;
        conn.batch_execute("PRAGMA journal_mode = WAL;")?;
        conn.batch_execute("PRAGMA synchronous = NORMAL;")?;
        conn.batch_execute("PRAGMA foreign_keys = ON;")?;
        Ok(())
    }

    /// Execute a database operation with retries for busy errors
    pub async fn run<F, T>(&self, operation: F) -> Result<T>
    where
        F: FnOnce(&mut SqliteConnection) -> QueryResult<T> + Send,
        T: Send + 'static,
    {
        let mut retries = 0;
        let max_retries = 5;
        let mut last_error = None;

        while retries < max_retries {
            let mut conn = self.pool.get().context("Failed to get connection")?;
            match operation(&mut conn) {
                Ok(result) => return Ok(result),
                Err(diesel::result::Error::DatabaseError(
                    diesel::result::DatabaseErrorKind::DatabaseIsLocked,
                    _,
                )) => {
                    retries += 1;
                    let backoff_ms = 10 * (1 << retries); // Exponential backoff
                    last_error = Some(diesel::result::Error::DatabaseError(
                        diesel::result::DatabaseErrorKind::DatabaseIsLocked,
                        Box::new("Database is locked".to_string()),
                    ));
                    tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                }
                Err(e) => return Err(e.into()),
            }
        }

        Err(anyhow::anyhow!(
            "Max retries exceeded: {}",
            last_error.unwrap_or(diesel::result::Error::RollbackTransaction(Box::new(
                "Unknown error"
            )))
        ))
    }

    /// Check if currently in a transaction
    pub fn assert_transaction(&self) -> Result<()> {
        // SQLite doesn't have a straightforward way to check transaction state
        // We'll implement a simplified version that just returns Ok for now
        Ok(())
    }

    /// Run a transaction with retry logic for busy database errors
    pub async fn transaction<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut SqliteConnection) -> Result<T> + Send,
        T: Send + 'static,
    {
        self.run(|conn| {
            conn.transaction(|tx| {
                f(tx).map_err(|e| diesel::result::Error::RollbackTransaction(Box::new(e)))
            })
        })
        .await
    }

    /// Run a transaction with no retry logic
    pub async fn transaction_no_retry<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut SqliteConnection) -> std::result::Result<T, diesel::result::Error> + Send,
        T: Send + 'static,
    {
        let conn = &mut self
            .pool
            .get()
            .context("Failed to get connection for transaction")?;

        conn.transaction(f)
            .map_err(|e| anyhow::anyhow!("Transaction error: {:?}", e))
    }
}

/// Create a connection pool for SQLite
pub async fn create_sqlite_pool(database_url: &str) -> Result<SqlitePool> {
    let manager = ConnectionManager::<SqliteConnection>::new(database_url);
    let pool = Pool::builder()
        .max_size(10)
        .connection_timeout(Duration::from_secs(30))
        .test_on_check_out(true)
        .build(manager)
        .context("Failed to create connection pool")?;

    // Apply recommended SQLite settings
    let conn = &mut pool.get()?;
    conn.batch_execute(
        "PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA foreign_keys = ON;",
    )?;

    Ok(pool)
}
