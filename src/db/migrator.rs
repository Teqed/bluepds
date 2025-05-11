//! Database migration management.

use sqlx::{SqlitePool, migrate::Migrator};
use std::path::Path;
use thiserror::Error;

/// Error type for migration-related issues.
#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("Migration failed: {0}")]
    MigrationFailed(String),
    #[error("Unknown failure occurred while migrating")]
    UnknownFailure,
}

/// Migrator struct for managing database migrations.
pub struct DatabaseMigrator {
    /// SQLx migrator instance.
    migrator: Migrator,
    /// SQLite connection pool.
    db: SqlitePool,
}

impl DatabaseMigrator {
    /// Creates a new `DatabaseMigrator` instance.
    ///
    /// # Arguments
    /// - `migrations_path`: Path to the directory containing migration files.
    /// - `db`: SQLite connection pool.
    pub async fn new(migrations_path: &Path, db: SqlitePool) -> Self {
        let migrator = Migrator::new(migrations_path)
            .await
            .expect("Failed to initialize migrator");
        Self { migrator, db }
    }

    /// Migrates the database to a specific migration or throws an error.
    ///
    /// # Arguments
    /// - `migration`: The target migration name.
    ///
    /// # Unimplemented
    /// This currently runs all migrations instead of a specific one.
    pub async fn migrate_to_or_throw(&self, _migration: &str) -> Result<(), MigrationError> {
        // TODO: Implement migration to a specific version
        // For now, we will just run all migrations
        let result = self.migrator.run(&self.db).await;

        match result {
            Ok(_) => Ok(()),
            Err(err) => Err(MigrationError::MigrationFailed(err.to_string())),
        }
    }

    /// Migrates the database to the latest migration or throws an error.
    pub async fn migrate_to_latest_or_throw(&self) -> Result<(), MigrationError> {
        let result = self.migrator.run(&self.db).await;

        match result {
            Ok(_) => Ok(()),
            Err(err) => Err(MigrationError::MigrationFailed(err.to_string())),
        }
    }
}
