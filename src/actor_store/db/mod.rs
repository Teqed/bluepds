//! Database schema and connection management for the actor store.

pub(crate) mod migrations;
pub(crate) mod schema;

use crate::db::Database;
use anyhow::{Context as _, Result};

/// Type alias for the actor database.
pub(crate) type ActorDb = Database;

/// Gets a database connection for the actor store.
///
/// # Arguments
///
/// * `location` - The file path or URI for the SQLite database.
/// * `disable_wal_auto_checkpoint` - Whether to disable the WAL auto-checkpoint.
///
/// # Returns
///
/// A `Result` containing the `ActorDb` instance or an error.
pub async fn get_db(location: &str, disable_wal_auto_checkpoint: bool) -> Result<ActorDb> {
    let pragmas = if disable_wal_auto_checkpoint {
        Some(&[("wal_autocheckpoint", "0")][..])
    } else {
        None
    };

    Database::new(location, pragmas)
        .await
        .context("Failed to initialize the actor database")
}

/// Gets a migrator for the actor database.
///
/// # Arguments
///
/// * `db` - The actor database instance.
///
/// # Returns
///
/// A `migrations::Migrator` instance for managing database migrations.
pub fn get_migrator(db: &ActorDb) -> migrations::Migrator {
    migrations::Migrator::new(db.clone())
}
