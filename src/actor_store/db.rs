//! Database schema and connection management for the actor store.

use crate::db::DatabaseConnection;
use anyhow::{Context as _, Result};
use diesel::prelude::*;

/// Type alias for the actor database.
pub(crate) type ActorDb = DatabaseConnection;

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
        Some(
            &[
                ("wal_autocheckpoint", "0"),
                ("journal_mode", "WAL"),
                ("synchronous", "NORMAL"),
                ("foreign_keys", "ON"),
            ][..],
        )
    } else {
        Some(
            &[
                ("journal_mode", "WAL"),
                ("synchronous", "NORMAL"),
                ("foreign_keys", "ON"),
            ][..],
        )
    };

    let db = DatabaseConnection::new(location, pragmas)
        .await
        .context("Failed to initialize the actor database")?;

    // Ensure WAL mode is properly set up
    db.ensure_wal().await?;

    // Run migrations
    // TODO: make sure the migrations are populated?
    db.run_migrations()
        .context("Failed to run migrations on the actor database")?;

    Ok(db)
}
