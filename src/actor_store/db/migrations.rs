//! Database migrations for the actor store.
use anyhow::{Context as _, Result};

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
            migrations: vec![init_migration],
        }
    }

    /// Run all migrations
    pub(crate) async fn migrate_to_latest(&self) -> Result<()> {
        // In a production system, we'd track which migrations have been run
        // For simplicity, we just run them all for now
        for migration in &self.migrations {
            migration(&self.db)?;
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
fn init_migration(db: &ActorDb) -> Result<()> {
    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(async {
            // Use the create_tables function that's now defined in the db module
            crate::actor_store::db::create_tables(&db.db).await
        })
    })
}
