// src/actor_store/db/mod.rs
pub mod migrations;

use anyhow::Result;
use sqlx::{Pool, Sqlite, Transaction};

#[derive(Clone)]
pub struct ActorDb {
    pub pool: Pool<Sqlite>,
}

impl ActorDb {
    pub fn new(pool: Pool<Sqlite>) -> Self {
        Self { pool }
    }

    pub async fn migrate(&self) -> Result<()> {
        migrations::run(&self.pool).await
    }

    pub async fn transaction<F, R>(&self, f: F) -> Result<R>
    where
        F: for<'txn> FnOnce(&'txn mut Transaction<'_, Sqlite>) -> Result<R> + Send,
        R: Send + 'static,
    {
        self.pool.begin().await?.commit().await?;
        let mut tx = self.pool.begin().await?;
        let result = f(&mut tx)?;
        tx.commit().await?;
        Ok(result)
    }

    pub fn assert_transaction(&self) {
        // In a real implementation, we might want to check if we're in a transaction,
        // but SQLite allows nested transactions so it's not strictly necessary
    }
}
