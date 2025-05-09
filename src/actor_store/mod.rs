//! Actor store implementation for ATProto PDS.

mod blob;
mod block_map;
mod db;
mod preference;
mod record;
mod repo;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context as _, Result};
use atrium_crypto::keypair::{Export as _, Secp256k1Keypair};
use sqlx::SqlitePool;

use crate::config::RepoConfig;

/// Resources required by the actor store.
pub(crate) struct ActorStoreResources {
    /// Configuration for the repo.
    pub(crate) config: RepoConfig,
    /// Background task queue (we'll need to implement this later).
    pub(crate) background_queue: Arc<()>, // TODO: Placeholder until we implement a proper queue
}

/// The location of an actor's data.
pub(crate) struct ActorLocation {
    /// The directory for the actor's data.
    pub(crate) directory: PathBuf,
    /// The database location for the actor.
    pub(crate) db_location: PathBuf,
    /// The keypair location for the actor.
    pub(crate) key_location: PathBuf,
}

/// The actor store for repository data.
pub(crate) struct ActorStore {
    /// The directory for actor data.
    pub(crate) directory: PathBuf,
    /// The directory for reserved keys.
    reserved_key_dir: PathBuf,
    /// Resources used by the actor store.
    resources: ActorStoreResources,
}

/// Reader for actor data.
pub(crate) struct ActorStoreReader {
    /// The DID of the actor.
    did: String,
    /// The database connection.
    db: SqlitePool,
    /// The actor's keypair.
    keypair: Arc<Secp256k1Keypair>,
    /// Resources for the actor store.
    resources: Arc<ActorStoreResources>,
}

/// Writer for actor data with transaction support.
pub(crate) struct ActorStoreWriter {
    /// The DID of the actor.
    did: String,
    /// The database connection.
    db: SqlitePool,
    /// The actor's keypair.
    keypair: Arc<Secp256k1Keypair>,
    /// Resources for the actor store.
    resources: Arc<ActorStoreResources>,
}

/// Transactor for actor data.
pub(crate) struct ActorStoreTransactor {
    /// The DID of the actor.
    did: String,
    /// The database connection.
    db: SqlitePool,
    /// The actor's keypair.
    keypair: Arc<Secp256k1Keypair>,
    /// Resources for the actor store.
    resources: Arc<ActorStoreResources>,
}

impl ActorStore {
    /// Create a new actor store.
    pub(crate) fn new(directory: impl Into<PathBuf>, resources: ActorStoreResources) -> Self {
        let directory = directory.into();
        let reserved_key_dir = directory.join("reserved_keys");
        Self {
            directory,
            reserved_key_dir,
            resources,
        }
    }

    /// Get the location of a DID's data.
    pub(crate) async fn get_location(&self, did: &str) -> Result<ActorLocation> {
        let did_hash = sha256_hex(did).await?;
        let directory = self.directory.join(&did_hash[0..2]).join(did);
        let db_location = directory.join("store.sqlite");
        let key_location = directory.join("key");

        Ok(ActorLocation {
            directory,
            db_location,
            key_location,
        })
    }

    /// Check if an actor exists.
    pub(crate) async fn exists(&self, did: &str) -> Result<bool> {
        let location = self.get_location(did).await?;
        Ok(tokio::fs::try_exists(&location.db_location).await?)
    }

    /// Get the keypair for an actor.
    pub(crate) async fn keypair(&self, did: &str) -> Result<Arc<Secp256k1Keypair>> {
        let location = self.get_location(did).await?;
        let priv_key = tokio::fs::read(&location.key_location).await?;
        let keypair = Secp256k1Keypair::import(&priv_key)?;
        Ok(Arc::new(keypair))
    }

    /// Open the database for an actor.
    pub(crate) async fn open_db(&self, did: &str) -> Result<SqlitePool> {
        let location = self.get_location(did).await?;

        if !tokio::fs::try_exists(&location.db_location).await? {
            return Err(anyhow::anyhow!("Repo not found"));
        }

        let db = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(
                sqlx::sqlite::SqliteConnectOptions::new()
                    .filename(&location.db_location)
                    .create_if_missing(false),
            )
            .await
            .context("failed to connect to SQLite database")?;

        Ok(db)
    }

    /// Read from an actor store.
    pub(crate) async fn read<T, F>(&self, did: &str, f: F) -> Result<T>
    where
        F: FnOnce(ActorStoreReader) -> Result<T>,
    {
        let db = self.open_db(did).await?;
        let keypair = self.keypair(did).await?;
        let resources = Arc::new(self.resources.clone());

        let reader = ActorStoreReader {
            did: did.to_string(),
            db,
            keypair,
            resources,
        };

        f(reader)
    }

    /// Transact against an actor store with full transaction support.
    pub(crate) async fn transact<T, F>(&self, did: &str, f: F) -> Result<T>
    where
        F: FnOnce(ActorStoreTransactor) -> Result<T>,
    {
        let db = self.open_db(did).await?;
        let keypair = self.keypair(did).await?;
        let resources = Arc::new(self.resources.clone());

        let transactor = ActorStoreTransactor {
            did: did.to_string(),
            db,
            keypair,
            resources,
        };

        f(transactor)
    }

    /// Write to an actor store without transaction support.
    pub(crate) async fn write_no_transaction<T, F>(&self, did: &str, f: F) -> Result<T>
    where
        F: FnOnce(ActorStoreWriter) -> Result<T>,
    {
        let db = self.open_db(did).await?;
        let keypair = self.keypair(did).await?;
        let resources = Arc::new(self.resources.clone());

        let writer = ActorStoreWriter {
            did: did.to_string(),
            db,
            keypair,
            resources,
        };

        f(writer)
    }

    /// Create a new actor repository.
    pub(crate) async fn create(&self, did: &str, keypair: Secp256k1Keypair) -> Result<()> {
        let location = self.get_location(did).await?;

        // Create directory if it doesn't exist
        tokio::fs::create_dir_all(&location.directory).await?;

        // Check if repo already exists
        if tokio::fs::try_exists(&location.db_location).await? {
            return Err(anyhow::anyhow!("Repo already exists"));
        }

        // Export and save keypair
        let priv_key = keypair.export();
        tokio::fs::write(&location.key_location, priv_key).await?;

        // Create database
        let db = sqlx::sqlite::SqlitePoolOptions::new()
            .connect_with(
                sqlx::sqlite::SqliteConnectOptions::new()
                    .filename(&location.db_location)
                    .create_if_missing(true),
            )
            .await
            .context("failed to create SQLite database")?;

        // Create database schema
        db::create_tables(&db).await?;

        Ok(())
    }

    // TODO: To be implemented: destroy, reserve_keypair, etc.
}

// Helper function for SHA-256 hashing
async fn sha256_hex(input: &str) -> Result<String> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    let result = hasher.finalize();
    Ok(hex::encode(result))
}

impl Clone for ActorStoreResources {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            background_queue: self.background_queue.clone(),
        }
    }
}
