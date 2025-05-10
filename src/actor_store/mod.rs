//! Actor store implementation for ATProto PDS.

mod blob;
mod db;
mod preference;
mod record;
mod repo;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context as _, Result};
use atrium_crypto::keypair::Export as _;
use sqlx::SqlitePool;

use crate::SigningKey;
use crate::config::RepoConfig;

/// Resources required by the actor store.
pub(crate) struct ActorStoreResources {
    /// Configuration for the repo.
    pub(crate) config: RepoConfig,
    /// Configuration for the blob store.
    pub(crate) blob_config: crate::config::BlobConfig,
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
    pub(crate) did: String,
    /// The database connection.
    pub(crate) db: SqlitePool,
    /// The actor's keypair.
    keypair: Arc<SigningKey>,
    /// Resources for the actor store.
    pub(crate) resources: Arc<ActorStoreResources>,
    /// Repository reader
    pub(crate) repo: repo::RepoReader,
    /// Record reader
    pub(crate) record: record::RecordReader,
    /// Preference reader
    pub(crate) pref: preference::PreferenceReader,
}

/// Writer for actor data with transaction support.
pub(crate) struct ActorStoreWriter {
    /// The DID of the actor.
    pub(crate) did: String,
    /// The database connection.
    pub(crate) db: SqlitePool,
    /// The actor's keypair.
    keypair: Arc<SigningKey>,
    /// Resources for the actor store.
    pub(crate) resources: Arc<ActorStoreResources>,
    /// Repository access
    pub(crate) repo: repo::RepoTransactor,
    /// Record access
    pub(crate) record: record::RecordTransactor,
    /// Preference access
    pub(crate) pref: preference::PreferenceTransactor,
}

/// Transactor for actor data.
pub(crate) struct ActorStoreTransactor {
    /// The DID of the actor.
    pub(crate) did: String,
    /// The database connection.
    pub(crate) db: SqlitePool,
    /// The actor's keypair.
    keypair: Arc<SigningKey>,
    /// Resources for the actor store.
    pub(crate) resources: Arc<ActorStoreResources>,
    /// Repository access
    pub(crate) repo: repo::RepoTransactor,
    /// Record access
    pub(crate) record: record::RecordTransactor,
    /// Preference access
    pub(crate) pref: preference::PreferenceTransactor,
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

    /// Load an actor store based on a given DID.
    pub(crate) async fn load(did: &str, resources: ActorStoreResources) -> Result<Self> {
        let did_hash = sha256_hex(did).await?;
        let directory = resources.config.path.join(&did_hash[0..2]).join(did);
        let reserved_key_dir = directory.join("reserved_keys");

        Ok(Self {
            directory,
            reserved_key_dir,
            resources,
        })
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
    pub(crate) async fn keypair(&self, did: &str) -> Result<Arc<SigningKey>> {
        let location = self.get_location(did).await?;
        let priv_key = tokio::fs::read(&location.key_location).await?;
        let keypair = SigningKey::import(&priv_key)?;
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
        let did_str = did.to_string();

        let reader = ActorStoreReader {
            did: did_str.clone(),
            repo: repo::RepoReader::new(
                db.clone(),
                did_str.clone(),
                self.resources.blob_config.clone(),
            ),
            record: record::RecordReader::new(db.clone(), did_str.clone()),
            pref: preference::PreferenceReader::new(db.clone(), did_str),
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
        let keypair = self.keypair(did).await?;
        let db = self.open_db(did).await?;
        let resources = Arc::new(self.resources.clone());
        let did_str = did.to_string();

        let transactor = ActorStoreTransactor {
            did: did_str.clone(),
            repo: repo::RepoTransactor::new(
                db.clone(),
                did_str.clone(),
                (*keypair).clone(),
                self.resources.blob_config.clone(),
            ),
            record: record::RecordTransactor::new(
                db.clone(),
                db.clone(), // Using db as placeholder for blobstore
                did_str.clone(),
            ),
            pref: preference::PreferenceTransactor::new(db.clone(), did_str),
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
        let did_str = did.to_string();

        let writer = ActorStoreWriter {
            did: did_str.clone(),
            repo: repo::RepoTransactor::new(
                db.clone(),
                did_str.clone(),
                (*keypair).clone(),
                self.resources.blob_config.clone(),
            ),
            record: record::RecordTransactor::new(
                db.clone(),
                db.clone(), // Using db as placeholder for blobstore
                did_str.clone(),
            ),
            pref: preference::PreferenceTransactor::new(db.clone(), did_str),
            db,
            keypair,
            resources,
        };

        f(writer)
    }

    /// Create a new actor repository.
    pub(crate) async fn create(&self, did: &str, keypair: SigningKey) -> Result<()> {
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

    /// Destroy an actor's repository and associated data.
    pub(crate) async fn destroy(&self, did: &str) -> Result<()> {
        // TODO: Implement repository destruction
        // - Delete all blobs for the repository
        // - Remove the repository directory
        todo!("Implement repository destruction")
    }

    /// Reserve a keypair for a DID.
    pub(crate) async fn reserve_keypair(&self, did: Option<&str>) -> Result<String> {
        // TODO: Implement keypair reservation
        // - Generate a keypair if one doesn't exist
        // - Store the keypair in the reserved_key_dir
        // - Return the DID of the keypair
        todo!("Implement keypair reservation")
    }

    /// Get a reserved keypair.
    pub(crate) async fn get_reserved_keypair(
        &self,
        signing_key_or_did: &str,
    ) -> Result<Option<()>> {
        // TODO: Implement getting a reserved keypair
        // - Load the keypair from the reserved_key_dir
        todo!("Implement getting a reserved keypair")
    }

    /// Clear a reserved keypair.
    pub(crate) async fn clear_reserved_keypair(
        &self,
        key_did: &str,
        did: Option<&str>,
    ) -> Result<()> {
        // TODO: Implement clearing a reserved keypair
        // - Remove the keypair file from the reserved_key_dir
        todo!("Implement clearing a reserved keypair")
    }

    /// Store a PLC operation.
    pub(crate) async fn store_plc_op(&self, did: &str, op: &[u8]) -> Result<()> {
        // TODO: Implement storing a PLC operation
        // - Store the operation in the actor's directory
        todo!("Implement storing a PLC operation")
    }

    /// Get a stored PLC operation.
    pub(crate) async fn get_plc_op(&self, did: &str) -> Result<Vec<u8>> {
        // TODO: Implement getting a PLC operation
        // - Retrieve the operation from the actor's directory
        todo!("Implement getting a PLC operation")
    }

    /// Clear a stored PLC operation.
    pub(crate) async fn clear_plc_op(&self, did: &str) -> Result<()> {
        // TODO: Implement clearing a PLC operation
        // - Remove the operation file from the actor's directory
        todo!("Implement clearing a PLC operation")
    }
}

impl ActorStoreWriter {
    /// Transact with the writer.
    pub(crate) async fn transact<T, F>(&self, f: F) -> Result<T>
    where
        F: FnOnce(ActorStoreTransactor) -> Result<T>,
    {
        todo!("Implement transact method for ActorStoreWriter")
    }
}

impl Clone for ActorStoreResources {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            blob_config: self.blob_config.clone(),
            background_queue: self.background_queue.clone(),
        }
    }
}

// Helper function for SHA-256 hashing
async fn sha256_hex(input: &str) -> Result<String> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    let result = hasher.finalize();
    Ok(hex::encode(result))
}
