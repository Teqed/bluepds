use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{Context as _, Result, anyhow, bail};
use atrium_crypto::keypair::{Did as _, Export as _, Secp256k1Keypair};
use atrium_repo::Cid;
use diesel::prelude::*;
use sha2::Digest as _;
use tokio::fs;

use super::PreparedWrite;
use super::actor_store_handler::ActorStoreHandler;
use super::actor_store_resources::ActorStoreResources;
use super::blob::{BlobStore as _, BlobStorePlaceholder};
use super::db::{ActorDb, get_db};
use crate::SigningKey;

/// Central manager for actor stores
pub(crate) struct ActorStore {
    /// Base directory for actor data
    pub directory: PathBuf,
    /// Resources shared between actor stores
    pub resources: ActorStoreResources,
}

struct ActorLocation {
    /// Actor's directory path
    directory: PathBuf,
    /// Database path
    db_location: PathBuf,
    /// Key path
    key_location: PathBuf,
}

impl ActorStore {
    /// Create a new actor store manager
    pub(crate) fn new(directory: impl Into<PathBuf>, resources: ActorStoreResources) -> Self {
        Self {
            directory: directory.into(),
            resources,
        }
    }

    /// Get the location information for an actor
    pub(crate) async fn get_location(&self, did: &str) -> Result<ActorLocation> {
        // Hash the DID for directory organization
        let did_hash = sha2::Sha256::digest(did.as_bytes());
        let hash_prefix = format!("{:02x}", did_hash[0]);

        // Create paths
        let directory = self.directory.join(hash_prefix).join(did);
        let db_location = directory.join("store.sqlite");
        let key_location = directory.join("key");

        Ok(ActorLocation {
            directory,
            db_location,
            key_location,
        })
    }

    /// Check if an actor store exists
    pub(crate) async fn exists(&self, did: &str) -> Result<bool> {
        let location = self.get_location(did).await?;
        Ok(location.db_location.exists())
    }

    /// Get the signing keypair for an actor
    pub(crate) async fn keypair(&self, did: &str) -> Result<Arc<SigningKey>> {
        let location = self.get_location(did).await?;
        let priv_key = fs::read(&location.key_location)
            .await
            .context("Failed to read key file")?;

        let keypair = SigningKey::import(&priv_key).context("Failed to import signing key")?;

        Ok(Arc::new(keypair))
    }

    /// Open the database for an actor
    pub(crate) async fn open_db(&self, did: &str) -> Result<ActorDb> {
        let location = self.get_location(did).await?;

        if !location.db_location.exists() {
            bail!("Repo not found");
        }

        // Convert path to string for SQLite connection
        let db_path = location
            .db_location
            .to_str()
            .ok_or_else(|| anyhow!("Invalid path encoding"))?;

        // Open database with WAL mode enabled
        let db = get_db(db_path, false)
            .await
            .context("Failed to open actor database")?;

        // Run a simple query to ensure the database is ready
        db.run(|conn| diesel::sql_query("SELECT 1 FROM repo_root LIMIT 1").execute(conn))
            .await
            .context("Database not ready")?;

        Ok(db)
    }

    /// Execute read operations on an actor store
    pub(crate) async fn read<T, F>(&self, did: &str, f: F) -> Result<T>
    where
        F: FnOnce(ActorStoreHandler) -> Result<T>,
    {
        let db = self.open_db(did).await?;
        let blobstore = self.resources.blobstore(did.to_string());

        // Create a read-only handler
        let handler = ActorStoreHandler::new_reader(db.clone(), did.to_string(), blobstore);

        // Execute the function
        f(handler)
    }

    /// Execute read-write operations with a transaction
    pub(crate) async fn transact<T, F>(&self, did: &str, f: F) -> Result<T>
    where
        F: FnOnce(ActorStoreHandler) -> Result<T>,
    {
        let db = self.open_db(did).await?;
        let keypair = self.keypair(did).await?;
        let blobstore = self.resources.blobstore(did.to_string());
        let background_queue = self.resources.background_queue();

        // Create a read-write handler with transaction support
        let handler = ActorStoreHandler::new_writer(
            db,
            did.to_string(),
            blobstore,
            keypair,
            background_queue.as_ref().clone(),
        );

        // Execute the function (will handle transactions internally)
        f(handler)
    }

    /// Execute read-write operations without a transaction
    pub(crate) async fn write_no_transaction<T, F>(&self, did: &str, f: F) -> Result<T>
    where
        F: FnOnce(ActorStoreHandler) -> Result<T>,
    {
        let db = self.open_db(did).await?;
        let keypair = self.keypair(did).await?;
        let blobstore = self.resources.blobstore(did.to_string());
        let background_queue = self.resources.background_queue();

        // Create a read-write handler without automatic transaction
        let handler = ActorStoreHandler::new_writer(
            db,
            did.to_string(),
            blobstore,
            keypair,
            background_queue.as_ref().clone(),
        );

        // Execute the function
        f(handler)
    }

    /// Create a new actor store
    pub(crate) async fn create(&self, did: &str, keypair: SigningKey) -> Result<()> {
        let location = self.get_location(did).await?;

        // Ensure directory exists
        fs::create_dir_all(&location.directory)
            .await
            .context("Failed to create directory")?;

        // Check if repo already exists
        if location.db_location.exists() {
            bail!("Repo already exists");
        }

        // Export and save private key
        let priv_key = keypair.export();
        fs::write(&location.key_location, priv_key)
            .await
            .context("Failed to write key file")?;

        // Initialize the database
        let db_path = location
            .db_location
            .to_str()
            .ok_or_else(|| anyhow!("Invalid path encoding"))?;

        let db = get_db(db_path, false)
            .await
            .context("Failed to create actor database")?;

        // Ensure WAL mode and run migrations
        db.ensure_wal().await?;
        db.run_migrations()?;

        Ok(())
    }

    /// Destroy an actor store
    pub(crate) async fn destroy(&self, did: &str) -> Result<()> {
        // Get all blob CIDs first
        let cids = self
            .read(did, |handler| async move {
                handler.repo.blob.get_blob_cids().await
            })
            .await?;

        // Delete all blobs
        let blobstore = self.resources.blobstore(did.to_string());
        if !cids.is_empty() {
            // Process in chunks of 500
            for chunk in cids.chunks(500) {
                let _ = blobstore.delete_many(chunk.to_vec()).await;
            }
        }

        // Remove directory and all files
        let location = self.get_location(did).await?;
        if location.directory.exists() {
            fs::remove_dir_all(&location.directory)
                .await
                .context("Failed to remove actor directory")?;
        }

        Ok(())
    }

    /// Reserve a keypair for future use
    pub(crate) async fn reserve_keypair(&self, did: Option<&str>) -> Result<String> {
        let reserved_dir = self
            .resources
            .reserved_key_dir()
            .ok_or_else(|| anyhow!("No reserved key directory configured"))?;

        // If DID is provided, check if key already exists
        let mut key_path = None;
        if let Some(did_str) = did {
            assert_safe_path_part(did_str)?;
            key_path = Some(reserved_dir.join(did_str));

            if key_path.as_ref().unwrap().exists() {
                let key_data = fs::read(key_path.as_ref().unwrap()).await?;
                let keypair = Secp256k1Keypair::import(&key_data)
                    .context("Failed to import existing reserved key")?;
                return Ok(keypair.did());
            }
        }

        // Create a new keypair
        let keypair = Secp256k1Keypair::create(&mut rand::thread_rng());
        let key_did = keypair.did();

        // Set path if not already set
        let final_path = key_path.unwrap_or_else(|| reserved_dir.join(&key_did));

        // Ensure directory exists
        fs::create_dir_all(reserved_dir).await?;

        // Save key
        fs::write(&final_path, keypair.export()).await?;

        Ok(key_did)
    }

    /// Get a reserved keypair
    pub(crate) async fn get_reserved_keypair(
        &self,
        key_did: &str,
    ) -> Result<Option<Arc<SigningKey>>> {
        let reserved_dir = self
            .resources
            .reserved_key_dir()
            .ok_or_else(|| anyhow!("No reserved key directory configured"))?;

        let key_path = reserved_dir.join(key_did);
        if !key_path.exists() {
            return Ok(None);
        }

        let key_data = fs::read(key_path).await?;
        let keypair = SigningKey::import(&key_data).context("Failed to import reserved key")?;

        Ok(Some(Arc::new(keypair)))
    }

    /// Clear a reserved keypair
    pub(crate) async fn clear_reserved_keypair(
        &self,
        key_did: &str,
        did: Option<&str>,
    ) -> Result<()> {
        let reserved_dir = self
            .resources
            .reserved_key_dir()
            .ok_or_else(|| anyhow!("No reserved key directory configured"))?;

        // Remove key by DID
        let key_path = reserved_dir.join(key_did);
        if key_path.exists() {
            fs::remove_file(key_path).await?;
        }

        // If DID mapping provided, remove that too
        if let Some(did_str) = did {
            let did_path = reserved_dir.join(did_str);
            if did_path.exists() {
                fs::remove_file(did_path).await?;
            }
        }

        Ok(())
    }

    /// Store a PLC operation
    pub(crate) async fn store_plc_op(&self, did: &str, op: &[u8]) -> Result<()> {
        let location = self.get_location(did).await?;
        let op_path = location.directory.join("did-op");

        fs::write(op_path, op).await?;
        Ok(())
    }

    /// Get a stored PLC operation
    pub(crate) async fn get_plc_op(&self, did: &str) -> Result<Vec<u8>> {
        let location = self.get_location(did).await?;
        let op_path = location.directory.join("did-op");

        let data = fs::read(op_path).await?;
        Ok(data)
    }

    /// Clear a stored PLC operation
    pub(crate) async fn clear_plc_op(&self, did: &str) -> Result<()> {
        let location = self.get_location(did).await?;
        let op_path = location.directory.join("did-op");

        if op_path.exists() {
            fs::remove_file(op_path).await?;
        }

        Ok(())
    }
}

/// Ensure a path part is safe to use in a filename
fn assert_safe_path_part(part: &str) -> Result<()> {
    let normalized = std::path::Path::new(part)
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow!("Invalid path"))?;

    if part != normalized || part.starts_with('.') || part.contains('/') || part.contains('\\') {
        bail!("Unsafe path part: {}", part);
    }

    Ok(())
}
